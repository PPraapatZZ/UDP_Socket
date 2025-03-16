import socket
import os
import struct
import sys
import time
import random
import hashlib

# Constants
BUFFER_SIZE = 4096  # Increased from 1024 to handle larger datagrams
CHUNK_SIZE = 1024   # Size of data chunks (keep this at 1024)
TIMEOUT = 0.2       # Retransmission timeout in seconds
WINDOW_SIZE = 32    # Number of packets that can be in flight
MAX_RETRIES = 25    # Increased maximum retries for challenging networks
MAX_TRANSFER_TIME = 120  # Increased time limit for Case 6 (high RTT)

def send_file(file_path, server_ip, server_port):
    print(f"Starting transfer with connection to {server_ip}:{server_port}")
    print("NOTE: Configure and run Clumsy if testing network conditions")
    
    start_time = time.time()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT)

    # Set socket buffer size to handle larger packets
    try:
        # Increase socket buffer sizes
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)  # 256 KB receive buffer
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 262144)  # 256 KB send buffer
    except:
        print("Warning: Couldn't set socket buffer size")
    
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    
    # Calculate MD5 hash of the file for verification
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f_hash:
        for chunk in iter(lambda: f_hash.read(4096), b""):
            md5_hash.update(chunk)
    file_md5 = md5_hash.hexdigest()
    print(f"File MD5: {file_md5}")
    
    print(f"Sending file: {file_name} ({file_size} bytes)")
    
    # Send file name, size and MD5 hash first
    header = f"{file_name}:{file_size}:{file_md5}".encode()
    ack_received = False
    retries = 0
    
    while not ack_received and retries < MAX_RETRIES:
        try:
            sock.sendto(header, (server_ip, server_port))
            # Wait for header acknowledgement
            ack, _ = sock.recvfrom(BUFFER_SIZE)
            if ack.decode() == "HEADER_ACK":
                ack_received = True
            else:
                retries += 1
        except socket.timeout:
            retries += 1
            print(f"Header timeout! Retry {retries}/{MAX_RETRIES}")
    
    if retries >= MAX_RETRIES:
        print("Failed to send file header after maximum retries")
        sock.close()
        return False
    
    # Detect high RTT based on header exchange timing
    initial_rtt = 0
    for attempt in range(3):
        start_probe = time.time()
        sock.sendto(b"RTT_PROBE", (server_ip, server_port))
        try:
            sock.settimeout(1.0)  # Longer timeout for RTT detection
            probe_response, _ = sock.recvfrom(BUFFER_SIZE)
            if probe_response == b"RTT_ACK":
                initial_rtt = time.time() - start_probe
                break
        except socket.timeout:
            pass

    # Reset socket timeout to normal
    sock.settimeout(TIMEOUT)

    # Adapt parameters based on detected RTT
    high_rtt_mode = initial_rtt > 0.1  # Over 100ms RTT
    extreme_rtt_mode = initial_rtt > 0.2  # Over 200ms RTT (Case 6)
    
    if extreme_rtt_mode:
        print(f"Detected extreme RTT: {initial_rtt*1000:.0f}ms - Using aggressive parameters")
        effective_timeout = max(1.0, initial_rtt * 3)  # More conservative timeout for extreme RTT
        effective_window = min(192, WINDOW_SIZE * 6)  # Much larger window for Case 6
    elif high_rtt_mode:
        print(f"Detected high RTT: {initial_rtt*1000:.0f}ms - Adapting parameters")
        effective_timeout = max(0.5, initial_rtt * 2)  # Dynamic timeout based on RTT
        effective_window = min(128, WINDOW_SIZE * 4)  # Larger window for high RTT
    else:
        effective_timeout = TIMEOUT
        effective_window = WINDOW_SIZE
    
    # Prepare sliding window
    window = {}  # seq_num -> (packet_data, sent_time, retries)
    next_seq_num = 0
    base_seq_num = 0  # First unacknowledged packet
    
    with open(file_path, "rb") as f:
        # Read all file data into chunks for easier window management
        chunks = []
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            chunks.append(chunk)
        
        total_packets = len(chunks)
        print(f"File split into {total_packets} packets")
        
        # Process until all packets are acknowledged
        while base_seq_num < total_packets:
            current_time = time.time()
            if current_time - start_time > MAX_TRANSFER_TIME:
                print(f"Transfer exceeded time limit of {MAX_TRANSFER_TIME} seconds. Aborting.")
                sock.close()
                return False

            # Adaptive sending strategy based on remaining time
            remaining_time_percent = (MAX_TRANSFER_TIME - (current_time - start_time)) / MAX_TRANSFER_TIME * 100
            expected_progress_percent = 100 - remaining_time_percent  # What percentage should be done by now
            actual_progress_percent = (base_seq_num / total_packets) * 100  # What percentage is actually done
            
            progress_deficit = expected_progress_percent - actual_progress_percent

            # Send packets more aggressively when time is running low
            send_count = 0
            max_burst = 12 if high_rtt_mode else 8  # Higher burst for high RTT

            # Adjust burst size based on deficit and remaining time
            if progress_deficit > 5 or remaining_time_percent < 70:
                max_burst = 24 if high_rtt_mode else 16
                
            if progress_deficit > 10 or remaining_time_percent < 50:
                max_burst = 48 if high_rtt_mode else 32
                
            if progress_deficit > 15 or remaining_time_percent < 30:
                max_burst = 96 if high_rtt_mode else 64

            # For the final push when we're close to the end
            if remaining_time_percent < 20 and actual_progress_percent > 75:
                max_burst = 192 if high_rtt_mode else 128
                
            # For Case 6 (extreme RTT), use even more aggressive bursting
            if extreme_rtt_mode:
                max_burst *= 2
                if remaining_time_percent < 50:
                    max_burst *= 2  # Even more aggressive for Case 6 when time is running out

            # Send packets more aggressively with adaptive window size
            current_window_size = effective_window
            while len(window) < current_window_size and next_seq_num < total_packets and send_count < max_burst:
                chunk = chunks[next_seq_num]
                packet = struct.pack("!II", next_seq_num, total_packets) + chunk
                window[next_seq_num] = (packet, time.time(), 0)
                sock.sendto(packet, (server_ip, server_port))
                next_seq_num += 1
                send_count += 1
            
            # Check for ACKs and timeouts
            try:
                ack, _ = sock.recvfrom(BUFFER_SIZE)
                try:
                    ack_num = struct.unpack("!I", ack)[0]
                    
                    if ack_num in window:
                        # Got ACK for a packet in our window
                        del window[ack_num]
                        
                        # Update base if possible
                        while base_seq_num < total_packets and base_seq_num not in window:
                            base_seq_num += 1

                        # Dynamically adjust window size based on progress
                        if remaining_time_percent < 40 and len(window) < current_window_size//2:
                            # If we're making good progress despite time pressure, be more aggressive
                            # Try to send multiple packets for each ACK received when window is not full
                            for _ in range(min(3, current_window_size - len(window))):
                                if next_seq_num < total_packets:
                                    chunk = chunks[next_seq_num]
                                    packet = struct.pack("!II", next_seq_num, total_packets) + chunk
                                    window[next_seq_num] = (packet, time.time(), 0)
                                    sock.sendto(packet, (server_ip, server_port))
                                    next_seq_num += 1
                            
                        # More frequent progress updates for high RTT
                        if (base_seq_num % (total_packets // (40 if high_rtt_mode else 20))) == 0:
                            progress = (base_seq_num / total_packets) * 100
                            print(f"Progress: {progress:.1f}% (ACK: {ack_num})")
                except struct.error:
                    print("Received malformed ACK, ignoring")
                    
            except socket.timeout:
                # Check for packets that need retransmission
                current_time = time.time()
                for seq_num in list(window.keys()):
                    packet, sent_time, retries = window[seq_num]
                    
                    # Calculate dynamic timeout based on RTT mode
                    if extreme_rtt_mode:
                        # For Case 6, use specialized timeouts
                        if retries == 0:
                            dynamic_timeout = effective_timeout  # First attempt uses base timeout
                        elif retries <= 3:
                            dynamic_timeout = effective_timeout * (1.1 ** retries)  # Very gentle backoff for Case 6
                        else:
                            dynamic_timeout = effective_timeout * (1.2 ** min(retries-3, 3))  # Limited backoff later
                    elif high_rtt_mode:
                        # For high RTT, use more conservative timeouts
                        if retries == 0:
                            dynamic_timeout = effective_timeout  # First attempt uses base timeout
                        elif retries <= 3:
                            dynamic_timeout = effective_timeout * (1.2 ** retries)  # Gentle backoff for initial retries
                        else:
                            dynamic_timeout = effective_timeout * (1.5 ** min(retries-3, 3))  # More aggressive backoff later
                    else:
                        # Original logic for low RTT
                        if retries <= 1:
                            dynamic_timeout = TIMEOUT * 0.8
                        elif retries <= 3: 
                            dynamic_timeout = TIMEOUT
                        else:
                            dynamic_timeout = TIMEOUT * (1.05 ** (retries - 3))

                    # Adjust timeout based on remaining time and progress
                    remaining_time_percent = (MAX_TRANSFER_TIME - (current_time - start_time)) / MAX_TRANSFER_TIME * 100
                    if remaining_time_percent < 50:
                        # When time is running out, reduce timeout aggressively
                        dynamic_timeout = max(TIMEOUT * 0.2, dynamic_timeout * 0.4)
                    elif remaining_time_percent < 25:
                        # Almost out of time - use absolute minimum timeout
                        dynamic_timeout = TIMEOUT * 0.15

                    # Special handling for final phase - super aggressive
                    if remaining_time_percent < 20 and actual_progress_percent > 75:
                        dynamic_timeout = TIMEOUT * 0.1  # Ultra-minimal timeout for final push
                    
                    if current_time - sent_time > dynamic_timeout:
                        if retries >= MAX_RETRIES:
                            print(f"Packet {seq_num} failed after {MAX_RETRIES} retries")
                            sock.close()
                            return False
                        
                        # Retransmit with exponential backoff
                        sock.sendto(packet, (server_ip, server_port))
                        window[seq_num] = (packet, current_time, retries + 1)
                        # Only print occasionally to avoid slowing down execution
                        if retries <= 1 or retries % 5 == 0:
                            print(f"Timeout! Resending packet {seq_num} (retry {retries+1})")
    
    # Send termination packet
    term_packet = struct.pack("!II", total_packets, total_packets)
    ack_received = False
    retries = 0
    
    print("Sending termination packet...")
    # Try multiple times with increasing timeout
    while not ack_received and retries < MAX_RETRIES:
        # Check overall time limit
        current_time = time.time()
        if current_time - start_time > MAX_TRANSFER_TIME:
            print(f"Transfer exceeded time limit of {MAX_TRANSFER_TIME} seconds during termination. Aborting.")
            sock.close()
            return False
        try:
            # Determine number of packets to send based on RTT mode
            repeat_count = 3  # Default
            if high_rtt_mode:
                repeat_count = 10
            if extreme_rtt_mode:
                repeat_count = 20
                
            # Determine delay between packets based on RTT mode
            delay_time = 0.01  # Default
            if high_rtt_mode:
                delay_time = 0.02
            if extreme_rtt_mode:
                delay_time = 0.05
                
            # Send multiple termination packets to increase reliability
            for _ in range(repeat_count):
                sock.sendto(term_packet, (server_ip, server_port))
                time.sleep(delay_time)  # Small delay between sends, longer for high RTT
            
            # Wait for ACK with timeout adjusted for RTT
            termination_timeout = 1.0  # Default
            if high_rtt_mode:
                termination_timeout = 3.0
            if extreme_rtt_mode:
                termination_timeout = 5.0
                
            sock.settimeout(termination_timeout)
            ack, _ = sock.recvfrom(BUFFER_SIZE)
            ack_num = struct.unpack("!I", ack)[0]
            
            if ack_num == total_packets:
                ack_received = True
                print("Termination acknowledged!")
            else:
                retries += 1
        except socket.timeout:
            retries += 1
            print(f"Termination timeout! Retry {retries}/{MAX_RETRIES}")
            # Add a small delay for high RTT to let the network settle
            if extreme_rtt_mode:
                time.sleep(0.2)  # Longer delay for Case 6
            elif high_rtt_mode:
                time.sleep(0.1)  # Shorter delay for other high RTT cases
        except Exception as e:
            print(f"Error during termination: {e}")
            retries += 1
    
    end_time = time.time()
    duration = end_time - start_time
    
    sock.close()
    print(f"File transfer completed in {duration:.2f} seconds")
    return True

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python urft_client.py <file_path> <server_ip> <server_port>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    server_ip = sys.argv[2]
    server_port = int(sys.argv[3])
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist")
        sys.exit(1)
        
    success = send_file(file_path, server_ip, server_port)
    if not success:
        print("File transfer failed")
        sys.exit(1)