import socket
import os
import struct
import sys
import time
import hashlib

# Constants
BUFFER_SIZE = 4096
MAX_PACKET_HISTORY = 1000  # Track this many most recent packets to handle duplicates
SERVER_TIMEOUT = 120  # Increased timeout for Clumsy testing (especially Case 6)
MAX_REASONABLE_PACKETS = 100000  # Safety limit to prevent invalid packet counts

def receive_file(server_ip, server_port):
    start_time = time.time()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((server_ip, server_port))
    sock.settimeout(SERVER_TIMEOUT)  # Add timeout to prevent blocking indefinitely

    # Set socket buffer size to handle larger packets
    try:
        # Increase socket buffer sizes
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)  # 256 KB receive buffer
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 262144)  # 256 KB send buffer
    except:
        print("Warning: Couldn't set socket buffer size")
    
    print(f"Server listening on {server_ip}:{server_port}")
    print("NOTE: Configure and run Clumsy if testing network conditions")
    
    # Receive header
    header_received = False
    file_md5 = None  # Initialize MD5 field
    high_rtt_mode = False  # Track if we're in high RTT mode
    
    while not header_received:
        try:
            data, client_addr = sock.recvfrom(BUFFER_SIZE)
            
            # Check if this is an RTT probe
            if data == b"RTT_PROBE":
                sock.sendto(b"RTT_ACK", client_addr)
                # Record that we're likely in high RTT mode if we get multiple probes
                high_rtt_mode = True
                continue
                
            try:
                header_parts = data.decode().split(":")
                file_name = header_parts[0]
                file_size = int(header_parts[1])
                if len(header_parts) > 2:
                    file_md5 = header_parts[2]
                
                header_received = True
                print(f"Receiving file: {file_name} ({file_size} bytes)")
                if file_md5:
                    print(f"Expected MD5: {file_md5}")
                sock.sendto(b"HEADER_ACK", client_addr)
            except Exception as e:
                print(f"Invalid header received: {e}")
        except socket.timeout:
            print("Timed out waiting for header")
            return False
        except Exception as e:
            print(f"Error receiving header: {e}")
    
    # Calculate expected number of packets based on file size and chunk size
    expected_packets = (file_size + 1023) // 1024  # Ceiling division by chunk size
    print(f"Expecting approximately {expected_packets} packets based on file size")
    
    # Create output file
    output_file = "received_" + file_name
    
    # Set up packet tracking
    received_seq_nums = set()  # Track all received sequence numbers to detect duplicates
    received_packets = {}  # Buffer for out-of-order packets: seq_num -> data
    expected_seq = 0  # Next expected sequence number
    total_packets = 0  # Initialize total_packets before use
    transfer_complete = False
    
    # For tracking timeouts and high RTT conditions
    last_activity_time = time.time()
    last_progress_report = 0
    
    with open(output_file, "wb") as f:
        # Process data packets
        while not transfer_complete:
            try:
                data, client_addr = sock.recvfrom(BUFFER_SIZE)
                last_activity_time = time.time()  # Update activity timestamp
                
                # Handle RTT probe packets that might come at any time
                if data == b"RTT_PROBE":
                    sock.sendto(b"RTT_ACK", client_addr)
                    high_rtt_mode = True  # Enable high RTT mode
                    continue
                    
                # Extract sequence number and total packets
                header_size = struct.calcsize("!II")
                if len(data) < header_size:
                    print("Received malformed packet (too small)")
                    continue
                
                try:
                    seq_num, packet_total = struct.unpack("!II", data[:header_size])
                    data_chunk = data[header_size:]
                    
                    # Sanity check for packet_total (prevent absurdly large values)
                    if packet_total > MAX_REASONABLE_PACKETS or packet_total == 0:
                        # If packet total is unreasonable, use our expected value instead
                        if expected_packets > 0:
                            print(f"WARNING: Received invalid packet total: {packet_total}, using {expected_packets} instead")
                            packet_total = expected_packets
                        else:
                            print(f"WARNING: Received invalid packet total: {packet_total}, ignoring packet")
                            continue
                    
                    # Set total packets if not already set or validate against expected
                    if total_packets == 0:
                        total_packets = packet_total
                        print(f"Expecting {total_packets} packets based on packet header")
                        
                        # Sanity check against file size calculation
                        if abs(total_packets - expected_packets) > expected_packets * 0.5:
                            print(f"WARNING: Packet count ({total_packets}) differs significantly from expected ({expected_packets})")
                            # Trust the expected calculation if it's reasonable
                            if expected_packets > 0 and expected_packets < MAX_REASONABLE_PACKETS:
                                print(f"Using {expected_packets} as packet count based on file size")
                                total_packets = expected_packets
                    
                    # Termination packet
                    if seq_num == total_packets and packet_total == total_packets:
                        print("Received termination packet, sending acknowledgment")
                        # Send termination ACK multiple times for reliability
                        # More repeated ACKs for high RTT cases
                        repeat_count = 15 if high_rtt_mode else 5
                        delay_time = 0.05 if high_rtt_mode else 0.01
                        
                        for _ in range(repeat_count):
                            sock.sendto(struct.pack("!I", total_packets), client_addr)
                            time.sleep(delay_time)  # Longer delay for high RTT
                        
                        transfer_complete = True
                        break
                    
                    # Sanity check for sequence number
                    if seq_num >= total_packets:
                        print(f"WARNING: Received invalid sequence number: {seq_num} >= {total_packets}")
                        continue
                    
                    # Check if packet is a duplicate
                    if seq_num in received_seq_nums:
                        # Send ACK for duplicates without extra overhead
                        sock.sendto(struct.pack("!I", seq_num), client_addr)
                        continue
                    
                    # If we receive the packet we expect
                    if seq_num == expected_seq:
                        f.write(data_chunk)
                        received_seq_nums.add(seq_num)
                        expected_seq += 1
                        
                        # Process any buffered packets that are now in order
                        while expected_seq in received_packets:
                            f.write(received_packets[expected_seq])
                            received_seq_nums.add(expected_seq)
                            del received_packets[expected_seq]
                            expected_seq += 1
                        
                        # Report progress at reasonable intervals
                        if total_packets > 0:
                            current_progress = (expected_seq / total_packets) * 100
                            # Only print if progress has increased significantly
                            if current_progress - last_progress_report >= 5 or expected_seq >= total_packets:
                                print(f"Progress: {current_progress:.1f}% (Received: {seq_num})")
                                last_progress_report = current_progress
                    
                    # Out of order packet, buffer it
                    elif seq_num > expected_seq:
                        received_packets[seq_num] = data_chunk
                        received_seq_nums.add(seq_num)
                    
                    # Send ACK (single ACK for efficiency)
                    sock.sendto(struct.pack("!I", seq_num), client_addr)
                    
                except struct.error as e:
                    print(f"Error unpacking packet header: {e}")
                    continue
                
            except socket.timeout:
                current_time = time.time()
                # Adaptive timeout strategy for high RTT scenarios
                inactive_time = current_time - last_activity_time
                
                # Print periodic waiting messages for Case 6
                if int(inactive_time) > 0 and int(inactive_time) % 5 == 0:
                    print(f"Waiting for packets... (inactive for {int(inactive_time)}s)")
                
                # More lenient timeouts for high RTT mode
                if high_rtt_mode:
                    if inactive_time > SERVER_TIMEOUT * 2:
                        print(f"High RTT mode timeout ({inactive_time:.1f}s)")
                        return False
                else:
                    # Standard timeout for normal mode
                    if inactive_time > SERVER_TIMEOUT:
                        print(f"Server timeout waiting for packets ({inactive_time:.1f}s)")
                        return False
                
                continue  # Continue waiting for packets

            except socket.error as e:
                # Handle Windows-specific socket errors
                if hasattr(e, 'winerror') and e.winerror == 10054:  # Connection forcibly closed
                    print("WARNING: Connection reset by client. They may have terminated.")
                    # If we've received most of the file, try to complete anyway
                    if total_packets > 0 and expected_seq > total_packets * 0.9:
                        print(f"Attempting to complete transfer anyway (received {expected_seq}/{total_packets} packets)")
                        transfer_complete = True
                        break
                elif hasattr(e, 'winerror') and e.winerror == 10040:  # Buffer size error
                    print("WARNING: Received packet larger than buffer. Consider increasing BUFFER_SIZE.")
                else:
                    print(f"Socket error: {e}")
                continue
                
            except Exception as e:
                print(f"Error during file transfer: {e}")
                continue  # Try to continue despite errors
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Verify file size and MD5
    received_size = os.path.getsize(output_file)
    
    print(f"File transfer complete: {output_file}")
    print(f"Received {received_size} bytes in {duration:.2f} seconds")
    print(f"Effective transfer rate: {(received_size / 1024 / 1024) / duration:.2f} MB/s")
    
    # Verify with MD5 if available
    if file_md5:
        print("Verifying file integrity with MD5...")
        md5_hash = hashlib.md5()
        with open(output_file, "rb") as f_hash:
            for chunk in iter(lambda: f_hash.read(4096), b""):
                md5_hash.update(chunk)
        received_md5 = md5_hash.hexdigest()
        
        print(f"Computed MD5: {received_md5}")
        
        if received_md5 == file_md5:
            print("MD5 verification: SUCCESS")
            return True
        else:
            print(f"MD5 verification: FAILED")
            print(f"Expected: {file_md5}")
            print(f"Received: {received_md5}")
            return False
    
    # Fallback to size verification if no MD5
    if received_size == file_size:
        print("Size verification: SUCCESS")
        return True
    else:
        print(f"Size verification: FAILED (expected {file_size}, got {received_size})")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python urft_server.py <server_ip> <server_port>")
        sys.exit(1)
    
    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    
    success = receive_file(server_ip, server_port)
    if not success:
        print("File reception failed")
        sys.exit(1)