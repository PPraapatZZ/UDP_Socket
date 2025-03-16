import os
import random

def create_test_file(filename, size_bytes):
    """Create a binary test file of the specified size."""
    # Ensure the directory exists
    dir_path = os.path.dirname(filename)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
    # Generate random binary data and write to file
    with open(filename, 'wb') as f:
        # Write data in chunks to handle large files efficiently
        chunk_size = 1024 * 64  # 64 KiB chunks
        remaining = size_bytes
        
        while remaining > 0:
            # Write either a full chunk or the remaining bytes
            current_chunk = min(chunk_size, remaining)
            data = random.randbytes(current_chunk)
            f.write(data)
            remaining -= current_chunk
    
    actual_size = os.path.getsize(filename)
    print(f"Created {filename} with size: {actual_size:,} bytes ({actual_size / (1024*1024):.2f} MiB)")

if __name__ == "__main__":
    filename = "test_file.bin"
    size_bytes = 1024 * 1024  # 1 MiB = 1,048,576 bytes
    
    create_test_file(filename, size_bytes)