with open("test_file.bin", "wb") as f:
    f.write(b"X" * (1024 * 1024))  # 1MiB test file