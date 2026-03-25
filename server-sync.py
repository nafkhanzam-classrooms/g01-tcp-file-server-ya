"""
server-sync.py  –  TCP File Server (Synchronous / Sequential)
Menangani satu client sekaligus; client berikutnya menunggu giliran.
"""

import socket
import os

HOST = '0.0.0.0'
PORT = 9090
BUFFER_SIZE = 4096
FILES_DIR = 'server_files'

os.makedirs(FILES_DIR, exist_ok=True)


def send_all(sock, data: bytes):
    total = 0
    while total < len(data):
        sent = sock.send(data[total:])
        if sent == 0:
            raise ConnectionError("Socket connection broken")
        total += sent


def receive_all(sock, length: int) -> bytes:
    data = b''
    while len(data) < length:
        chunk = sock.recv(min(BUFFER_SIZE, length - len(data)))
        if not chunk:
            raise ConnectionError("Connection closed while receiving")
        data += chunk
    return data


def handle_client(conn, addr):
    print(f"[+] Client terhubung: {addr}")
    try:
        while True:
            raw = conn.recv(BUFFER_SIZE)
            if not raw:
                break
            message = raw.decode(errors='replace').strip()
            print(f"[{addr}] {message}")

            # ── /list ────────────────────────────────────────────────────────
            if message == '/list':
                files = os.listdir(FILES_DIR)
                response = '\n'.join(files) if files else "(Tidak ada file di server)"
                conn.send(response.encode())

            # ── /upload <filename> <filesize> ────────────────────────────────
            elif message.startswith('/upload'):
                parts = message.split(maxsplit=2)
                if len(parts) < 3:
                    conn.send(b"ERROR Format salah")
                    continue
                _, filename, filesize_str = parts
                filesize = int(filesize_str)
                filename = os.path.basename(filename)  # keamanan path

                conn.send(b"READY")

                file_data = receive_all(conn, filesize)
                filepath = os.path.join(FILES_DIR, filename)
                with open(filepath, 'wb') as f:
                    f.write(file_data)

                response = f"File '{filename}' ({filesize} bytes) berhasil diupload."
                conn.send(response.encode())
                print(f"[Upload] {response}")

            # ── /download <filename> ─────────────────────────────────────────
            elif message.startswith('/download'):
                parts = message.split(maxsplit=1)
                if len(parts) < 2:
                    conn.send(b"ERROR Format salah")
                    continue
                filename = os.path.basename(parts[1])
                filepath = os.path.join(FILES_DIR, filename)

                if not os.path.exists(filepath):
                    conn.send(f"ERROR File '{filename}' tidak ditemukan".encode())
                    continue

                filesize = os.path.getsize(filepath)
                conn.send(f"OK {filesize}".encode())

                # Tunggu ACK dari client
                ack = conn.recv(BUFFER_SIZE)
                if ack != b"READY":
                    continue

                with open(filepath, 'rb') as f:
                    send_all(conn, f.read())
                print(f"[Download] File '{filename}' dikirim ke {addr}.")

            # ── /quit ────────────────────────────────────────────────────────
            elif message == '/quit':
                print(f"[-] Client {addr} memutus koneksi.")
                break

            # ── pesan biasa ──────────────────────────────────────────────────
            else:
                conn.send(f"[Echo] {message}".encode())

    except (ConnectionResetError, ConnectionAbortedError, ConnectionError) as e:
        print(f"[!] Koneksi {addr} terputus: {e}")
    finally:
        conn.close()
        print(f"[-] Koneksi {addr} ditutup.")


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f"[Sync Server] Mendengarkan di {HOST}:{PORT} ...")

    try:
        while True:
            conn, addr = server.accept()
            handle_client(conn, addr)   # blocking – satu client per satu
    except KeyboardInterrupt:
        print("\n[!] Server dihentikan.")
    finally:
        server.close()


if __name__ == '__main__':
    main()
