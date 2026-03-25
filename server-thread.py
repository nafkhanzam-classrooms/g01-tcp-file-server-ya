"""
server-thread.py  –  TCP File Server (Multi-threading)
Setiap client ditangani oleh thread tersendiri.
Broadcast menggunakan lock agar thread-safe.
"""

import socket
import threading
import os

HOST = '0.0.0.0'
PORT = 9090
BUFFER_SIZE = 4096
FILES_DIR = 'server_files'

os.makedirs(FILES_DIR, exist_ok=True)

# Daftar semua koneksi aktif (untuk broadcast)
clients: list[socket.socket] = []
clients_lock = threading.Lock()


def broadcast(message: str, sender: socket.socket):
    with clients_lock:
        for client in clients:
            if client is not sender:
                try:
                    client.send(message.encode())
                except:
                    pass


def receive_all(sock: socket.socket, length: int) -> bytes:
    data = b''
    while len(data) < length:
        chunk = sock.recv(min(BUFFER_SIZE, length - len(data)))
        if not chunk:
            raise ConnectionError("Koneksi terputus saat menerima data")
        data += chunk
    return data


def handle_client(conn: socket.socket, addr):
    print(f"[+] Client terhubung: {addr}")
    with clients_lock:
        clients.append(conn)

    try:
        while True:
            raw = conn.recv(BUFFER_SIZE)
            if not raw:
                break
            message = raw.decode(errors='replace').strip()
            print(f"[{addr}] {message[:80]}")

            # ── /list ────────────────────────────────────────────────────────
            if message == '/list':
                files = os.listdir(FILES_DIR)
                response = '\n'.join(files) if files else "(Tidak ada file di server)"
                conn.send(response.encode())

            # ── /upload <filename> <filesize> ────────────────────────────────
            elif message.startswith('/upload'):
                parts = message.split(maxsplit=2)
                if len(parts) < 3:
                    conn.send(b"ERROR Format salah. Gunakan: /upload <filename> <filesize>")
                    continue
                _, filename, filesize_str = parts
                filename = os.path.basename(filename)
                filesize = int(filesize_str)

                conn.send(b"READY")

                file_data = receive_all(conn, filesize)
                filepath = os.path.join(FILES_DIR, filename)
                with open(filepath, 'wb') as f:
                    f.write(file_data)

                response = f"File '{filename}' ({filesize} bytes) berhasil diupload."
                conn.send(response.encode())
                print(f"[Upload] {response}")
                broadcast(f"[Info] {addr} mengupload file '{filename}'", conn)

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

                ack = conn.recv(BUFFER_SIZE)
                if ack.strip() != b"READY":
                    continue

                with open(filepath, 'rb') as f:
                    conn.sendall(f.read())
                print(f"[Download] File '{filename}' dikirim ke {addr}.")

            # ── /quit ────────────────────────────────────────────────────────
            elif message == '/quit':
                print(f"[-] Client {addr} memutus koneksi.")
                break

            # ── pesan biasa / broadcast ───────────────────────────────────────
            else:
                conn.send(f"[Echo] {message}".encode())
                broadcast(f"[{addr}] {message}", conn)

    except (ConnectionResetError, ConnectionAbortedError, ConnectionError) as e:
        print(f"[!] Koneksi {addr} terputus: {e}")
    finally:
        with clients_lock:
            if conn in clients:
                clients.remove(conn)
        conn.close()
        print(f"[-] Koneksi {addr} ditutup.")


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(50)
    print(f"[Thread Server] Mendengarkan di {HOST}:{PORT} ...")

    try:
        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            thread.start()
            print(f"[Thread] Thread aktif: {threading.active_count() - 1}")
    except KeyboardInterrupt:
        print("\n[!] Server dihentikan.")
    finally:
        server.close()


if __name__ == '__main__':
    main()
