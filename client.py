import socket
import os
import sys

HOST = '127.0.0.1'
PORT = 9090
BUFFER_SIZE = 4096


def receive_all(sock, length):
    data = b''
    while len(data) < length:
        chunk = sock.recv(min(BUFFER_SIZE, length - len(data)))
        if not chunk:
            raise ConnectionError("Connection closed while receiving data")
        data += chunk
    return data


def main():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((HOST, PORT))
    print(f"[Connected] Terhubung ke server {HOST}:{PORT}")
    print("Commands: /list | /upload <filename> | /download <filename> | /quit")

    try:
        while True:
            command = input("\n> ").strip()

            if not command:
                continue

            # ── QUIT ────────────────────────────────────────────────────────
            if command == '/quit':
                client.send(command.encode())
                print("[Disconnected] Koneksi ditutup.")
                break

            # ── LIST ─────────────────────────────────────────────────────────
            elif command == '/list':
                client.send(command.encode())
                response = client.recv(BUFFER_SIZE).decode()
                print("[Server Files]\n" + response)

            # ── UPLOAD ───────────────────────────────────────────────────────
            elif command.startswith('/upload'):
                parts = command.split(maxsplit=1)
                if len(parts) < 2:
                    print("[Error] Usage: /upload <filename>")
                    continue

                filename = parts[1]
                if not os.path.exists(filename):
                    print(f"[Error] File '{filename}' tidak ditemukan di lokal.")
                    continue

                filesize = os.path.getsize(filename)
                # Kirim header: /upload <filename> <filesize>
                header = f"/upload {filename} {filesize}"
                client.send(header.encode())

                # Tunggu ACK dari server
                ack = client.recv(BUFFER_SIZE).decode()
                if ack != "READY":
                    print(f"[Error] Server tidak siap: {ack}")
                    continue

                # Kirim isi file
                with open(filename, 'rb') as f:
                    sent = 0
                    while sent < filesize:
                        chunk = f.read(BUFFER_SIZE)
                        if not chunk:
                            break
                        client.sendall(chunk)
                        sent += len(chunk)

                response = client.recv(BUFFER_SIZE).decode()
                print(f"[Upload] {response}")

            # ── DOWNLOAD ─────────────────────────────────────────────────────
            elif command.startswith('/download'):
                parts = command.split(maxsplit=1)
                if len(parts) < 2:
                    print("[Error] Usage: /download <filename>")
                    continue

                filename = parts[1]
                client.send(command.encode())

                # Terima header balasan: OK <filesize> atau ERROR <msg>
                header = client.recv(BUFFER_SIZE).decode()
                if header.startswith("ERROR"):
                    print(f"[Download] {header}")
                    continue

                _, filesize_str = header.split(maxsplit=1)
                filesize = int(filesize_str)

                # Kirim ACK
                client.send(b"READY")

                # Terima isi file
                data = receive_all(client, filesize)
                with open(filename, 'wb') as f:
                    f.write(data)
                print(f"[Download] File '{filename}' ({filesize} bytes) berhasil diunduh.")

            # ── CHAT / BROADCAST ─────────────────────────────────────────────
            else:
                client.send(command.encode())
                response = client.recv(BUFFER_SIZE).decode()
                print(f"[Server] {response}")

    except (ConnectionResetError, ConnectionAbortedError):
        print("\n[Disconnected] Koneksi ke server terputus.")
    finally:
        client.close()


if __name__ == '__main__':
    main()
