"""
server-select.py  –  TCP File Server (I/O Multiplexing dengan select)
Menangani banyak client secara bersamaan menggunakan select().
"""

import socket
import select
import os

HOST = '0.0.0.0'
PORT = 9090
BUFFER_SIZE = 4096
FILES_DIR = 'server_files'

os.makedirs(FILES_DIR, exist_ok=True)

# State per-client untuk protokol multi-langkah (upload/download)
# { conn: { 'state': str, 'filename': str, 'filesize': int, 'received': bytes } }
client_state: dict = {}


def init_state():
    return {'state': 'idle', 'filename': '', 'filesize': 0, 'buffer': b''}


def process_message(conn, addr, raw: bytes, clients: list, server: socket.socket):
    """Proses satu pesan dari client; kembalikan False jika koneksi harus ditutup."""
    state = client_state.get(conn, init_state())
    message = raw.decode(errors='replace').strip()
    print(f"[{addr}] {message[:80]}")

    # ── sedang menerima data upload ──────────────────────────────────────────
    if state['state'] == 'uploading':
        state['buffer'] += raw
        if len(state['buffer']) >= state['filesize']:
            filepath = os.path.join(FILES_DIR, state['filename'])
            with open(filepath, 'wb') as f:
                f.write(state['buffer'][:state['filesize']])
            response = f"File '{state['filename']}' ({state['filesize']} bytes) berhasil diupload."
            conn.send(response.encode())
            print(f"[Upload] {response}")
            client_state[conn] = init_state()
        else:
            client_state[conn] = state
        return True

    # ── sedang menunggu ACK download ─────────────────────────────────────────
    if state['state'] == 'download_ack':
        if raw.strip() == b"READY":
            filepath = os.path.join(FILES_DIR, state['filename'])
            with open(filepath, 'rb') as f:
                file_data = f.read()
            conn.sendall(file_data)
            print(f"[Download] File '{state['filename']}' dikirim ke {addr}.")
        client_state[conn] = init_state()
        return True

    # ── perintah normal ──────────────────────────────────────────────────────
    if message == '/list':
        files = os.listdir(FILES_DIR)
        response = '\n'.join(files) if files else "(Tidak ada file di server)"
        conn.send(response.encode())

    elif message.startswith('/upload'):
        parts = message.split(maxsplit=2)
        if len(parts) < 3:
            conn.send(b"ERROR Format salah. Gunakan: /upload <filename> <filesize>")
            return True
        _, filename, filesize_str = parts
        state['filename'] = os.path.basename(filename)
        state['filesize'] = int(filesize_str)
        state['buffer'] = b''
        state['state'] = 'uploading'
        client_state[conn] = state
        conn.send(b"READY")

    elif message.startswith('/download'):
        parts = message.split(maxsplit=1)
        if len(parts) < 2:
            conn.send(b"ERROR Format salah")
            return True
        filename = os.path.basename(parts[1])
        filepath = os.path.join(FILES_DIR, filename)
        if not os.path.exists(filepath):
            conn.send(f"ERROR File '{filename}' tidak ditemukan".encode())
            return True
        filesize = os.path.getsize(filepath)
        state['filename'] = filename
        state['state'] = 'download_ack'
        client_state[conn] = state
        conn.send(f"OK {filesize}".encode())

    elif message == '/quit':
        return False

    else:
        # Broadcast ke semua client kecuali server dan pengirim
        broadcast = f"[{addr}] {message}"
        for c in clients:
            if c is not server and c is not conn:
                try:
                    c.send(broadcast.encode())
                except:
                    pass
        conn.send(f"[Echo] {message}".encode())

    return True


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(50)
    server.setblocking(False)
    print(f"[Select Server] Mendengarkan di {HOST}:{PORT} ...")

    sockets_list = [server]

    try:
        while True:
            readable, _, exceptional = select.select(sockets_list, [], sockets_list, 1.0)

            for s in readable:
                if s is server:
                    conn, addr = server.accept()
                    conn.setblocking(False)
                    sockets_list.append(conn)
                    client_state[conn] = init_state()
                    print(f"[+] Client terhubung: {addr}")
                else:
                    try:
                        data = s.recv(BUFFER_SIZE)
                        if not data:
                            raise ConnectionError("Client menutup koneksi")
                        addr = s.getpeername()
                        keep = process_message(s, addr, data, sockets_list, server)
                        if not keep:
                            raise ConnectionError("Client /quit")
                    except (ConnectionError, ConnectionResetError, OSError) as e:
                        try:
                            addr = s.getpeername()
                        except:
                            addr = "unknown"
                        print(f"[-] {addr} terputus: {e}")
                        sockets_list.remove(s)
                        client_state.pop(s, None)
                        s.close()

            for s in exceptional:
                sockets_list.remove(s)
                client_state.pop(s, None)
                s.close()

    except KeyboardInterrupt:
        print("\n[!] Server dihentikan.")
    finally:
        server.close()


if __name__ == '__main__':
    main()
