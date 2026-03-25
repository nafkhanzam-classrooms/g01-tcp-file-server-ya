"""
server-poll.py  –  TCP File Server (I/O Multiplexing dengan poll)
Menangani banyak client secara bersamaan menggunakan select.poll().
Catatan: poll tersedia di Linux/macOS, tidak tersedia di Windows.
"""

import socket
import select
import os

HOST = '0.0.0.0'
PORT = 9090
BUFFER_SIZE = 4096
FILES_DIR = 'server_files'

os.makedirs(FILES_DIR, exist_ok=True)

# Map fd → socket dan fd → state
fd_to_socket: dict[int, socket.socket] = {}
client_state: dict[int, dict] = {}


def init_state():
    return {'state': 'idle', 'filename': '', 'filesize': 0, 'buffer': b''}


def process_message(fd: int, raw: bytes, poller: select.poll, server_fd: int):
    """Proses pesan; kembalikan False jika koneksi harus ditutup."""
    conn = fd_to_socket[fd]
    addr = conn.getpeername()
    state = client_state.get(fd, init_state())

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
            client_state[fd] = init_state()
        else:
            client_state[fd] = state
        return True

    # ── sedang menunggu ACK download ─────────────────────────────────────────
    if state['state'] == 'download_ack':
        if raw.strip() == b"READY":
            filepath = os.path.join(FILES_DIR, state['filename'])
            with open(filepath, 'rb') as f:
                file_data = f.read()
            conn.sendall(file_data)
            print(f"[Download] File '{state['filename']}' dikirim ke {addr}.")
        client_state[fd] = init_state()
        return True

    message = raw.decode(errors='replace').strip()
    print(f"[{addr}] {message[:80]}")

    # ── /list ────────────────────────────────────────────────────────────────
    if message == '/list':
        files = os.listdir(FILES_DIR)
        response = '\n'.join(files) if files else "(Tidak ada file di server)"
        conn.send(response.encode())

    # ── /upload <filename> <filesize> ────────────────────────────────────────
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
        client_state[fd] = state
        conn.send(b"READY")

    # ── /download <filename> ─────────────────────────────────────────────────
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
        client_state[fd] = state
        conn.send(f"OK {filesize}".encode())

    # ── /quit ─────────────────────────────────────────────────────────────────
    elif message == '/quit':
        return False

    # ── pesan biasa / broadcast ───────────────────────────────────────────────
    else:
        broadcast = f"[{addr}] {message}"
        for other_fd, other_conn in fd_to_socket.items():
            if other_fd != server_fd and other_fd != fd:
                try:
                    other_conn.send(broadcast.encode())
                except:
                    pass
        conn.send(f"[Echo] {message}".encode())

    return True


def close_client(fd: int, poller: select.poll):
    poller.unregister(fd)
    conn = fd_to_socket.pop(fd, None)
    client_state.pop(fd, None)
    if conn:
        try:
            addr = conn.getpeername()
            print(f"[-] Client {addr} terputus.")
        except:
            pass
        conn.close()


def main():
    if not hasattr(select, 'poll'):
        print("[!] poll() tidak tersedia di sistem ini (Windows). Gunakan server-select.py.")
        return

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(50)
    server.setblocking(False)
    print(f"[Poll Server] Mendengarkan di {HOST}:{PORT} ...")

    poller = select.poll()
    poller.register(server.fileno(), select.POLLIN)
    fd_to_socket[server.fileno()] = server
    server_fd = server.fileno()

    try:
        while True:
            events = poller.poll(1000)  # timeout 1000 ms

            for fd, event in events:
                if event & (select.POLLERR | select.POLLHUP | select.POLLNVAL):
                    if fd != server_fd:
                        close_client(fd, poller)
                    continue

                if fd == server_fd:
                    conn, addr = server.accept()
                    conn.setblocking(False)
                    poller.register(conn.fileno(), select.POLLIN)
                    fd_to_socket[conn.fileno()] = conn
                    client_state[conn.fileno()] = init_state()
                    print(f"[+] Client terhubung: {addr}")
                else:
                    conn = fd_to_socket.get(fd)
                    if not conn:
                        continue
                    try:
                        data = conn.recv(BUFFER_SIZE)
                        if not data:
                            close_client(fd, poller)
                            continue
                        keep = process_message(fd, data, poller, server_fd)
                        if not keep:
                            close_client(fd, poller)
                    except (ConnectionResetError, OSError) as e:
                        print(f"[!] Error fd={fd}: {e}")
                        close_client(fd, poller)

    except KeyboardInterrupt:
        print("\n[!] Server dihentikan.")
    finally:
        server.close()


if __name__ == '__main__':
    main()
