[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/mRmkZGKe)
# Network Programming - Assignment G01

## Anggota Kelompok
| Nama           | NRP        | Kelas     |
| ---            | ---        | ----------|
|    Denzel Daniels            |      5025241228      |     C      |
|                |            |           |

## Link Youtube (Unlisted)
Link ditaruh di bawah ini
```
https://youtu.be/5PBo4PuhSgA
```

## Penjelasan Program

### Penjelasan client.py

`client.py` adalah program sisi client yang dijalankan di terminal. Client terhubung ke server melalui TCP socket dan menyediakan antarmuka berbasis teks untuk mengirim perintah ke server.

Terdapat dua fungsi utama di dalam `client.py`:

- `receive_all(sock, length)` — menerima data sejumlah `length` bytes secara lengkap dari socket, menangani fragmentasi paket TCP.
- `main()` — loop utama yang membuat koneksi, membaca input user, dan mengirim/menerima data sesuai perintah.

Protokol komunikasi yang digunakan untuk setiap perintah adalah sebagai berikut:

- `/list` — client mengirim string `/list`, server membalas dengan daftar nama file.
- `/upload <filename>` — client mengirim header berisi nama dan ukuran file, menunggu `READY` dari server, lalu mengirim isi file.
- `/download <filename>` — client meminta file, server merespons dengan ukuran file, client membalas `READY`, lalu menerima isi file.
- `/quit` — client mengirim perintah quit dan menutup koneksi.

### Penjelasan Tiap Server

### server-sync.py — Synchronous (Sequential)

Server sinkron hanya dapat melayani **satu client dalam satu waktu**. Client berikutnya yang mencoba terhubung akan masuk ke antrian dan baru dilayani setelah client sebelumnya menutup koneksi dengan `/quit`. Implementasinya paling sederhana karena tidak memerlukan thread, select, maupun poll, namun tidak efisien jika terdapat banyak client.

```python
while True:
    conn, addr = server.accept()
    handle_client(conn, addr)   # blocking – satu client per satu
```

### server-select.py — I/O Multiplexing dengan select()

Server ini menggunakan modul `select` bawaan Python untuk memantau banyak socket sekaligus dalam **satu thread tunggal**. Ketika ada data masuk dari salah satu socket, server memrosesnya tanpa memblokir socket lainnya. Pendekatan ini bersifat cross-platform (Windows, macOS, Linux), namun terbatas pada sekitar 1024 file descriptor di beberapa sistem operasi.

```python
readable, _, exceptional = select.select(sockets_list, [], sockets_list, 1.0)

for s in readable:
    if s is server:
        conn, addr = server.accept()   # client baru
        sockets_list.append(conn)
    else:
        data = s.recv(BUFFER_SIZE)     # data dari client
        process_message(s, addr, data, ...)
```

---

### server-poll.py — I/O Multiplexing dengan poll()

Server ini menggunakan `select.poll()`, sebuah syscall Linux/macOS yang cara kerjanya mirip `select()` namun lebih efisien. Perbedaan utamanya adalah poll menggunakan event list sehingga tidak perlu mengiterasi semua file descriptor, dan tidak memiliki batas maksimum jumlah socket. Poll hanya tersedia di Linux dan macOS, tidak bisa digunakan di Windows.

```python
poller = select.poll()
poller.register(server.fileno(), select.POLLIN)

events = poller.poll(1000)  # timeout 1000 ms
for fd, event in events:
    if fd == server_fd:
        conn, addr = server.accept()
        poller.register(conn.fileno(), select.POLLIN)
    else:
        data = fd_to_socket[fd].recv(BUFFER_SIZE)
        process_message(fd, data, ...)
```

---

### server-thread.py — Multi-threading

Server ini menggunakan modul `threading` sehingga setiap client yang terhubung mendapatkan **thread tersendiri**. Semua client dilayani secara paralel dan dapat saling bertukar pesan melalui fitur broadcast. Untuk mencegah race condition saat mengakses daftar client, digunakan `threading.Lock`.

```python
conn, addr = server.accept()
thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
thread.start()
```

```python
def broadcast(message: str, sender: socket.socket):
    with clients_lock:          # thread-safe dengan Lock
        for client in clients:
            if client is not sender:
                client.send(message.encode())
```

- Cara Menjalankan

```bash
# Jalankan salah satu server
python3 server-sync.py
python3 server-select.py
python3 server-poll.py
python3 server-thread.py

# Di terminal lain, jalankan client
python3 client.py
```

Perintah yang tersedia di client:

```
/list                  → Tampilkan daftar file di server
/upload <filename>     → Upload file ke server
/download <filename>   → Download file dari server
/quit                  → Putus koneksi
<teks biasa>           → Kirim pesan broadcast ke semua client
```
## Screenshot Hasil
- server.py
<img width="1440" height="900" alt="Screenshot 2026-03-25 at 23 33 13" src="https://github.com/user-attachments/assets/8da1d433-314c-44e6-8338-85eef4cddb40" />

- sync.py
<img width="1440" height="900" alt="Screenshot 2026-03-25 at 23 36 10" src="https://github.com/user-attachments/assets/8556608d-fcf5-451f-adcb-edbd80413c74" />
<img width="1440" height="900" alt="Screenshot 2026-03-25 at 23 38 21" src="https://github.com/user-attachments/assets/e3739899-a7b6-4a38-8363-7baf0ada675d" />
<img width="1440" height="900" alt="Screenshot 2026-03-25 at 23 38 39" src="https://github.com/user-attachments/assets/78a329fb-5282-4084-8d38-57f16721344d" />

- select.py
<img width="1440" height="900" alt="Screenshot 2026-03-25 at 23 40 47" src="https://github.com/user-attachments/assets/7fbeb487-14d8-4ed3-a1b1-408d39807591" />

- poll.py
<img width="1440" height="900" alt="Screenshot 2026-03-25 at 23 17 29" src="https://github.com/user-attachments/assets/014711dd-44ef-495b-ace0-4eab9a5503e3" />
 
