import socket  # noqa: F401
import threading  # noqa: F401


def handle_client(connection: socket.socket, address: tuple[str, int]) -> None:
    with connection:
        print(f"Accecpted connection from {address}\n")
        while True:
            data: bytes = connection.recv(1024)
            if not data:
                break
            print(f"Received data: {data.decode()}")
            if "ping" in data.decode().lower():
                pong: str = "+PONG\r\n"
                connection.sendall(pong.encode())
def main():
    print("Logs from your program will appear here!\n")
    server_socket: socket.socket = socket.create_server(
        ("localhost", 6379), reuse_port=True
    )
    while True:
        try:
            connection: socket.socket
            address: tuple[str, int]
            connection, address = server_socket.accept()
            handle_client(connection, address)
            client_thread = threading.Thread(
                target=handle_client, args=[connection, address]
            )
            client_thread.start()
        except Exception as e:
            print(f"Exception: {e}")

if __name__ == "__main__":
    main()
