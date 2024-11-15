import socket
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def handle_client(connection: socket.socket, address: tuple[str, int]) -> None:
    with connection:
        logging.info("Accepted connection from %s", address)
        try:
            while True:
                data: bytes = connection.recv(1024)
                if not data:
                    break
                logging.info("Received data: %s", data.decode())
                if "ping" in data.decode().lower():
                    pong: str = "+PONG\r\n"
                    connection.sendall(pong.encode())
                elif "echo" in data.decode().lower():
                    echo_message = data.decode().split()[-1]
                    echo: str = f"+{echo_message}\r\n"
                    logging.info("Echoing data: %s", echo_message)
                    connection.sendall(echo.encode())
        except Exception as e:
            logging.error("Error handling client %s: %s", address, e)
        finally:
            logging.info("Connection from %s closed", address)


def main():
    logging.info("Logs from your program will appear here!")
    server_socket: socket.socket = socket.create_server(
        ("localhost", 6379), reuse_port=True
    )
    
    # Using ThreadPoolExecutor to manage a pool of threads
    # This allows us to limit the number of concurrent threads and reuse them
    with ThreadPoolExecutor(max_workers=10) as executor:
        try:
            while True:
                connection, address = server_socket.accept()
                # Submit the handle_client function to the thread pool
                # This ensures that the threads are managed efficiently
                executor.submit(handle_client, connection, address)
        except (socket.error, OSError) as e:
            logging.error("Server socket error: %s", e)
        finally:
            server_socket.close()
            logging.info("Server socket closed")


if __name__ == "__main__":
    main()