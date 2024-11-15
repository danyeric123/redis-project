import socket
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RedisServer:
    """
    A simple Redis server class
    with a basic implementation of the SET and GET commands
    using a dictionary to store key-value pairs
    It also does the basic PING and ECHO commands
    """

    def __init__(self, connection: socket.socket) -> None:
        self.storage = {}
        self._connection = connection
        self._commands = {
            "ping": self.ping,
            "echo": self.echo,
            "set": self.set,
            "get": self.get
        }

    def run(self) -> None:
        while True:
            data: bytes = self._connection.recv(1024)
            if not data:
                break
            logging.info("Received data: %s", data.decode())
            self.handle_command(data.decode().lower())

    def set(self, key: str, value: str) -> str:
        self.storage[key] = value
        return "+OK\r\n"

    def get(self, key: str) -> str:
        return f"+{self.storage.get(key, 'nil')}\r\n"

    def handle_command(self, command: str) -> str:
        """
        Handle the Redis command and send the response,
        or return an error message if the command is not recognized
        """
        logging.info("Handling command: %s", command)
        if "ping" in command:
            pong: str = "+PONG\r\n"
            self._connection.sendall(pong.encode()) 
        elif "echo" in command:
            echo_message = command.split()[-1]
            echo: str = f"+{echo_message}\r\n"
            logging.info("Echoing data: %s", echo_message)
            self._connection.sendall(echo.encode())
        elif "set" in command:
            key, value = command.split()[-3], command.split()[-1]
            response: str = self.set(key, value)
            self._connection.sendall(response.encode())
        elif "get" in command:
            key = command.split()[-1]
            response: str = self.get(key)
            self._connection.sendall(response.encode())
        else:
            error: str = "-ERR unknown command\r\n"
            self._connection.sendall(error.encode())


def handle_client(connection: socket.socket, address: tuple[str, int]) -> None:
    with connection:
        logging.info("Accepted connection from %s", address)
        try:
            server = RedisServer(connection)
            server.run()
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