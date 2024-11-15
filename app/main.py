import socket
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from threading import Timer

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

    def run(self) -> None:
        while True:
            data: bytes = self._connection.recv(1024)
            if not data:
                break
            logging.info("Received data: %s", data.decode())
            self.handle_command(data.decode().lower())

    def set(self, key: str, value: str, exp: int) -> str:
        self.storage[key] = value
        Timer(exp / 1000, self._clear_key, (key,)).start()
        return "+OK\r\n"
    
    def _clear_key(self, key: str)-> None:
        del self.storage[key]

    def get(self, key: str) -> str:
        return f"+{self.storage.get(key, 'nil')}\r\n"
    
    def parse_command(self, data: str) -> tuple[str, list[str]]:
        """
        Parse the Redis protocol command and extract the command and its arguments.
        The request is a bulk string with the following format:
        *<number of arguments>CRLF $<length of argument 1>CRLF <argument 1>CRLF ...
        For example, the command "SET key value" is represented as:
        *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n

        This function splits the data string and extracts the command and arguments.
        """
        parts = re.split(r'\s+', data)
        command = parts[2].lower()
        args = parts[4::2]
        return command, args

    def handle_command(self, raw_command: str) -> str:
        """
        Handle the Redis command and send the response,
        or return an error message if the command is not recognized
        """
        logging.info("Handling command: %s", raw_command)
        command, args = self.parse_command(raw_command)
        logging.info("Command: %s, Args: %s", command, args)
        if "ping" == command:
            pong: str = "+PONG\r\n"
            self._connection.sendall(pong.encode()) 
        elif "echo" == command:
            echo_message = args[0]
            echo: str = f"+{echo_message}\r\n"
            logging.info("Echoing data: %s", echo_message)
            self._connection.sendall(echo.encode())
        elif "set" == command:
            key, value = args[0], args[1]
            expiration = None
            if len(args) > 2 and args[2] == "px":
                expiration = int(args[3])
            response: str = self.set(key, value, expiration)
            self._connection.sendall(response.encode())
        elif "get" == command:
            key = args[0]
            response: str = self.get(key)
            logging.info("Sending response: %s", response)
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