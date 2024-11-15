import argparse
import logging
import re
import socket
from concurrent.futures import ThreadPoolExecutor
from threading import Timer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class RedisServer:
    """
    A simple Redis server class
    with a basic implementation of the SET and GET commands
    using a dictionary to store key-value pairs
    It also does the basic PING and ECHO commands
    """

    def __init__(
        self, connection: socket.socket, dbfilename: str | None, directory: str | None
    ) -> None:
        self.storage: dict[str, str] = {}
        self._connection = connection
        self._config: dict[str, str] = {
            "dir": directory,
            "dbfilename": dbfilename,
        }

        self.command_dispatch = {
            "ping": self.ping,
            "echo": self.echo,
            "set": self.set,
            "get": self.get,
            "config": self.config,
        }

    def run(self) -> None:
        while True:
            data: bytes = self._connection.recv(1024)
            if not data:
                break
            logging.info("Received data: %s", data.decode())
            self.handle_command(data.decode().lower())

    def set(self, *args) -> None:
        key, value = args[0], args[1]
        expiration = None
        if len(args) > 2 and args[2] == "px":
            expiration = int(args[3])
        self.storage[key] = value
        if expiration is not None:
            Timer(expiration / 1000, self._clear_key, (key,)).start()
        self._connection.sendall("+OK\r\n".encode())

    def _clear_key(self, key: str) -> None:
        del self.storage[key]

    def get(self, *args) -> None:
        key = args[0]
        value = self.storage.get(key)
        response = "$-1\r\n"
        if value is not None:
            response = f"${len(value)}\r\n{value}\r\n"
        logging.info("Sending response: %s", response)
        self._connection.sendall(response.encode())

    def ping(self, *args) -> None:
        pong: str = "+PONG\r\n"
        self._connection.sendall(pong.encode())

    def echo(self, *args) -> None:
        echo_message = args[0]
        echo = f"+{echo_message}\r\n"
        logging.info("Echoing data: %s", echo_message)
        self._connection.sendall(echo.encode())

    def parse_command(self, data: str) -> tuple[str, list[str]]:
        """
        Parse the Redis protocol command and extract the command and its arguments.
        The request is a bulk string with the following format:
        *<number of arguments>CRLF $<length of argument 1>CRLF <argument 1>CRLF ...
        For example, the command "SET key value" is represented as:
        *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n

        This function splits the data string and extracts the command and arguments.
        """
        parts = re.split(r"\s+", data)
        command = parts[2].lower()
        args = parts[4::2]
        return command, args

    def handle_command(self, raw_command: str) -> None:
        """
        Handle the Redis command and send the response,
        or return an error message if the command is not recognized
        """
        logging.info("Handling command: %s", raw_command)
        command, args = self.parse_command(raw_command)
        logging.info("Command: %s, Args: %s", command, args)
        handler = self.command_dispatch.get(command, self.handle_unknown)
        handler(*args)

    def handle_unknown(self, *args) -> None:
        """
        Handle unknown commands
        """
        error = "-ERR unknown command\r\n"
        self._connection.sendall(error.encode())

    def config(self, *args) -> None:
        subcommand = args[0]
        if "get" == subcommand:
            response = self.config_get(args[1])
        elif "set" == subcommand:
            response = self.config_set(args[1], args[2])
        else:
            response = "-ERR unknown subcommand\r\n"
        self._connection.sendall(response.encode())

    def config_get(self, parameter: str) -> str:
        """
        Get the value of a configuration parameter
        """
        value = self._config.get(parameter)
        if value is None:
            return "$-1\r\n"
        return f"*2\r\n${len(parameter)}\r\n{parameter}\r\n${len(value)}\r\n{value}\r\n"

    def config_set(self, parameter: str, value: str) -> str:
        """
        Set the value of a configuration parameter
        """
        self._config[parameter] = value
        return "+OK\r\n"


def handle_client(
    connection: socket.socket,
    address: tuple[str, int],
    dbfilename: str | None,
    directory: str | None,
) -> None:
    with connection:
        logging.info("Accepted connection from %s", address)
        try:
            server = RedisServer(connection, dbfilename, directory)
            server.run()
        except Exception as e:
            logging.error("Error handling client %s: %s", address, e)
        finally:
            logging.info("Connection from %s closed", address)


def main():
    parser = argparse.ArgumentParser(description="Redis mock server")
    parser.add_argument(
        "--dir", required=False, help="Directory for the database files"
    )
    parser.add_argument("--dbfilename", required=False, help="Database filename")
    args = parser.parse_args()

    logging.info("Logs from your program will appear here!")
    if args.dir is not None and args.dbfilename is not None:
        logging.info("Using directory: %s", args.dir)
        logging.info("Using database filename: %s", args.dbfilename)
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
                executor.submit(
                    handle_client, connection, address, args.dbfilename, args.dir
                )
        except (socket.error, OSError) as e:
            logging.error("Server socket error: %s", e)
        finally:
            server_socket.close()
            logging.info("Server socket closed")


if __name__ == "__main__":
    main()
