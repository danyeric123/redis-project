import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client, addr = server_socket.accept()  # wait for client
    while client.recv(1024):
        # # Decode the bytes to a string and split by the delimiter
        # data_string = data.decode('utf-8')
        # # The last part is what we need
        # extracted_value = data_string.split('\r\n')[-2].upper()
        # if extracted_value == "PING":
        client.send(b"+PONG\r\n")

if __name__ == "__main__":
    main()
