# Uncomment this to pass the first stage
import socket
import threading

def read_from_parts(parts):
    stack = []
    while part := next(parts):
        if part[0] == "*":
            length = int(part[1::])
            arr = []
            for i in range(length):
                middle = read_from_parts(parts)
                arr.append(middle)
            return arr
        elif part[0] == "$":
            length = int(part[1::])
            if length == 1:
                return None
            part_string = next(parts)
            return part_string
        else:
            return Exception("Invalid protocol")

def handle_client(client):
    while True:
        data = client.recv(1024)
        if not data:
            break
        data_parts = iter(data.decode().split("\r\n"))
        commands = read_from_parts(data_parts)
        if commands[0].lower() == "ping":
            response_data = b"+PONG\r\n"
            client.sendall(response_data)
        elif commands[0].lower() == "echo" and len(commands) > 1:
            response_data = f"+{commands[1]}\r\n".encode()
            client.sendall(response_data)
    
    client.close()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    while True:
        client_socket, addr = server_socket.accept()
        print('Connection from', addr)
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()


if __name__ == "__main__":
    main()
