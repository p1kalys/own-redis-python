# Uncomment this to pass the first stage
import socket
import threading

def handle_client(client):
    while True:
        data = client.recv(1024)
        if not data:
            break
        client.sendall(b"+PONG\r\n")
        client.close()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    while True:
        client_socket, addr = server_socket.accept()
        print('Connection from', addr)
        client_thread = threading.Thread(target=handle_client, args=(client_socket))
        client_thread.start()


if __name__ == "__main__":
    main()
