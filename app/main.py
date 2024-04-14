# Uncomment this to pass the first stage
import socket
import threading

DB = {}

def read_from_parts(msg):
    str_msg = msg.decode()
    tokens = str_msg.split("\r\n")
    print(f"tokens: {tokens}")
    parsed_msg = []
    for idx, token in enumerate(tokens):
        # bulk strings begin with $, so following value is the string
        if "*" in token:
            # array_token = tokens[idx+1]
            array_len = token[1:]
            print(f"array length: {array_len}")
        if "$" in token:
            bulk_string = tokens[idx + 1]
            print(f"bulk string: {bulk_string}")
            parsed_msg.append(bulk_string.lower())
    cmd = parsed_msg[0]
    msg_args = parsed_msg[1:]
    return cmd, msg_args

def handle_client(client):
    while True:
        data = client.recv(1024)
        
        if not data:
            break
        
        data_parts = iter(data.decode().split("\r\n"))
        command, args_data = read_from_parts(data_parts)
        
        if command == "ping":
            ping(client, args_data)
        elif cmd == "echo":
            echo(client, args_data)
        elif cmd == "get":
            get(client, args_data)
        elif cmd == "set":
            set(client, args_data)
        else:
            print("Command not supported!")
    
    client.close()

def ping(client, args) -> None:
    resp = b"+PONG\r\n"
    client.sendall(resp)

def echo(client, args) -> None:
    msg = " ".join(args)
    msg = f"+{msg}\r\n"  # format for simple string & CRLF
    resp = msg.encode()
    client.sendall(resp)

def set(client, args) -> None:
    key = args[0]
    val = args[1]
    try:
        DB[key] = val
    except:
        print("DB insert failed")
    resp = b"+OK\r\n"
    client.sendall(resp)

def get(client, args) -> None:
    key = args[0]
    try:
        val = DB[key]
    except:
        print(f"No value at key: {key}")
    msg = f"+{val}\r\n"
    resp = msg.encode()
    client.sendall(resp)

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
