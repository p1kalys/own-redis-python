# Uncomment this to pass the first stage
import socket
import threading
import time
from dataclasses import dataclass

@dataclass
class Record:
    data: str = None
    expiry: int = None


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

def handle_client(client, DB):
    while True:
        data = client.recv(1024)
        
        if not data:
            break
        
        command, args_data = read_from_parts(data)
        
        if command == "ping":
            ping(client, args_data)
        elif command == "echo":
            echo(client, args_data)
        elif command == "get":
            get(client, args_data, DB)
        elif command == "set":
            set(client, args_data, DB)
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

def set(client, args, DB) -> None:
    key = args[0]
    val = args[1]
    new_record = Record()
    
    if len(args) > 2:
        opt_args = [opt.lower() for opt in args]
        if "px" in opt_args:
            expiry = int(args[3])
            now_ms = int(round(time.time() * 1000))
            new_record.expiry = now_ms + expiry
    try:
        new_record.data = val
        DB[key] = new_record
    except:
        print("DB insert failed")
    resp = b"+OK\r\n"
    client.sendall(resp)

def get(client, args, DB) -> None:
    key = args[0]
    now = int(round(time.time() * 1000))

    try:
        val = DB[key]
        print(f"found {key}: {val}")

    except:
        resp = b"$-1\r\n"
        client.sendall(resp)
        return
    if val.expiry is not None and val.expiry < now:
        DB.pop(key)
        print("Message is expired")
        resp = b"$-1\r\n"
        client.sendall(resp)
        return

    msg = f"+{val.data}\r\n"
    resp = msg.encode()
    client.sendall(resp)

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    DB: dict[str, Record] = {}

    while True:
        client_socket, addr = server_socket.accept()
        print('Connection from', addr)
        client_thread = threading.Thread(target=handle_client, args=(client_socket,DB,))
        client_thread.start()


if __name__ == "__main__":
    main()
