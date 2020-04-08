#Lab3
#Author: Chongshi Wang

import sys
import socket

#TCP
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('127.0.0.1', int(sys.argv[1])))
server.listen(1)

while True:
    connection, address = server.accept()
    try:
        information = connection.recv(3000)
        information = information.decode()
        information = information.replace("/","")
        fileName = information.split()[1]
        file = open(fileName,'rb')
        content = file.read()
        if("png" in fileName):
            response = "HTTP/1.1 200 OK\r\nContent_type: image/png\r\n\r\n"
        elif("html" in fileName):
            response = "HTTP/1.1 200 OK\r\nContent_type: text/html\r\n\r\n"
        encoded_response = response.encode()
        connection.sendall(encoded_response)
        connection.sendall(content)
        connection.close()
    except Exception:
        response = "HTTP/1.1 404 Error\r\n\r\n"
        content = "<h1>404 File dose not exist</h1>"
        encoded_response = response.encode()
        encoded_content = content.encode()
        connection.sendall(encoded_response)
        connection.sendall(encoded_content)
        connection.close()
