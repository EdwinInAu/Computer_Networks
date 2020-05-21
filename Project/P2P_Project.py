#
# Author: Chongshi Wang
#
# P2P networks
#

import sys
from socket import *
import threading
import time
import pickle
import re


# data (sender peer id，receiver peer id，request or response or else, first or second successor peer, ping log for check loss, attach data like join peer id)
# Message standard format
class Message:
    def __init__(self, send_peer_id, receive_peer_id, message_type, peer_order=None, ping_log=None, data=None):
        self.send_peer_id = send_peer_id
        self.receive_peer_id = receive_peer_id
        self.message_type = message_type
        self.peer_order = peer_order
        self.ping_log = ping_log
        self.data = data


# Peer basic information: peer id, port number, transfer address and ping log used for check loss peer
class HostProfile:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.base_port = 12000
        self.port = self.base_port + self.peer_id
        self.address = ('localhost', self.port)
        self.ping_log = []


# main host class
class MainHost:

    # set current peer, first successor, second successor, first predecessor and second predecessor
    def __init__(self, current_peer_id=None, first_successor_id=None, second_successor_id=None, ping_interval=None):
        self.current_peer_id = None
        if current_peer_id:
            self.current_peer_id = int(current_peer_id)
        self.first_successor_id = None
        if first_successor_id:
            self.first_successor_id = int(first_successor_id)
        self.second_successor_id = None
        if second_successor_id:
            self.second_successor_id = int(second_successor_id)
        self.ping_interval = None
        if ping_interval:
            self.ping_interval = int(ping_interval)
        self.first_successor_peer = None
        self.second_successor_peer = None
        self.first_predecessor_peer = None
        self.second_predecessor_peer = None

    # step 3 join peer initialization
    # set join_peer, known peer, before updating the correct first and second successors, set that to join_peer first
    def join_host_init(self, join_peer_id, known_peer_id, ping_interval):
        self.join_peer_id = int(join_peer_id)
        self.known_peer_id = int(known_peer_id)
        self.ping_interval = int(ping_interval)
        self.join_peer = HostProfile(self.join_peer_id)
        self.known_peer = HostProfile(self.known_peer_id)
        self.current_peer = self.join_peer
        self.first_successor_peer = HostProfile(self.join_peer_id)
        self.second_successor_peer = HostProfile(self.join_peer_id)

    # send tcp message to known peer
    def join_send_message(self):

        send_socket = socket(AF_INET, SOCK_STREAM)
        send_socket.connect(self.known_peer.address)
        message = Message(self.join_peer.peer_id, self.known_peer.peer_id, 'join_request', None, None,
                          self.join_peer_id)
        # self.udp_receive_thread()
        # self.tcp_receive_thread()
        # self.ping_successors_thread()
        send_socket.send(pickle.dumps(message))
        send_socket.close()

    # step 6 Data insertion
    def data_insertion(self, command_line):
        command_line.replace("\n", '')
        data = command_line.split()
        # check input format: 4 digit number from 0000 to 9999
        if len(data) == 2:
            if len(data[1]) == 4 and bool(re.match('^[0-9]+$', data[1])):
                file_name = int(data[1])
                # print(file_name)
                if file_name % 256 == self.current_peer.peer_id:
                    print(f"Store {file_name} request accepted")
                else:
                    # if not meet, send message to first successor
                    send_socket = socket(AF_INET, SOCK_STREAM)
                    send_socket.connect(self.first_successor_peer.address)
                    message = Message(self.current_peer.peer_id, self.first_successor_peer.peer_id, 'store', None, None,
                                      file_name)
                    send_socket.send(pickle.dumps(message))
                    send_socket.close()
                    print(f"Store {file_name} request forwarded to my successor")
            else:
                print("Invalid file name format")
        else:
            print("Invalid file name format")

    # step 7 data retrieval
    def data_retrieval(self, command_line):
        command_line.replace("\n", '')
        data = command_line.split()
        # check input format: 4 digit number from 0000 to 9999
        if len(data) == 2:
            if len(data[1]) == 4 and bool(re.match('^[0-9]+$', data[1])):
                file_name = int(data[1])
                if self.first_predecessor_peer.peer_id < file_name % 256 < self.current_peer.peer_id:
                    print(f"File {file_name} is stored here")
                    print(f"File {file_name} received")
                else:
                    # if not meet, send message to first successor
                    print(f"File request for {file_name} has been sent to my successor")
                    send_socket = socket(AF_INET, SOCK_STREAM)
                    send_socket.connect(self.first_successor_peer.address)
                    message = Message(self.current_peer.peer_id, self.first_successor_peer.peer_id, 'retrieval', None,
                                      None, (file_name, self.current_peer.peer_id))
                    send_socket.send(pickle.dumps(message))
                    send_socket.close()
            else:
                print("Invalid file name format")
        else:
            print("Invalid file name format")

    # step 5 check loss peer
    def check_peer_abrupt_departure(self, current_successor_peer):
        successor_ping_log = current_successor_peer.ping_log
        # ping log list length > 3
        if len(successor_ping_log) < 3 or (self.first_successor_peer.peer_id == self.second_successor_peer.peer_id):
            return None
        else:
            # 2 consecutive 0 in the list: [1,1,1,0,0]
            zero_number = 0
            for i in range(-1, -3, -1):
                if successor_ping_log[i] == 0:
                    zero_number += 1
            if zero_number == 2:
                print(f"Peer {current_successor_peer.peer_id} is no longer alive")
                send_socket = socket(AF_INET, SOCK_STREAM)
                # first successor quit
                if current_successor_peer.peer_id == self.first_successor_peer.peer_id:
                    # send message to second successor
                    send_socket.connect(self.second_successor_peer.address)
                    message = Message(self.current_peer.peer_id, self.second_successor_peer.peer_id, 'lose_request',
                                      'second')
                    send_socket.send(pickle.dumps(message))
                    new_message = send_socket.recv(20482048)
                    new_message = pickle.loads(new_message)
                    # second successor becomes the first
                    # and second successor's first becomes the second
                    self.first_successor_peer = self.second_successor_peer
                    self.second_successor_peer = HostProfile(new_message.data)
                # second successor quit
                elif current_successor_peer.peer_id == self.second_successor_peer.peer_id:
                    # send message to first successor
                    send_socket.connect(self.first_successor_peer.address)
                    message = Message(self.current_peer.peer_id, self.first_successor_peer.peer_id, 'lose_request',
                                      'first')
                    send_socket.send(pickle.dumps(message))
                    new_message = send_socket.recv(20482048)
                    new_message = pickle.loads(new_message)
                    # first successor's new first successor becomes its new second successor
                    self.second_successor_peer = HostProfile(new_message.data)
                print(f"My new first successor is Peer {self.first_successor_peer.peer_id}")
                print(f"My new second successor is Peer {self.second_successor_peer.peer_id}")
                send_socket.close()

    # step 1 init peer
    def host_initialization(self):
        self.current_peer = HostProfile(self.current_peer_id)
        self.first_successor_peer = HostProfile(self.first_successor_id)
        self.second_successor_peer = HostProfile(self.second_successor_id)
        if self.first_successor_peer.peer_id != self.second_successor_peer.peer_id:
            print(f"Start peer {self.current_peer.peer_id} at port {self.current_peer.port}")
            print(
                f"Peer {self.current_peer.peer_id} can find first successor on port {self.first_successor_peer.port} and second successor "
                f"on port {self.second_successor_peer.port}.")

    # step 2 ping successor operation
    def ping_successors(self):
        while True:
            if self.first_successor_peer and self.second_successor_peer and (
                    self.first_successor_peer.peer_id != self.second_successor_peer.peer_id):
                # check whether its first and second successor depart abruptly
                self.check_peer_abrupt_departure(self.first_successor_peer)
                self.check_peer_abrupt_departure(self.second_successor_peer)
                # ping_log [0,0,0,0]
                self.first_successor_peer.ping_log.append(0)
                self.second_successor_peer.ping_log.append(0)
                print(
                    f'Ping requests sent to Peers {self.first_successor_peer.peer_id} and {self.second_successor_peer.peer_id}')
                first_message = Message(self.current_peer.peer_id, self.first_successor_peer.peer_id, 'request',
                                        'first',
                                        self.first_successor_peer.ping_log)
                second_message = Message(self.current_peer.peer_id, self.second_successor_peer.peer_id, 'request',
                                         'second',
                                         self.second_successor_peer.ping_log)
                self.udp_send_thread(first_message, self.first_successor_peer.address)
                self.udp_send_thread(second_message, self.second_successor_peer.address)
                # ping_interval == 30
                time.sleep(self.ping_interval)

    # ping successor thread
    def ping_successors_thread(self):
        ping_sender_thread = threading.Thread(target=self.ping_successors, name='Ping_Thread', daemon=True)
        ping_sender_thread.start()

    # udp send handler
    def udp_send_handler(self, message, address):
        send_socket = socket(AF_INET, SOCK_DGRAM)
        send_socket.sendto(pickle.dumps(message), address)
        send_socket.close()

    # udp send thread
    def udp_send_thread(self, message, address):
        send_thread = threading.Thread(target=self.udp_send_handler, args=(message, address), name='UDP_Send_Thread',
                                       daemon=True)
        send_thread.start()

    # udp receive handler
    def udp_receive_handler(self):
        receive_socket = socket(AF_INET, SOCK_DGRAM)
        receive_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        receive_socket.bind(self.current_peer.address)
        while True:
            message, address = receive_socket.recvfrom(20482048)
            message = pickle.loads(message)
            # if message type is request
            # example: peer 2 and peer 4 send ping request message to peer 5, peer 5 receives it now
            if message.message_type == 'request':
                receive_peer_id = HostProfile(message.send_peer_id).peer_id
                receive_address = HostProfile(message.send_peer_id).address
                # peer 4 becomes peer 5's first predecessor, and peer 5 sends response message to peer 4
                if message.peer_order == 'first':
                    self.first_predecessor_peer = HostProfile(message.send_peer_id)
                    new_message = Message(self.current_peer.peer_id, receive_peer_id, 'response', 'first',
                                          message.ping_log)
                # peer 2 becomes peer 5's second predecessor, and peer 5 sends response message to peer 2
                elif message.peer_order == 'second':
                    self.second_predecessor_peer = HostProfile(message.send_peer_id)
                    new_message = Message(self.current_peer.peer_id, receive_peer_id, 'response', 'second',
                                          message.ping_log)

                receive_socket.sendto(pickle.dumps(new_message), receive_address)
                # test
                if self.first_predecessor_peer and self.second_predecessor_peer:
                    print(
                        f'Ping request message received from Peer {message.send_peer_id}')
                else:
                    print(f'Ping request message received from Peer {message.send_peer_id}')
            # if message type is response
            # example: peer 4 and peer 5 send ping response message to peer 2, peer 2 receives it now
            elif message.message_type == 'response':
                # peer 2's first successor peer 4 send response message
                if message.peer_order == 'first' and message.send_peer_id == self.first_successor_peer.peer_id:
                    self.first_successor_peer.ping_log = message.ping_log
                    # change peer 4's ping log last element from 0 to 1: [1,1,1,1]
                    self.first_successor_peer.ping_log[-1] = 1
                # peer 2's second successor peer 5 send response message
                elif message.peer_order == 'second' and message.send_peer_id == self.second_successor_peer.peer_id:
                    self.second_successor_peer.ping_log = message.ping_log
                    # change peer 5's ping log last element from 0 to 1: [1,1,1,1]
                    self.second_successor_peer.ping_log[-1] = 1
                print(f'Ping response received from Peer {message.send_peer_id}')

    # udp receive thread
    def udp_receive_thread(self):
        receive_thread = threading.Thread(target=self.udp_receive_handler, name='TCP_Receive_Thread', daemon=True)
        receive_thread.start()

    # tcp send handler
    def tcp_send_handler(self, message, address):
        send_socket = socket(AF_INET, SOCK_STREAM)
        send_socket.settimeout(20)
        send_socket.connect(address)
        send_socket.send(pickle.dumps(message))
        send_socket.close()

    # tcp send thread
    def tcp_send_thread(self, message, address):
        send_thread = threading.Thread(target=self.tcp_send_handler, args=(message, address), name='TCP_Send_Thread',
                                       daemon=True)
        send_thread.start()

    # tcp receive handler
    def tcp_receive_handler(self):
        receive_socket = socket(AF_INET, SOCK_STREAM)
        receive_socket.bind(self.current_peer.address)
        receive_socket.listen(1)
        while True:
            connection, address = receive_socket.accept()
            message = connection.recv(20482048)
            message = pickle.loads(message)
            # if message type is lose_request
            # example: peer 14 departs abruptly
            if message.message_type == 'lose_request':
                # peer 14's second predecessor peer 8 sends lose request message to peer 8's first successor peer 9, peer 9 receives it now
                if message.peer_order == 'first':
                    # peer 9 sends lose response message to peer 8 with peer 9's second successor peer 19's id
                    new_message = Message(self.current_peer.peer_id, message.send_peer_id, 'lose_response', 'first',
                                          None, self.second_successor_peer.peer_id)
                # peer 14's first predecessor peer 9 sends lose request message to peer 9's second successor peer 19, peer 19 receives it now
                elif message.peer_order == 'second':
                    # peer 19 sends lose response message to peer 9 with peer 19's first successor peer 2's id
                    new_message = Message(self.current_peer.peer_id, message.send_peer_id, 'lose_response', 'second',
                                          None, self.first_successor_peer.peer_id)
                connection.sendall(pickle.dumps(new_message))
            # if message type is quit
            elif message.message_type == 'quit':
                # example peer 9 inputs Quit, peer 9 sends quit message to its first predecessor peer 8, peer 8 receives it now
                if message.peer_order == 'first':
                    self.first_successor_peer = self.second_successor_peer
                    # message with peer 9's second successor peer 19's id
                    # peer 8 sets its new second successor to 19
                    self.second_successor_peer = HostProfile(message.data)
                    print(f"Peer {message.send_peer_id} will depart from the network ")
                    print(f"My new first successor is Peer {self.first_successor_peer.peer_id}")
                    print(f"My new second successor is Peer {self.second_successor_peer.peer_id}")
                # example peer 9 inputs Quit, peer 9 sends quit message to its second predecessor peer 5, peer 5 receives it now
                elif message.peer_order == 'second':
                    # message with peer 9's second first peer 14's id
                    # peer 5 sets its new second successor to 14
                    self.second_successor_peer = HostProfile(message.data)
                    print(f"Peer {message.send_peer_id} will depart from the network ")
                    print(f"My new first successor is Peer {self.first_successor_peer.peer_id}")
                    print(f"My new second successor is Peer {self.second_successor_peer.peer_id}")
            # if message type is join request
            # example join peer 15 and known peer 4
            # known peer 4 receives message now
            elif message.message_type == 'join_request':
                # meet condition like: peer 14
                if self.current_peer.peer_id < message.data < self.first_successor_peer.peer_id or self.first_successor_peer.peer_id < self.current_peer.peer_id < message.data or message.data < self.first_successor_peer.peer_id < self.current_peer.peer_id:
                    join_peer_id = message.data
                    print(f"Peer {join_peer_id} Join request received")
                    join_peer = HostProfile(join_peer_id)
                    send_socket_a = socket(AF_INET, SOCK_STREAM)
                    send_socket_b = socket(AF_INET, SOCK_STREAM)
                    send_socket_a.connect(join_peer.address)
                    send_socket_b.connect(self.first_predecessor_peer.address)
                    # peer 14 sends join accept message to its new first successor peer 15
                    message_a = Message(self.current_peer.peer_id, join_peer.peer_id, 'join_accept', None, None,
                                        (self.first_successor_peer.peer_id, self.second_successor_peer.peer_id))
                    # peer 14 sends join change message to its first predecessor peer 9
                    message_b = Message(self.current_peer.peer_id, self.first_predecessor_peer.peer_id, 'join_change',
                                        None, None, join_peer_id)
                    send_socket_a.send(pickle.dumps(message_a))
                    send_socket_b.send(pickle.dumps(message_b))
                    send_socket_a.close()
                    send_socket_b.close()
                    self.second_successor_peer = self.first_successor_peer
                    self.first_successor_peer = join_peer
                    print(f"My new first successor is Peer {self.first_successor_peer.peer_id}")
                    print(f"My new second successor is Peer {self.second_successor_peer.peer_id}")
                else:
                    # forward join request message to its first successor with join peer 15's id
                    join_peer_id = message.data
                    print(f'Peer {join_peer_id} Join request forwarded to my successor')
                    send_socket = socket(AF_INET, SOCK_STREAM)
                    send_socket.connect(self.first_successor_peer.address)
                    message = Message(self.current_peer.peer_id, self.first_predecessor_peer.peer_id, 'join_request',
                                      None, None, join_peer_id)
                    send_socket.send(pickle.dumps(message))
                    send_socket.close()
            # if message type is join accept
            # example: peer 14 send join accept message to its new first successor peer 15
            elif message.message_type == 'join_accept':
                # message with peer 14's first and second successors id
                # peer 15 update
                first_successor_id = message.data[0]
                second_successor_id = message.data[1]
                self.first_successor_peer = HostProfile(first_successor_id)
                self.second_successor_peer = HostProfile(second_successor_id)
                print("Join request has been accepted")
                print(f"My first successor is Peer {first_successor_id}")
                print(f"My second successor is Peer {second_successor_id}")
            # if message type is join change
            # example: peer 14 send join change message to its first predecessor peer 9
            elif message.message_type == 'join_change':
                print(f"Successor Change request received")
                self.second_successor_peer = HostProfile(message.data)
                print(f"My new first successor is Peer {self.first_successor_peer.peer_id}")
                print(f"My new second successor is Peer {self.second_successor_peer.peer_id}")
            # if message type is store
            # step 6 receive message operation
            elif message.message_type == 'store':
                file_name = message.data
                if int(file_name) % 256 == self.current_peer.peer_id:
                    print(f"Store {file_name} request accepted")
                else:
                    message = Message(self.current_peer.peer_id, self.first_successor_peer.peer_id, 'store',
                                      None, None, file_name)
                    self.tcp_send_thread(message, self.first_successor_peer.address)

                    print(f"Store {file_name} request forwarded to my successor")

            # if message type is retrieval
            # step 7 receive message operation
            elif message.message_type == 'retrieval':
                file_name = message.data[0]
                if self.first_predecessor_peer.peer_id < file_name % 256 < self.current_peer.peer_id:
                    print(f"File {file_name} is stored here")
                    print(f"Sending file {file_name} to Peer {message.data[1]}")
                    print(f"The file has been sent")
                    send_peer = HostProfile(message.data[1])
                    send_socket = socket(AF_INET, SOCK_STREAM)
                    send_socket.connect(send_peer.address)
                    message = Message(self.current_peer.peer_id, send_peer.peer_id, 'retrieval_send', None,
                                      None, (file_name, message.data[1]))
                    send_socket.send(pickle.dumps(message))
                    send_socket.close()
                else:
                    print(f"Request for File {file_name} has been received, but the file is not stored here")
                    send_socket = socket(AF_INET, SOCK_STREAM)
                    send_socket.connect(self.first_successor_peer.address)
                    message = Message(self.current_peer.peer_id, self.first_successor_peer.peer_id, 'retrieval', None,
                                      None, (file_name, message.data[1]))
                    send_socket.send(pickle.dumps(message))
                    send_socket.close()
            # if message type is retrieval send
            # step 7 receive message operation
            elif message.message_type == 'retrieval_send':
                file_name = message.data[0]
                send_peer_id = HostProfile(message.send_peer_id).peer_id
                print(f"Peer {send_peer_id} had File {file_name}")
                print(f"Receiving File {file_name} from Peer {send_peer_id}")
                print(f"File {file_name} received")

    # tcp receive thread
    def tcp_receive_thread(self):
        receive_thread = threading.Thread(target=self.tcp_receive_handler, name='TCP_Receive_Thread', daemon=True)
        receive_thread.start()

    # step 4: peer departure graceful
    # example: peer 9 quit
    def processing_peer_graceful_departure(self):
        # peer 9 sends quit message to its first predecessor peer 8 with peer 9's second successor peer 19's id
        if self.first_predecessor_peer:
            send_socket = socket(AF_INET, SOCK_STREAM)
            send_socket.connect(self.first_predecessor_peer.address)
            message = Message(self.current_peer.peer_id, self.first_predecessor_peer.peer_id, 'quit', 'first', None,
                              self.second_successor_peer.peer_id)
            send_socket.send(pickle.dumps(message))
            send_socket.close()
        # peer 9 sends quit message to its second predecessor peer 5 with peer 9's first successor peer 14's id
        if self.second_successor_peer:
            send_socket = socket(AF_INET, SOCK_STREAM)
            send_socket.connect(self.second_predecessor_peer.address)
            message = Message(self.current_peer.peer_id, self.second_predecessor_peer.peer_id, 'quit', 'second', None,
                              self.first_successor_peer.peer_id)
            send_socket.send(pickle.dumps(message))
            send_socket.close()
        sys.exit(0)

    # check command line content
    def check_input(self):
        while True:
            command_line = sys.stdin.readline()
            # if input 'Quit'
            if command_line.startswith('Quit'):
                self.processing_peer_graceful_departure()
            # if input 'Store'
            elif command_line.startswith('Store'):
                self.data_insertion(command_line)
            # if input 'Request'
            elif command_line.startswith('Request'):
                self.data_retrieval(command_line)


# main function
def main():
    type = sys.argv[1]
    # when type is 'init
    if type == 'init':
        current_peer_id = sys.argv[2]
        first_successor_id = sys.argv[3]
        second_successor_id = sys.argv[4]
        ping_interval = sys.argv[5]
        # peer initialization
        host = MainHost(current_peer_id, first_successor_id, second_successor_id, ping_interval)
        host.host_initialization()
        host.udp_receive_thread()
        host.tcp_receive_thread()
        host.ping_successors_thread()
        host.check_input()
    # when type is 'join
    elif type == 'join':
        join_peer_id = sys.argv[2]
        known_peer_id = sys.argv[3]
        ping_interval = sys.argv[4]
        host = MainHost()
        # join peer initialization
        host.join_host_init(join_peer_id, known_peer_id, ping_interval)
        host.udp_receive_thread()
        host.tcp_receive_thread()
        host.ping_successors_thread()
        host.join_send_message()
        host.check_input()


if __name__ == "__main__":
    main()
