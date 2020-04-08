# Lab 2
# Author: chongshi wang

from socket import *
import time
import sys

host = sys.argv[1]
port = sys.argv[2]

time_list = []

ping_number = 0

while ping_number < 10:

    try:

        socket_parameter = socket(AF_INET, SOCK_DGRAM)        
        socket_parameter.settimeout(1)
        before_time = time.time()
        before_message = ('PING' + ' ' + str(ping_number) + ' ' + str(before_time))
        before_address = (host,int(port))
        socket_parameter.sendto(before_message.encode(), before_address)
        after_essage, after_ddress = socket_parameter.recvfrom(3000)
        after_time = time.time()
        delay_time = (after_time - before_time) * 1000
        time_list.append(delay_time * delay_time)
        print('ping to {}, seq = {} , rtt = {:.2f} ms'.format(host,ping_number,delay_time))
    except timeout:
        print('ping to {}, seq = {} , timeout'.format(host,ping_number))

    ping_number = ping_number + 1
    
print('MAX: {:.2f} ms'.format(max(time_list)))
print('MIN: {:.2f} ms' .format(min(time_list)))
print('AVG: {:.2f} ms' .format(sum(time_list) / len(time_list)))
socket_parameter.close()
        
        
        

    
    
    
    

    





