import socket
import struct
from datetime import datetime

# Get current datetime
# https://stackoverflow.com/questions/36500197/python-get-time-from-ntp-server

def RequestTimefromNtp(addr='2.vn.pool.ntp.org'):
    REF_TIME_1970 = 2208988800      # Reference time
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    data = '\x1b' + 47 * '\0'
    client.sendto(bytes(data, encoding='utf8'), (addr, 123))
    data, address = client.recvfrom(1024)
    if data:
        t = struct.unpack('!12I', data)[10]
        t -= REF_TIME_1970
    return datetime.fromtimestamp(t)

# superset/tasks/schedule.py