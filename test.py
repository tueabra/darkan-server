import zmq

context = zmq.Context()

#socket1 = context.socket(zmq.REQ)
#socket1.connect("tcp://127.0.0.1:12345")

socket2 = context.socket(zmq.REQ)
socket2.connect("ipc:///tmp/darkan.sck")

def run(cmd, *args):
    socket2.send_json({'command': cmd, 'args': args})
    return socket2.recv_json()

def print_hosts(hosts, headline):
    print headline
    for host in hosts:
        print " ", host['id'], host['hostname']

"""
sockets = [socket1, socket2]

do_first = True
for x in ['dette', 'ere', 'en', 'test'] * 3:
    socket = sockets[do_first]
    do_first = not do_first

    socket.send_json({'data': x})
    msg = socket.recv_json()
    print msg
"""

print_hosts(run('hosts.list')['hosts'], 'Hosts')
ahosts = run('autohosts.list')
print_hosts(ahosts['hosts'], 'Autohosts')
if ahosts['hosts']:
    print run('autohosts.add', ahosts['hosts'][0]['id'])

print_hosts(run('hosts.list')['hosts'], 'Hosts')
print_hosts(run('autohosts.list')['hosts'], 'Autohosts')
