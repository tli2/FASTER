import subprocess
import signal
import os
import socketserver
import socket
import sys

args = []
cluster = ["10.0.1.27", "10.0.1.28"]

def serversForExpSize(size):
  result = ''
  for i in range(int(size / 2)):
    result += cluster[i]
    result += ':11211,'
  return result[:-1]

class ExpHandler(socketserver.StreamRequestHandler):
  def handle(self):
    i = int(args[1])
    data = self.rfile.readline().strip()
    experiment_size = int(data)
    if (experiment_size = -1):
      if (i >= len(cluster) / 2):
        os.system('memaslap -s ' +  cluster[i - len(cluster) / 2] + ':11211 -T 16 -c 160 -F ~/memcached-scripts/memaslap.fill.cnf -x 250000000')
    if (i >= experiment_size):
      return 
    if (i >= experiment_size / 2):
      os.system('memaslap -s ' +  serversForExpSize(experiment_size) + '-T 16 -F ~/memcached-scripts/memaslap.run.cnf -d 1024 -t 30s'])
    else:
      return

if __name__ == "__main__":
    args = sys.argv[1:]
    if args[0] == 'worker':
      host = socket.gethostbyname(socket.gethostname())
      server = socketserver.TCPServer((host, 15000), ExpHandler)
      server.serve_forever()
    if args[0] == 'coordinator':
      experiment_size = args[1]
      socks = []
      for i in range(int(experiment_size / 2), experiment_size):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((cluster[i], 15000))
        s.send("{}\n".format(experiment_size).encode())
        socks.append(s)
      for s in socks:
        s.recv(1)
    if args[0] = 'setup':
      socks = []
      for i in range(int(experiment_size/2)):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((cluster[i], 15000))
        s.send("-1\n".encode())
        socks.append(s)
      for s in socks:
        s.recv(1)
