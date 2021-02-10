import subprocess
import signal
import os
import socketserver
import socket
import sys

args = []
cluster = ["10.0.1.27", "10.0.1.28", "10.0.1.29", "10.0.1.30", "10.0.1.43", "10.0.1.32", "10.0.1.33", "10.0.1.34",
           "10.0.1.35", "10.0.1.36", "10.0.1.37", "10.0.1.38", "10.0.1.39", "10.0.1.40", "10.0.1.41", "10.0.1.42"]

def serversForExpSize(size):
  result = ''
  for i in range(int(size / 2)):
    result += cluster[i]
    result += ':11211,'
  return result[:-1]

class ExpHandler(socketserver.StreamRequestHandler):
  def handle(self):
    i = int(args[1])
    if (i < len(cluster) / 2):
      return;
    data = self.rfile.readline().decode().strip()
    experiment_size = int(data)
    if (experiment_size == -1):
      os.system('memaslap -s ' +  cluster[i - int(len(cluster) / 2)] + ':11211 -T 16 -c 160 -F ~/memcached-scripts/memaslap.fill.cnf -x 250000000')
    else:
      os.system('memaslap -s ' +  serversForExpSize(experiment_size) + ' -T 16 -F ~/memcached-scripts/memaslap.run.cnf -d 1024 -t 30s')

if __name__ == "__main__":
    args = sys.argv[1:]
    if args[0] == 'worker':
      host = socket.gethostbyname(socket.gethostname())
      server = socketserver.TCPServer((host, 15000), ExpHandler)
      server.serve_forever()
    if args[0] == 'coordinator':
      experiment_size = int(args[1])
      socks = []
      for i in cluster:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((i, 15000))
        s.send("{}\n".format(experiment_size).encode())
        socks.append(s)
      for s in socks:
        s.recv(1)
    if args[0] == 'setup':
      socks = []
      for i in cluster:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((i, 15000))
        s.send("-1\n".encode())
        socks.append(s)
      for s in socks:
        s.recv(1)
