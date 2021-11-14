# import sys
# import socket
# print("Hello from {} with id {}".format(sys.argv[1], sys.argv[2]))
# print("Running on host:", socket.gethostname())

#!/usr/bin/env python
import os
import sys
import socket
import struct


def mcast_receiver(hostport):
  """create a multicast socket listening to the address"""
  recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
  recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  recv_sock.bind(hostport)

  mcast_group = struct.pack("4sl", socket.inet_aton(hostport[0]), socket.INADDR_ANY)
  recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mcast_group)
  return recv_sock


def mcast_sender():
  """create a udp socket"""
  send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
  return send_sock


def parse_cfg(cfgpath):
  cfg = {}
  with open(cfgpath, 'r') as cfgfile:
    for line in cfgfile:
      (role, host, port) = line.split()
      cfg[role] = (host, int(port))
  return cfg


# ----------------------------------------------------


def acceptor(config, id):
  print '-> acceptor', id
  state = {
    v_round: 0,
    last_round_participated_in: 0,
    v_value : None,
  }

  r = mcast_receiver(config['acceptors'])
  s = mcast_sender()
  while True:
    msg = r.recv(2**16)
    
# v-round: comes from proposer
# round: the last round the acceptor has participated in
# v-value: The final value that needs be learned by the learners

    # 1a
    # receive c-round from proposer
    #   

    # 1b
    # send the current v-value & v-round & c-round to proposer
    # 

    # 2a
    # receive c-round, v-val from proposer
    # REQUIREMENTS:
    # cannot participate in round that is lower than the one it has already participated in
    # if receive c-message that has lower round, sends abort
    # if receive c-message that has a higher value, then it replaces the current -value with that c-message
    # If "accept": assign c-value, c-round to v-val, v-round


    # 2b
    # at every received message, sends a message to the propsers on whether they "accepted" or "aborted" (v-round, v-value)
    # 


    # fake acceptor! just forwards messages to the learner
    if id == 1:
      # print "acceptor: sending %s to learners" % (msg)
      s.sendto(msg, config['learners'])


def proposer(config, id):
  print '-> proposer', id
  r = mcast_receiver(config['proposers'])
  s = mcast_sender()

    # 1a
    # send c-rnd to all acceptors

    # 1b
    # receive all round, v-round, v-value
    # to move to around 2a, take the highest round number, and use that

    # 2a
    # REQUIREMENTS:
    # If one message is abort (message loss), go back to 1a with new (higher) round number


    # 2b
    # receive v-round, v-value
    # confirm that a majority of acceptors have accepted the proposed value
    # If yes, send v-val to all learners
    # If no, resend...? (?) CHECK, WE DONT KNOW

  # sends the agreed upon value to the learners

  while True:
    msg = r.recv(2**16)
    # fake proposer! just forwards message to the acceptor
    if id == 1:
      # print "proposer: sending %s to acceptors" % (msg)
      s.sendto(msg, config['acceptors'])


def learner(config, id):
  r = mcast_receiver(config['learners'])
  while True:
    msg = r.recv(2**16)
    print msg
    sys.stdout.flush()


def client(config, id):
  print '-> client ', id
  s = mcast_sender()
  for value in sys.stdin:
    value = value.strip()
    print "client: sending %s to proposers" % (value)
    s.sendto(value, config['proposers'])
  print 'client done.'


os.system('clear')
if __name__ == '__main__':
  cfgpath = sys.argv[1]
  config = parse_cfg(cfgpath)
  role = sys.argv[2]
  id = int(sys.argv[3])
  if role == 'acceptor':
    rolefunc = acceptor
  elif role == 'proposer':
    rolefunc = proposer
  elif role == 'learner':
    rolefunc = learner
  elif role == 'client':
    rolefunc = client
  rolefunc(config, id)

