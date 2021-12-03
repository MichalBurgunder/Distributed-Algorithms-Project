# import sys
# import socket
# print("Hello from {} with id {}".format(sys.argv[1], sys.argv[2]))
# print("Running on host:", socket.gethostname())

#!/usr/bin/env python
import os
import sys
import socket
import struct
import random
import time


def mcast_receiver(hostport):
  """create a multicast socket listening to the address"""
  print(hostport)
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
    'c_rnd' : 0,
    'c_val' : None,
    'rnd'  : 0
  }

  r = mcast_receiver(config['acceptors'])
  s = mcast_sender()
  while True:
    msg = r.recv(2**16)
    print(msg)
    # MICHAL:
    
    # v-round: comes from proposer
    # c-round: the last round the acceptor has participated in
    # v-value: The final value that needs be learned by the learners
    if msg:
        if msg.cRound:
          if not msg.vVal:
            # receiving initialization message (1a)
            # send the last round that has participated in
            s.sendto({"stage": "1b", "c_round": state.c_rnd}, config['proposers']) 
          else:
            # receiving actual values to accept
            # must by default send accept, unless the round given is lower than an already accepted round
            if msg.rnd < state.c_rnd:
              # round is smaller than the one participated in
              s.sendto('abort', config['proposers'])
            else:
              # received a valid message from the proposers
              # therefore, send proposers v-round & v-value
              s.sendto({"stage": "2b", "rnd": state.rnd, "c_val": state.c_val }, config['proposers'])


    # if msg
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
  print(config)
  r = mcast_receiver(config['proposers'])
  s = mcast_sender()
  pro_states = {
    'c_rnd' : 0,
    'c_val' : None,
    'rnd'  : 0,
    'v_rnd': 0,
    'v_val':None,
  }
  propose_times = 100
  in_propose = False
    # XINTAN:

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
     
     
    # Decision:
    # sends the agreed upon value to the learners

  while True:
    # msg = r.recv(2**16)
    # 1a
    if id == 0 and propose_times>0 and not in_propose:
      msg_1a = {'stage':'1a'}
      # randomly increase the c_rnd as initiate
      pro_states['c_rnd'] = pro_states['c_rnd'] + random.randint(0,5)
      msg_1a['c_rnd'] = pro_states['c_rnd']
      s.sendto(msg_1a, config['acceptors'])
      time.sleep(1)
      print("Send 1a message")
      print(msg_1a)
      propose_times = propose_times -1
      in_propose = True
    # 1b receive 1b,rnd,v_rnd,v_val
    #??? CHECK RECEIVE FORMAT
    msg = r.recv(2**16)
    if msg :
      print(msg)
      if msg['stage'] == '1b':
        print("received 1b message")
        pro_states['v_rnd'].append(msg['v_rnd'])
        pro_states['v_val'].append(msg['v_val'])
        k = max(pro_states['v_rnd']) # need to check the format
        k_index = pro_states['v_rnd'].index(k)
        v = 'init'
        if k == 0:
           pro_states['c_val'] = v
        else:
           pro_states['c_val'] = pro_states['v_val'][k_index]
        # 2a
        msg_2a = {}
        msg_2a['stage'] =  '2a'
        msg_2a['c_rnd'] = pro_states['c_rnd']
        msg_2a['c_val'] = pro_states['c_val']
        s.sendto(msg_2a, config['acceptors'])
        time.sleep(1)
        print("send 2a message")
        print(msg_2a)
      elif msg['stage'] == '2b'
        # 2b & decision
        msg_dec = {}
        msg_dec['stage'] = 'dec'
        pro_states['v_rnd'].append(msg['v_rnd'])
        pro_states['v_val'].append(msg['v_val'])
        if set(pro_states['v_rnd']) == set([pro_states['c_rnd']]):
          msg_dec['v_val'] = pro_states['c_val']
        else:
          msg_dec['v_val'] = ''
          s.sendto(msg_dec, config['learners'])
          time.sleep(1)
          print("send decision message")
          print(msg_dec)
          in_propose = False
      else:
        print("message received not 1b not 2b")
        print("repropse 1a stage")
        time.sleep(1)
        in_propose = False
        propose_times = propose_times + 1



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
  rolefunc =  None
  if role == 'acceptor':
    rolefunc = acceptor
  elif role == 'proposer':
    rolefunc = proposer
  elif role == 'learner':
    rolefunc = learner
  elif role == 'client':
    rolefunc = client
  rolefunc(config, id)

