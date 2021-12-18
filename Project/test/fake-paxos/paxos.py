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
import json
import ast

def mcast_receiver(hostport):
  """create a multicast socket listening to the address"""
  # print(hostport)
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

def acceptor(config, id):
  print '-> acceptor', id
  state = {
    'rnd'  : 0,
    'v_rnd': 0,
    'v_val': 0
  }

  r = mcast_receiver(config['acceptors'])
  s = mcast_sender()
  while True:
    msg = r.recv(2**16)
    # MICHAL:
    # v-round: comes from proposer
    # c-round: the last round the acceptor has participated in
    # v-value: The final value that needs be learned by the learners
    if msg:
      print("Acceptor ", id, " receive:", msg)
      msg = ast.literal_eval(msg)
      if msg['stage'] == '1a':
        # receiving initialization message (1a)
        # check to see if -rnd is bigger than rnd
        if msg['c_rnd'] < state['rnd']:
          print("ABORT! 1a msg_rnd received: " + str(msg['c_rnd']) + ", highest rnd so far: " + str(state['rnd']))
          s.sendto(str({'stage': 'abort'}), config['proposers'])
          msg = None
          pass
        else: 
          state['rnd'] = msg['c_rnd']
        # send the last round that has participated in
        s.sendto(str({"stage": "1b", "rnd": state['rnd'], 'v_rnd': state['v_rnd'], 'v_val': state['v_val']}), config['proposers'])  
      elif msg['stage'] == '2a':
        # receiving actual values to accept
        # must by default send accept, unless the round given is lower than an already accepted round
        if msg['c_rnd'] < state['rnd']:
          # round is smaller than the one participated in
          print("ABORT! 2a msg_rnd received: " + str(msg['c_rnd']) + ", highest rnd so far: " + str(state['rnd']))
          s.sendto(str({'stage': 'abort'}), config['proposers'])
        else:
          # received a valid message from the proposers
          state['rnd'] = msg['c_rnd']
          state['v_rnd'] = msg['c_rnd']
          state['v_val'] = msg['c_val']
          # therefore, send proposers v-round & v-value
          s.sendto(str({'stage': '2b', 'v_rnd': state['v_rnd'], 'v_val': state['v_val'] }), config['proposers'])
      else:
          # false message. shouldn't happen
          print("False message detected: You have likely coded something wrong")


    
    # MICHAL:
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
    # fake acceptor! Let's for now do nothing
  # if id == 1:
      # print "acceptor: sending %s to learners" % (msg)
    # s.sendto(str(msg), config['learners'])


def proposer(config, id):
  print '-> proposer', id
  r = mcast_receiver(config['proposers'])
  s = mcast_sender()
  pro_states = {
    'c_rnd' : 0,
    'c_val' : 0,
    'rnd'  : 0,
    'v_val' : [],
    'v_rnd' : []
  }
  rev_accp_states = []
  intial_v = None
    # XINTAN:

    # 1a
    # send c-rnd to all acceptors

    # 1b
    # receive all round, v-round, v-value
    # to move to around 2a, take the highest round number, and use that

    # 2a
    # REQUIREMENTS:
    # If one message is abort (message loss), go back to 1a with new (higher) round number
    # send c_rnd, c_value

    # 2b
    # receive v-rund, v-value
    # confirm that a majority of acceptors have accepted the proposed value
    # If yes, send v-val to all learners
    # If no, resend...? (?) CHECK, WE DONT KNOW
     
    # Decision:
    # sends the agreed upon value to the learners
  in_propose = False
  propose_times = 10
  client_msg = []
  while True:
    msg = r.recv(2**16)
    msg = ast.literal_eval(msg)
    print("Proposer ", id, " receive:", msg)
    # 1a
    if msg['stage'] == '1a':
      if not in_propose:
        msg_1a = {'stage':'1a'}
        # randomly increase the c_rnd as initiate
        pro_states['c_rnd'] = pro_states['c_rnd'] + 1
        msg_1a['c_rnd'] = pro_states['c_rnd']
        intial_v = msg['v']
        print("sending 1a message",msg_1a)
        s.sendto(str(msg_1a), config['acceptors'])
        # print("sleeping 1s")
        # time.sleep(1)
        propose_times = propose_times -1
        in_propose = True
        # 1b receive 1b,rnd,v_rnd,v_val
        msg = r.recv(2**16)
      else:
        client_msg.append(msg['v'])
    elif msg['stage'] == '1b':
      print("received 1b message")
      pro_states['v_rnd'].append(msg['v_rnd'])
      pro_states['v_val'].append(msg['v_val'])
      # print(id,"proposer v_rnd",pro_states['v_rnd'])
      # print(id,"proposer v_val",pro_states['v_val'])
      k = max(pro_states['v_rnd']) # need to check the format
      k_index = pro_states['v_rnd'].index(k)
      if k == 0:
        pro_states['c_val'] = intial_v
        print("initial value")
      else:
        pro_states['c_val'] = pro_states['v_val'][k_index]
        print("biggest c value")
      # 2a
      msg_2a = {}
      msg_2a['stage'] =  '2a'
      msg_2a['c_rnd'] = pro_states['c_rnd']
      msg_2a['c_val'] = pro_states['c_val']
      print("sending 2a message:",msg_2a)
      s.sendto(str(msg_2a), config['acceptors'])
    elif msg['stage'] == '2b':
      # 2b & decision
      msg_dec = {}
      msg_dec['stage'] = 'dec'
      pro_states['v_rnd'].append(msg['v_rnd'])
      pro_states['v_val'].append(msg['v_val'])
      # print("Proposer v_rnd",pro_states['v_rnd'])
      # print("Proposer c_rnd",pro_states['c_rnd'])
      print("In proposer, at 2b stage. We made it!")
      if set(pro_states['v_rnd']) == set([pro_states['c_rnd']]):
        msg_dec['v_val'] = pro_states['c_val']
      else:
        msg_dec['v_val'] = ''
        s.sendto(str(msg['v_val']), config['learners'])
        #time.sleep(1)
        print("send decision message:",msg_dec)
        in_propose = False
    else:
      print("proposer received not 1a or 1b or 2b")
      print("Stop current propose, restart propose 1a")
      # time.sleep(0.1)
      in_propose = False
      propose_times = propose_times + 1

def learner(config, id):
  r = mcast_receiver(config['learners'])
  while True:
    msg = r.recv(2**16)
    if msg:
      print(msg)
      msg = None
    sys.stdout.flush()


def client(config, id):
  print '-> client ', id
  s = mcast_sender()
  for value in sys.stdin:
    value = value.strip()
    print("Letting client sleep 1sec...")
    time.sleep(1)
    print "client: sending %s to proposers" % (value)
    s.sendto(str({'stage':'1a', 'v':value,}), config['proposers'])
  print 'client done.'


#os.system('clear')
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

