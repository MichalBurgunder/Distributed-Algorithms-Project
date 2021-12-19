# import sys
# import socket
# print("Hello from {} with id {}".format(sys.argv[1], sys.argv[2]))
# print("Running on host:", socket.gethostname())

#!/usr/bin/env python
#from ctypes import _CVoidConstPLike
import sys
import socket
import struct
import ast

def mcast_receiver(hostport):
  """create a multicast socket listening to the address"""
  #print(hostport)
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
    
    
    # MICHAL:
    # v-round: comes from proposer
    # c-round: the last round the acceptor has participated in
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
def acceptor(config, id):
  print '-> acceptor', id
  state = {
    'rnd'  : 0,
    'v_rnd': 0,
    'v_val': 0
  }

  r = mcast_receiver(config['acceptors'])
  s = mcast_sender()
  in_vote = False
  while True:
    msg = r.recv(2**16)
    if msg:
      print("Acceptor ", id, " receive:", msg)
      msg = ast.literal_eval(msg)
      if msg['stage'] == '1a' and not in_vote:
        # receiving initialization message (1a)
        # check to see if -rnd is bigger than rnd
        s.sendto(str({'stage': 'announce','id':id}), config['proposers'])
        if msg['c_rnd'] < state['rnd']:
          print("ABORT! 1a msg_rnd received: " + str(msg['c_rnd']) + ", highest rnd so far: " + str(state['rnd']))
          s.sendto(str({'stage': 'abort'}), config['proposers'])
          msg = None
          pass
        else: 
          state['rnd'] = msg['c_rnd']
        # send the last round that has participated in
        s.sendto(str({"stage": "1b", "rnd": state['rnd'], 'v_rnd': state['v_rnd'], 'v_val': state['v_val']}), config['proposers'])
        in_vote = True  
      elif msg['stage'] == '2a' and in_vote:
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
          print("Acceptor",id,"send", str({'stage': '2b', 'v_rnd': state['v_rnd'], 'v_val': state['v_val'] }))
          state['rnd'] = 0
          state['v_rnd'] = 0
          state['v_val'] = 0 
          in_vote = False 
      else:
          # false message. shouldn't happen
          print("False message detected: You have likely coded something wrong")



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
def proposer(config, id):
  print '-> proposer', id
  if id == 1:
    r = mcast_receiver(config['proposers'])
    s = mcast_sender()
    rev_accp_states = []
    intial_v = None
    accp_list = []
    in_propose = False
    client_msg = []
    a_done = False
    b_done = False
    while True:
      msg = r.recv(2**16)
      msg = ast.literal_eval(msg)
      print("Proposer ",id, " receive:", msg)
      #sys.stdout.flush()
      if msg['stage'] == 'announce' and msg['id'] not in accp_list:
        accp_list.append(msg['id'])
        majority = len(accp_list)/2 + 1
        print("majority of accpetors",majority)
      elif msg['stage'] == '1a':
        if msg not in client_msg:
          client_msg.append(msg)
        print("Proposer",id,"client message:",client_msg)
      elif msg['stage'] == '1b' and a_done and not b_done:
        if msg['rnd'] == pro_states['c_rnd']:
          pro_states['v_rnd'].append(msg['v_rnd'])
          pro_states['v_val'].append(msg['v_val'])
        print("Proposer",id,"current v_rnd",pro_states['v_rnd'])
        print("Proposer",id,"current v_val",pro_states['v_val'])
        if len(pro_states['v_rnd']) >= majority and len(pro_states['v_val']) >= majority:
          k = max(pro_states['v_rnd']) 
          k_index = pro_states['v_rnd'].index(k)
          if k == 0:
            pro_states['c_val'] = intial_v
            print("Proposer",id,"current value is initial value",intial_v)
          else:
            pro_states['c_val'] = pro_states['v_val'][k_index]
            print("Proposer",id,"current value is proposed v value",pro_states['v_val'][k_index])
          # 2a
          msg_2a = {}
          msg_2a['stage'] =  '2a'
          msg_2a['c_rnd'] = pro_states['c_rnd']
          msg_2a['c_val'] = pro_states['c_val']
          s.sendto(str(msg_2a), config['acceptors'])
          print("Proposer",id,"send 2a message:",msg_2a)
          pro_states['v_rnd'] = []
          pro_states['v_val'] = []
          b_done = True
      elif msg['stage'] == '2b' and a_done and b_done:
        # 2b & decision
        msg_dec = {}
        msg_dec['stage'] = 'dec'
        pro_states['v_rnd2'].append(msg['v_rnd'])
        pro_states['v_val2'].append(msg['v_val'])
        print("Proposer v_rnd2",pro_states['v_rnd2'])
        print("Proposer v_val2",pro_states['v_val2'])
        if len(pro_states['v_rnd2']) >= majority and len(pro_states['v_val2']) >= majority:
          if set(pro_states['v_rnd2']) == set([pro_states['c_rnd']]):
            msg_dec['v_val'] = pro_states['c_val']
          else:
            msg_dec['v_val'] = ''
          s.sendto(str(msg_dec), config['learners'])
          # decide proposer
          print("send decision message:",msg_dec)
          client_msg.pop(0)
          print("Proposer",id,"remaining client message:",client_msg)
          in_propose = False
          a_done = False
          b_done = False
      elif msg['stage'] == 'abort':
        print("Abort, restart")
        in_propose = False
        a_done = False
        b_done = False
      if not in_propose and not a_done and not b_done and len(client_msg)>0:
        in_propose = True
        msg_client = client_msg[0]
        pro_states = {
          'c_rnd' : 0,
          'c_val' : 0,
          'rnd'  : 0,
          'v_val' : [],
          'v_rnd' : [],
          'v_val2' : [],
          'v_rnd2' : []
        }
        msg_1a = {'stage':'1a'}
        pro_states['c_rnd'] = pro_states['c_rnd'] + 1
        msg_1a['c_rnd'] = pro_states['c_rnd']
        intial_v = msg_client['v']
        s.sendto(str(msg_1a), config['acceptors'])
        print("Proposer ",id," send 1a message",msg_1a, "initial value", intial_v)
        a_done = True

def learner(config, id):
  r = mcast_receiver(config['learners'])
  while True:
    msg = r.recv(2**16)
    msg = ast.literal_eval(msg)
    #print("learners receive:",msg)
    if msg:
      print(msg['v_val'])
      msg = None
    sys.stdout.flush()


def client(config, id):
  print '-> client ', id
  s = mcast_sender()
  for value in sys.stdin:
    value = value.strip()
    print("Letting client sleep 1sec...")
    #time.sleep(1)
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