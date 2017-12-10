import sys,select
from threading import Thread
import threading
from messages import *
from agents import *
import time
import socket,pickle
import config


name_info = {}
port_info = {}
ip_info = {}
port2client = {}
client2name = {}



def parse_file(filename,processName):
	f = open(filename,'r')
	count = 1
	for line in f:
		a = line.split()
		if(a[0] == processName):
			name_info['server'] = processName
			port_info['server'] = int(a[1])
			ip_info['server'] = a[2]
		else:
			name_info['client'+str(count)] = a[0]
			port_info['client'+str(count)] = int(a[1])
			ip_info['client'+str(count)] = a[2]
			count = count + 1
	## To get the information about the client if we know the port number	
	for key,value in port_info.items():
		port2client[str(value)] = key
	for key,value in name_info.items():
		client2name[str(value)] = key


class console_thread(Thread):
	def __init__(self,name,consoleToProposerQueueLock):
		Thread.__init__(self)
		self.name = name
		self.consoleToProposerQueueLock = consoleToProposerQueueLock
		self.msgCount = 1 ## 0 is used for configuration message 
	
	def run(self):
		config.active = True 
		while(True):
  			line = sys.stdin.readline().strip()
			if(len(line.split()) > 0):
  				if (line.split()[0] == "Buy"):
					if(len(line.split()) == 2):
						value = int(line.split()[1])
					else:
						value = 1
					##print "Client has input Buy"
					self.msgCount =  self.msgCount + 1
					msgId = self.name+str(self.msgCount)	
					msg = clientMessage(self.name,time.time(),value,msgId)
					self.consoleToProposerQueueLock.acquire()
					config.consoleToProposerQueue.put(msg)
					self.consoleToProposerQueueLock.release()
				## To Quit the System
				elif (line.split()[0]=="Quit"):
					config.active = False
					break
				elif (line.split()[0]=="Sleep"):
					msg = "Sleep"
					config.consoleToServerQueue.put(msg)
				elif(line.split()[0] == "Show"):
					msg = "Show"
					config.consoleToStateMachineQueue.put(msg)
				
				else:
					print (self.name).upper() + ": Invalid input"	

class config_thread(Thread):

	def __init__(self,name,port,ip,configToProposerQueueLock):
		Thread.__init__(self)
		self.name = name
		self.port = port
		self.ip = ip
		self.configToProposerQueueLock = configToProposerQueueLock

	def run(self):
		config.server_socket = socket.socket()
		config.server_socket.bind((self.ip,self.port))
		config.server_socket.listen(6)

		config.client,config.addr = config.server_socket.accept()
		config.client_info = config.client.recv(1024)
		config.connections_made.append(name_info[port2client[config.client_info]])
		config.ref_client_info[str(port2client[config.client_info])] = config.client
		print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[config.client_info]] + " has been formed."
	

		config.client1,config.addr1 = config.server_socket.accept()
		config.client1_info = config.client1.recv(1024)
		config.connections_made.append(name_info[port2client[config.client1_info]])
		config.ref_client_info[str(port2client[config.client1_info])] = config.client1
		print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[config.client1_info]] + " has been formed."
	
		config.client2,config.addr2 = config.server_socket.accept()
		config.client2_info = config.client2.recv(1024)
		config.connections_made.append(name_info[port2client[config.client2_info]])
		config.ref_client_info[str(port2client[config.client2_info])] = config.client2
		print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[config.client2_info]] + " has been formed."
		

		config.client3,config.addr3 = config.server_socket.accept()
		config.client3_info = config.client3.recv(1024)
		config.connections_made.append(name_info[port2client[config.client3_info]])
		config.ref_client_info[str(port2client[config.client3_info])] = config.client3
		print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[config.client3_info]] + " has been formed."
		### Send this configuration message to the propser queue if you are the leader
		###if(self.name == config.currLeader):
		###	self.configToProposerQueueLock.acquire()
		###	config.configToProposerQueue.put(name_info[port2client[config.client3_info]])
		###	self.configToProposerQueueLock.release()

		config.client4,config.addr4 = config.server_socket.accept()
		config.client4_info = config.client4.recv(1024)
		config.connections_made.append(name_info[port2client[config.client4_info]])
		config.ref_client_info[str(port2client[config.client4_info])] = config.client4
		print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[config.client4_info]] + " has been formed."
		###if(self.name == config.currLeader):
		###	self.configToProposerQueueLock.acquire()
		###	config.configToProposerQueue.put(name_info[port2client[config.client4_info]])
		###	self.configToProposerQueueLock.release()

		##ref_client_info[str(port2client[self.client3_info])] = self.client3		
		##print config.ref_client_info
		##print name_info
		##print client2name





class server_thread(Thread):
	def __init__(self,name,port,ip,proposerToServerQueueLock,acceptorToServerQueueLock,learnerToServerQueueLock,stateMachineToServerQueueLock,configToProposerQueueLock):
		Thread.__init__(self)
		self.name = name
		self.port = port
		self.ip = ip
		self.proposerToServerQueueLock = proposerToServerQueueLock
		self.acceptorToServerQueueLock = acceptorToServerQueueLock
		self.learnerToServerQueueLock  = learnerToServerQueueLock
		self.stateMachineToServerQueueLock = stateMachineToServerQueueLock
		self.configToProposerQueueLock = configToProposerQueueLock

	def run(self):
		##self.invoke_server()
		self.send_info()
	
	def send_info(self):
		time.sleep(1)

		## Send the configuration message to its proposer so it can send the leader.
		self.configToProposerQueueLock.acquire()
		config.configToProposerQueue.put(self.name)
		self.configToProposerQueueLock.release()

		while(config.active):
			## This is just used for testing making the server sleep so other process thinks it is dead
			while(not config.consoleToServerQueue.empty()):
				msg = config.consoleToServerQueue.get()
				if (msg == "Sleep"):
					print "Sleep Started ..............."
					time.sleep(config.sleepTime)
					print "Sleep Ended ..............."
					config.proposerToServerQueue.queue.clear()
					config.acceptorToServerQueue.queue.clear()
					config.learnerToServerQueue.queue.clear()
					config.stateMachineToServerQueue.queue.clear()
					## Emptying all the queues connected to server


			## Have totally 4 queues which server needs to check

			## Checking the proposer to server queue the ID in the message is used to get corresponding socket of the receiveing end
			while(not config.proposerToServerQueue.empty()):
				##print "proposer put something to server"
				self.proposerToServerQueueLock.acquire()
				msg = config.proposerToServerQueue.get()
				try:
					config.ref_client_info[client2name[msg.recvId]].send(pickle.dumps(msg))
				except socket.error as msg:
					pass
				self.proposerToServerQueueLock.release()
				time.sleep(0)
			
			## Checking the acceptor to server queue the ID in the message is used to get corresponding socket of the receiveing end
			while(not config.acceptorToServerQueue.empty()):
				##print "acceptor put something to server"
				self.acceptorToServerQueueLock.acquire()
				msg = config.acceptorToServerQueue.get()
				try:
					config.ref_client_info[client2name[msg.recvId]].send(pickle.dumps(msg))
				except socket.error as msg:
					pass
				self.acceptorToServerQueueLock.release()
				time.sleep(0)


			## Checking the learner to server queue the ID in the message is used to get corresponding socket of the receiveing end
			while(not config.learnerToServerQueue.empty()):
				##print "learner put something to server"
				self.learnerToServerQueueLock.acquire()
				msg = config.learnerToServerQueue.get()
				try :
					config.ref_client_info[client2name[msg.recvId]].send(pickle.dumps(msg))
				except socket.error as msg:
					pass
				self.learnerToServerQueueLock.release()
				time.sleep(0)

			## Checking for the request made by state machine in case of missing log entries
			while(not config.stateMachineToServerQueue.empty()):
				##print "State Machine put something to server"
				self.stateMachineToServerQueueLock.acquire()
				msg = config.stateMachineToServerQueue.get()
				try :
					config.ref_client_info[client2name[msg.recvId]].send(pickle.dumps(msg))
				except socket.error as msg:
					pass
				self.stateMachineToServerQueueLock.release()
				time.sleep(0)
				
			time.sleep(0)


		## Sending Quit message to all the clients
		for names in config.connections_made:
			config.ref_client_info[client2name[names]].send(pickle.dumps("Quit"))
		config.server_socket.close()

			## Later for reconfiguration you can constantly check for the new connections


class client_thread(Thread):
	def __init__(self,name,port,ip,clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock):
		Thread.__init__(self)
		self.name = name
		self.port = port
		self.ip = ip
		self.clientToProposerQueueLock = clientToProposerQueueLock
		self.clientToAcceptorQueueLock = clientToAcceptorQueueLock
		self.clientToLearnerQueueLock  = clientToLearnerQueueLock
		#self.client_socket = socket.socket()


	def run(self):
		self.invoke_client()
		self.get_info()
		self.client_socket.close()
	
	def invoke_client(self):
		self.client_socket = socket.socket()
		#self.client_socket.setblocking(0)
		while (True):
			try:
				##print "waiting to connect to master"
				self.client_socket.connect(('127.0.0.1',self.port))
				self.client_socket.send(str(port_info['server']))
				break
			except socket.error as msg:
				continue


	def get_info(self):
		while(config.active):

		## depdending on the message type we put it on corresponding queue
			recvdMessage = None
			try:
				recvd = self.client_socket.recv(1024)
				recvdMessage = pickle.loads(recvd)
			except socket.error as m:
				pass
			except EOFError as m:
				pass
			##print "Client received a message"
			
					
			## Can be removed later
			if recvdMessage == "Quit":
				break

			if isinstance(recvdMessage,hearBeatMessage):
				##print "Process received HeartBeat Message from " + recvdMessage.leaderId
				config.prevRecvHeartBeat = time.time()
				config.currLeader = recvdMessage.leaderId

			## if received message is a message from proposer to acceptor for proposing value then send it to acceptor
			if isinstance(recvdMessage,sendProposedValueToAcceptors):
				##print "client received message from proposer to acceptor to accept value"
				self.clientToAcceptorQueueLock.acquire()
				config.clientToAcceptorQueue.put(recvdMessage)
				self.clientToAcceptorQueueLock.release()


			## if received message is a message from proposer to acceptor for proposing configuration then send it to acceptor
			if isinstance(recvdMessage,configurationMessageToAcceptors):
				##print "client received message from proposer to acceptor to accept configuration"
				self.clientToAcceptorQueueLock.acquire()
				config.clientToAcceptorQueue.put(recvdMessage)
				self.clientToAcceptorQueueLock.release()


			## if received message is a message from acceptor to learner to accept the configuration then send it to learner
			if isinstance(recvdMessage,configurationMessageToLearners):
				##print "client received message from acceptot to Learner to accept configuration"
				self.clientToLearnerQueueLock.acquire()
				config.clientToLearnerQueue.put(recvdMessage)
				self.clientToLearnerQueueLock.release()

			## if received message is a message from acceptor that it has accepted proposed value send it to the proposer
			if isinstance(recvdMessage,sendAcceptedValueToLeader):
				##print "client received message from acceptor to leader that it has accepted"
				self.clientToProposerQueueLock.acquire()
				config.clientToProposerQueue.put(recvdMessage)
				self.clientToProposerQueueLock.release()

			## if received message is a message from proposer to learner to write into log send it to learner
			if isinstance(recvdMessage,sendAcceptedValueToLearners):
				##print "client received message from acceptor to learner to accept values"
				self.clientToLearnerQueueLock.acquire()
				config.clientToLearnerQueue.put(recvdMessage)
				self.clientToLearnerQueueLock.release()

			## if received message is a message from another process and is a console message in that process
			if isinstance(recvdMessage,sendClientMessageToLeader):
				##print "client received console message from another process which is not the leader"
				self.clientToProposerQueueLock.acquire()
				config.clientToProposerQueue.put(recvdMessage)
				self.clientToProposerQueueLock.release()

			## if received message is a message from another process which wants to be leader
			if isinstance(recvdMessage,sendProposedLeaderToAcceptors):
				##print "client received message from another process which wants to be leader"
				self.clientToAcceptorQueueLock.acquire()
				config.clientToAcceptorQueue.put(recvdMessage)
				self.clientToAcceptorQueueLock.release()
				
			## if receives message is a message from another process which has accepted the current process to be leader
			if isinstance(recvdMessage,sendAcceptedLeaderToProposer):
				##print "client received message from another process which has accepted the current process to be leader"
				## Putting it on acceptor queue since the proposer thread is waiting for response
				self.clientToAcceptorQueueLock.acquire()
				config.clientToAcceptorQueue.put(recvdMessage)
				self.clientToAcceptorQueueLock.release()

			## if received a message from another process state machine for acquiring logs
			if isinstance(recvdMessage,sendRequestForLogEntries):
				##print "client received a message from annother process state machine for log entries.Putting the message in learner queue"
				self.clientToLearnerQueueLock.acquire()
				config.clientToLearnerQueue.put(recvdMessage)
				self.clientToLearnerQueueLock.release()

			## if received log entries message from another process, send it to the learner it will update
			if isinstance(recvdMessage,sendLogEntriesMessage):
				##print "Client received missing log entries from another process"
				self.clientToLearnerQueueLock.acquire()
				config.clientToLearnerQueue.put(recvdMessage)
				self.clientToLearnerQueueLock.release()

			## Configuration message received should be sent to leader. Therefor put the message in the proposer 
			if isinstance(recvdMessage,configurationMessageToProcess):
				##print "Received configuration message from newly added process"
				self.clientToProposerQueueLock.acquire()
				config.clientToProposerQueue.put(recvdMessage)
				self.clientToProposerQueueLock.release()

			## Configuration message which is sent to other process is now sent to the leader	
			if isinstance(recvdMessage,configurationMessageToLeader):
				##print "Received configuration message from existing process"
				self.clientToProposerQueueLock.acquire()
				config.clientToProposerQueue.put(recvdMessage)
				self.clientToProposerQueueLock.release()
	
			time.sleep(0)


def process(argv):
	
	parse_file(sys.argv[2],sys.argv[1])

	clientToProposerQueueLock  = threading.RLock()  
	clientToAcceptorQueueLock  = threading.RLock() 
	clientToLearnerQueueLock   = threading.RLock() 
	proposerToServerQueueLock  = threading.RLock()  
	acceptorToServerQueueLock  = threading.RLock()  
	learnerToServerQueueLock   = threading.RLock() 
	consoleToProposerQueueLock = threading.RLock()
	stateMachineToServerQueueLock = threading.RLock()
	configToProposerQueueLock = threading.RLock()	

	console = console_thread(name_info['server'],consoleToProposerQueueLock)
	server  = server_thread(name_info['server'],port_info['server'],ip_info['server'],proposerToServerQueueLock,acceptorToServerQueueLock,learnerToServerQueueLock,stateMachineToServerQueueLock,configToProposerQueueLock)
	client  = client_thread(name_info['server'],port_info['server'],ip_info['server'],clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock)   
	client1 = client_thread(name_info['client1'],port_info['client1'],ip_info['client1'],clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock)   
	client2 = client_thread(name_info['client2'],port_info['client2'],ip_info['client2'],clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock)   
	client3 = client_thread(name_info['client3'],port_info['client3'],ip_info['client3'],clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock)   
	client4 = client_thread(name_info['client4'],port_info['client4'],ip_info['client4'],clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock)   

	config = config_thread(name_info['server'],port_info['server'],ip_info['server'],configToProposerQueueLock)
	

	proposer = Proposer(name_info['server'],consoleToProposerQueueLock,proposerToServerQueueLock,clientToProposerQueueLock,configToProposerQueueLock,"Srinu")
	acceptor = Acceptor(name_info['server'],clientToAcceptorQueueLock,acceptorToServerQueueLock,"Srinu")
	learner  = Learner(name_info['server'],clientToLearnerQueueLock,learnerToServerQueueLock)

	stateMachine = StateMachine(name_info['server'],stateMachineToServerQueueLock)
	console.start()
	config.start()
	server.start()
	client.start()
	client1.start()
	client2.start()
	client3.start() 
	client4.start()
	proposer.start()
	acceptor.start()
	learner.start()
  	stateMachine.start() 



if __name__ == '__main__':
	process(sys.argv)

