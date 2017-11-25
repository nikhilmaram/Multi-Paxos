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
ref_client_info = {}
port2client = {}
client2name = {}


##serverActive = True
##clientActive = True
##active = True


def parse_file(filename,processName):
	f = open(filename,'r')
	count = 1
	for line in f:
		a = line.split()
		if(a[0] == processName):
			name_info['server'] = processName
			port_info['server'] = int(a[1])
		else:
			name_info['client'+str(count)] = a[0]
			port_info['client'+str(count)] = int(a[1])
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
		self.msgCount = 0 
	
	def run(self):
		global serverActive
		global clientActive
		##global active
		config.active = True 
		while(True):
  			line = sys.stdin.readline().strip()
			if(len(line.split()) > 0):
  				if (line.split()[0] == "Buy"):
					if(len(line.split()) == 2):
						value = int(line.split()[1])
					else:
						value = 1
					print "Client has input Buy"
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
				
				else:
					print (self.name).upper() + ": Invalid input"	



class server_thread(Thread):
	def __init__(self,name,port,proposerToServerQueueLock,acceptorToServerQueueLock,learnerToServerQueueLock):
		Thread.__init__(self)
		self.name = name
		self.port = port
		self.proposerToServerQueueLock = proposerToServerQueueLock
		self.acceptorToServerQueueLock = acceptorToServerQueueLock
		self.learnerToServerQueueLock  = learnerToServerQueueLock

	def run(self):
		self.invoke_server()
		self.send_info()
		self.server_socket.close()


	def invoke_server(self):
		self.server_socket = socket.socket()
		self.server_socket.bind(('',self.port))
		self.server_socket.listen(5)
		
		self.client,self.addr = self.server_socket.accept()
		self.client_info = self.client.recv(1024)
		config.connections_made.append(name_info[port2client[self.client_info]])
		print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[self.client_info]] + " has been formed."
	

		self.client1,self.addr1 = self.server_socket.accept()
		self.client1_info = self.client1.recv(1024)
		config.connections_made.append(name_info[port2client[self.client1_info]])
		print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[self.client1_info]] + " has been formed."
	
		self.client2,self.addr2 = self.server_socket.accept()
		self.client2_info = self.client2.recv(1024)
		config.connections_made.append(name_info[port2client[self.client2_info]])
		print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[self.client2_info]] + " has been formed."

		###self.client3,self.addr3 = self.server_socket.accept()
		###self.client3_info = self.client3.recv(1024)
		###connections_made.append(name_info[port2client[self.client3_info]])
		###print (self.name).upper()+ ": Connection between "+self.name + " and " + name_info[port2client[self.client3_info]] + " has been formed."

		self.assign_clients()


	def assign_clients(self):
		ref_client_info[str(port2client[self.client_info])] = self.client
		ref_client_info[str(port2client[self.client1_info])] = self.client1
		ref_client_info[str(port2client[self.client2_info])] = self.client2
		##ref_client_info[str(port2client[self.client3_info])] = self.client3		
		print ref_client_info
		print name_info
		print client2name
	
	def send_info(self):
		while(config.active):
			## This is just used for testing making the server sleep so other process thinks it is dead
			while(not config.consoleToServerQueue.empty()):
				msg = config.consoleToServerQueue.get()
				if (msg == "Sleep"):
					print "Sleep Started ..............."
					time.sleep(100)
					print "Sleep Ended ..............."


			## Have totally 3 queues which server needs to check

			## Checking the proposer to server queue the ID in the message is used to get corresponding socket of the receiveing end
			while(not config.proposerToServerQueue.empty()):
				print "proposer put something to server"
				self.proposerToServerQueueLock.acquire()
				msg = config.proposerToServerQueue.get()
				ref_client_info[client2name[msg.recvId]].send(pickle.dumps(msg))
				self.proposerToServerQueueLock.release()
				time.sleep(0)
			
			## Checking the acceptor to server queue the ID in the message is used to get corresponding socket of the receiveing end
			while(not config.acceptorToServerQueue.empty()):
				print "acceptor put something to server"
				self.acceptorToServerQueueLock.acquire()
				msg = config.acceptorToServerQueue.get()
				ref_client_info[client2name[msg.recvId]].send(pickle.dumps(msg))
				self.acceptorToServerQueueLock.release()
				time.sleep(0)


			## Checking the learner to server queue the ID in the message is used to get corresponding socket of the receiveing end
			while(not config.learnerToServerQueue.empty()):
				print "learner put something to server"
				self.learnerToServerQueueLock.acquire()
				msg = config.learnerToServerQueue.get()
				ref_client_info[client2name[msg.recvId]].send(pickle.dumps(msg))
				self.learnerToServerQueueLock.release()
				time.sleep(0)
			time.sleep(0)


		## Sending Quit message to all the clients
		for names in config.connections_made:
			ref_client_info[client2name[names]].send(pickle.dumps("Quit"))

			## Later for reconfiguration you can constantly check for the new connections


class client_thread(Thread):
	def __init__(self,name,port,clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock):
		Thread.__init__(self)
		self.name = name
		self.port = port
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
			recvd = self.client_socket.recv(1024)
			recvdMessage = pickle.loads(recvd)
			print "Client received a message"
			
			## Can be removed later
			if recvdMessage == "Quit":
				break

			if isinstance(recvdMessage,hearBeatMessage):
				print "Process received HeartBeat Message from " + recvdMessage.leaderId
				config.prevRecvHeartBeat = time.time()

			## if received message is a message from proposer to acceptor for proposing value then send it to acceptor
			if isinstance(recvdMessage,sendProposedValueToAcceptors):
				print "client received message from proposer to acceptor to accept value"
				self.clientToAcceptorQueueLock.acquire()
				config.clientToAcceptorQueue.put(recvdMessage)
				self.clientToAcceptorQueueLock.release()

			## if received message is a message from acceptor that it has accepted proposed value send it to the proposer
			if isinstance(recvdMessage,sendAcceptedValueToLeader):
				print "client received message from acceptor to leader that it has accepted"
				self.clientToProposerQueueLock.acquire()
				config.clientToProposerQueue.put(recvdMessage)
				self.clientToProposerQueueLock.release()

			## if received message is a message from proposer to learner to write into log send it to learner
			if isinstance(recvdMessage,sendAcceptedValueToLearners):
				print "client received message from acceptor to learner to accept values"
				self.clientToLearnerQueueLock.acquire()
				config.clientToLearnerQueue.put(recvdMessage)
				self.clientToLearnerQueueLock.release()

			## if received message is a message from another process and is a console message in that process
			if isinstance(recvdMessage,sendClientMessageToLeader):
				print "client received console message from another process which is not the leader"
				self.clientToProposerQueueLock.acquire()
				config.clientToProposerQueue.put(recvdMessage)
				self.clientToProposerQueueLock.release()

			## if received message is a message from another process which wants to be leader
			if isinstance(recvdMessage,sendProposedLeaderToAcceptors):
				print "client received message from another process which wants to be leader"
				self.clientToAcceptorQueueLock.acquire()
				config.clientToAcceptorQueue.put(recvdMessage)
				self.clientToAcceptorQueueLock.release()
				
			## if receives message is a message from another process which has accepted the current process to be leader
			if isinstance(recvdMessage,sendAcceptedLeaderToProposer):
				print "client received message from another process which has accepted the current process to be leader"
				## Putting it on acceptor queue since the proposer thread is waiting for response
				self.clientToAcceptorQueueLock.acquire()
				config.clientToAcceptorQueue.put(recvdMessage)
				self.clientToAcceptorQueueLock.release()
	
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
	console = console_thread(name_info['server'],consoleToProposerQueueLock)
	server  = server_thread(name_info['server'],port_info['server'],proposerToServerQueueLock,acceptorToServerQueueLock,learnerToServerQueueLock)
	client  = client_thread(name_info['server'],port_info['server'],clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock)   
	client1 = client_thread(name_info['client1'],port_info['client1'],clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock)   
	client2 = client_thread(name_info['client2'],port_info['client2'],clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock)   
	##client3 = client(name_info['client3'],port_info['client3'],clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock)   
	##client4 = client(name_info['client4'],port_info['client4'],clientToProposerQueueLock,clientToAcceptorQueueLock,clientToLearnerQueueLock)   

	proposer = Proposer(name_info['server'],consoleToProposerQueueLock,proposerToServerQueueLock,clientToProposerQueueLock,30,"Srinu")
	acceptor = Acceptor(name_info['server'],clientToAcceptorQueueLock,acceptorToServerQueueLock,"Srinu")
	learner  = Learner(name_info['server'],clientToLearnerQueueLock)
	console.start()
	server.start()
	client.start()
	client1.start()
	client2.start()
	#client3.start();client4.start(),client5.start()
	proposer.start()
	acceptor.start()
	learner.start()
   



if __name__ == '__main__':
	process(sys.argv)

