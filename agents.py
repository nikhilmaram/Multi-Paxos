from threading import Thread
from messages import *
from protocol import *
import config
import time

import Queue as queue


class Agent:
	## Base class for Proposer, Acceptor, Leraner
	def __init__(self,processId,active):
		self.pid = processId
		self.active = active


class Proposer(Thread):
	def __init__(self,pid,consoleToProposerQueueLock,proposerToServerQueueLock,clientToProposerQueueLock,tolerance,currLeader):
		Thread.__init__(self)
		self.pid = pid
		self.consoleToProposerQueueLock = consoleToProposerQueueLock
		self.proposerToServerQueueLock  = proposerToServerQueueLock
		self.clientToProposerQueueLock  = clientToProposerQueueLock
		self.tolerance = tolerance
		##self.currLogEntry = 0
		self.prevAcceptedLogEntry = 0
		self.Active = True
		self.instances = {} ## gives details about each log entry
		self.prevHeartBeat = time.time()


	def run(self):
		while(config.active):
			## Need to check if I have received anything from the console queue
			## If requested propose value to everyone
			while(not config.consoleToProposerQueue.empty()):
				print "proposer received a command from console"
				self.consoleToProposerQueueLock.acquire()
				msg = config.consoleToProposerQueue.get()
				self.consoleToProposerQueueLock.release()

				if (config.currLeader == self.pid):
					## Request came from the console which the leader is present
					print "Current Leader got the client request"
					config.currLogEntry = config.currLogEntry+1
					self.handle_client_request(msg)
				else:
					## Request came from the console which is not the leader so send it to the leader
					print "Send the client request to the corresponding leader"
					## putting the message in the request to Leader Queue because when the Leader is down
					## and two servers compete for the leader, only 1 server wins. The message from the lost server
					## need to be sent to the leader after the time out.

					## The learner will pop the item if the item is still present after timeout the message is sent again. 
					config.requestLeaderLock.acquire()
					config.requestSentToLeaderQueue.append(msg)
					config.requestLeaderLock.release()
					self.leader_check(msg)
				time.sleep(0)	

			while(not config.clientToProposerQueue.empty()):
				print "proposer received a command from client"
				self.clientToProposerQueueLock.acquire()
				msg = config.clientToProposerQueue.get()
				self.clientToProposerQueueLock.release()
				if isinstance(msg,sendAcceptedValueToLeader):
					## Message from another acceptor to the leader
					self.handle_accepted_value_from_acceptor(msg)
				if isinstance(msg,sendClientMessageToLeader):
					## Console message obtained from other process which is not the leader
					print "Current Leader has got the console message from another process"
					config.currLogEntry = config.currLogEntry + 1
					self.handle_client_request(msg.clientMsg)
				time.sleep(0)	

			if(len(config.requestSentToLeaderQueue) > 0):
				##print "Checking if request retry is needed"
				msg = config.requestSentToLeaderQueue[0]
				if(time.time() - msg.timeStamp > 20):
					print "No response yet from the leader to the message"
					### Before retrying check if the leader is still intact
					msg.timeStamp = time.time()
					self.leader_check(msg)
					
			self.send_hearbeat()
			time.sleep(0)


	def handle_client_request(self,msg):
		## Proposer gets a client request
		## For now we assume there is no checking of available log entries for multi paxos
		## The Proposer fixes on a log Entry and runs the Basic Paxos Protocol
		if config.currLogEntry not in self.instances.keys():
			self.instances[config.currLogEntry] = PaxosProposerProtocol(self)
		self.instances[config.currLogEntry].sendProposedValueToAllAcceptors(msg)


	def handle_accepted_value_from_acceptor(self,msg):
		self.instances[msg.logEntry].accepted_value_from_acceptor(msg)

	def send_to_current_leader(self,msg):
   		self.proposerToServerQueueLock.acquire()
   		config.proposerToServerQueue.put(msg)
   		self.proposerToServerQueueLock.release()	

	def leader_check(self,msg):
		if(config.prevRecvHeartBeat + 10 > time.time()):
			print "Leader is still intact"
			sendMsg = sendClientMessageToLeader(msg,config.currLeader)
			self.send_to_current_leader(sendMsg)
		else:	
			print "Leader is down"
			config.currLeader = None
			config.phase1Leader = None
			config.currLogEntry = config.currLogEntry + 1
			self.select_leader(msg)	


	def select_leader(self,msg):
		## Combining the phase 1 and phase 2 of paxos 
		print "Process is initiating the start of the leader - Phase 1"
		if config.currLogEntry not in self.instances.keys():
			self.instances[config.currLogEntry] = PaxosProposerProtocol(self)
		self.instances[config.currLogEntry].sendProposedLeaderToAllAcceptors()
		print "Please Wait......"
		time.sleep(15)
		while(config.phase1Leader == None and config.currLeader == None):
			continue
		print "Checking after time out"
		if(config.phase1Leader == self.pid):
			print "Current process has been chosen as a leader after time out"
			self.handle_client_request(msg)
		else :
			print "Current process has not be chosen as a leader"
		
	
	def send_hearbeat(self):
		## Sending Heartbeat
		if(config.currLeader == self.pid):
			if (config.prevSentHeartBeat + 10 < time.time()):
				config.prevSentHeartBeat = time.time()
				print "Sending HeartBeat to all the acceptors"
				self.proposerToServerQueueLock.acquire()
				for recvId in config.connections_made :
					config.proposerToServerQueue.put(hearBeatMessage(self.pid,recvId))
				self.proposerToServerQueueLock.release() 




class Acceptor(Thread):
	def __init__(self,pid,clientToAcceptorQueueLock,acceptorToServerQueueLock,leaderId):
		Thread.__init__(self)
		self.pid = pid
		self.clientToAcceptorQueueLock = clientToAcceptorQueueLock
		self.acceptorToServerQueueLock = acceptorToServerQueueLock
		self.instances = {}
		self.Active = True
		self.leaderId = leaderId
	
	def run(self):
		while(config.active):
			## Check if there are any messages in client queue
			while(not config.clientToAcceptorQueue.empty()):
				print "acceptor received a message from client"
				self.clientToAcceptorQueueLock.acquire()	
				recvdMsg = config.clientToAcceptorQueue.get()
				self.clientToAcceptorQueueLock.release()
				if isinstance(recvdMsg,sendProposedValueToAcceptors):
					self.handle_value_from_proposer(recvdMsg)

				## Acceptor has got a proposal from another process which wants to be a leader
				if isinstance(recvdMsg,sendProposedLeaderToAcceptors):
					self.handle_leaderMsg_from_proposer(recvdMsg)

				## Acceptor has got acceptance from other process that the current process can be leader
				if isinstance(recvdMsg,sendAcceptedLeaderToProposer):
					self.handle_leaderAcceptance_from_otherProcess(recvdMsg)

			time.sleep(0)

	def handle_value_from_proposer(self,msg):
		logEntry = msg.logEntry
		## if logEntry is already present
		if logEntry not in self.instances.keys(): 
			self.instances[logEntry] = PaxosAcceptorProtocol(self)
		self.instances[logEntry].sendAcceptedValueToProposer(msg)


	def handle_leaderMsg_from_proposer(self,msg):
		## Acceptor has got a proposal from another process which wants to be a leader
		logEntry = msg.logEntry	
		if logEntry not in self.instances.keys(): 
			self.instances[logEntry] = PaxosAcceptorProtocol(self)
		self.instances[logEntry].sendAcceptedLeaderToProposer(msg)

	def handle_leaderAcceptance_from_otherProcess(self,msg):
		logEntry = msg.logEntry
		if logEntry not in self.instances.keys():
			self.instances[logEntry] = PaxosAcceptorProtocol(self)
		self.instances[logEntry].recvdAcceptedLeaderToProposer(msg)
		


class Learner(Thread):
	def __init__(self,pid,clientToLearnerQueueLock,learnerToServerQueueLock):
		Thread.__init__(self)
		self.pid = pid
		self.clientToLearnerQueueLock = clientToLearnerQueueLock
		self.learnerToServerQueueLock = learnerToServerQueueLock
		self.Active = True
		self.instances = {}

	def run(self):
		while(config.active):
			while(not config.clientToLearnerQueue.empty()):
				print "learner received a message from client"
				self.clientToLearnerQueueLock.acquire()
				msg = config.clientToLearnerQueue.get()
				self.clientToLearnerQueueLock.release()
				if isinstance(msg,sendAcceptedValueToLearners):
					self.handle_accepted_value_from_acceptors(msg)
				if isinstance(msg,sendRequestForLogEntries):
					self.send_log_entries(msg)
				if isinstance(msg,sendLogEntriesMessage):
					self.update_log(msg)
			time.sleep(0)


	def handle_accepted_value_from_acceptors(self,msg):
		logEntry = msg.logEntry
		if logEntry not in self.instances.keys():
			self.instances[logEntry] = PaxosLearnerAcceptingValueProtocol(self)

		self.instances[logEntry].updateResponse(msg)
	

	def send_log_entries(self,msg):
		## if the process has the requested log entry then it will send the message back to the requestor
		index = msg.requestedIndex
		print "In sending Log Entries"
		if index in config.msgLog.keys():
			print "Process has the index and sendint the index"
			sendMsg = sendLogEntriesMessage(config.msgLog[index].clientMsg,msg.senderId,config.msgLog[index].value,msg.requestedIndex)
			self.learnerToServerQueueLock.acquire()
			config.learnerToServerQueue.put(sendMsg)
			self.learnerToServerQueueLock.release()

	def update_log(self,msg):
		key = msg.requestedIndex
		if key not in config.msgLog.keys():
			print "Updated Log Entry"
			config.msgLog[key] = msg
			config.log[key] = msg.value
			print config.msgLog
		

## Start the state machine on a separate thread which checks for the log entries
class StateMachine(Thread):
	def __init__(self,pid,stateMachineToServerQueueLock):
		Thread.__init__(self)
		self.pid = pid
		self.stateMachineToServerQueueLock = stateMachineToServerQueueLock
		self.currIndex = 0
		self.nextAvailableIndex = 0
		self.requestedIndex = 0
		self.prevRequestedIndex = 0
		self.prevRequestTime = time.time()
		self.numOfTickets = 0

	def run(self):
		while(config.active):
			
			## if there are show messages from the console

			while(not config.consoleToStateMachineQueue.empty()):
				msg = config.consoleToStateMachineQueue.get()
				if(msg == "Show"):
					print config.msgLog
					print config.log
			
			## if there is an index i.e previous check Index + 1 then process it.
			## if there is a gap in the index then we request for data from other active process
			for key in config.msgLog.keys():			
				if(key > self.currIndex):
					self.nextAvailableIndex = key
					print "Next Available Index = %s , Curr Index : %s" %(str(self.nextAvailableIndex), str(self.currIndex))
					print "Has an extra key to process ....."
					if(self.nextAvailableIndex == self.currIndex + 1):
						## Now process the log entry
						print "State Machine processing the log entry......"
						self.currIndex = self.nextAvailableIndex
						msg = config.msgLog[self.currIndex]
						
						if (msg.clientMsg.clientSource == self.pid):
							print "Tickets requested....."
							if(self.numOfTickets + msg.value < config.totalNumTickets):
								print "Please take the requested tickets : " + str(msg.value)
								self.numOfTickets = self.numOfTickets + msg.value
							else:
								print "Declined Transaction : Avaiable Tickets : " +str(config.totalNumTickets - self.numOfTickets)
						else:
							if(self.numOfTickets + msg.value < config.totalNumTickets):
								self.numOfTickets = self.numOfTickets + msg.value
		
					else:	
						self.requestedIndex = self.currIndex + 1
						## we still didnt get the index which we have requested for, send the request again
						if((self.prevRequestedIndex == self.requestedIndex) and (self.prevRequestTime + 10 < time.time())):
							print "Same request trying again"
							print "Requested Index : " + str(self.requestedIndex)
							self.prevRequestTime = time.time()
							self.requestForLogEntries()
						elif(self.prevRequestedIndex != self.requestedIndex):
							print "Trying another request"	
							print "Requested Index : " + str(self.requestedIndex)
							self.prevRequestedIndex = self.requestedIndex
							self.prevRequestTime = time.time()
							self.requestForLogEntries()
			time.sleep(0)


	def requestForLogEntries(self):
		self.stateMachineToServerQueueLock.acquire()
		for recv_id in config.connections_made: 
			msg = sendRequestForLogEntries(self.pid,self.requestedIndex,recv_id)
			config.stateMachineToServerQueue.put(msg)
		self.stateMachineToServerQueueLock.release()
