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
	def __init__(self,pid,consoleToProposerQueueLock,proposerToServerQueueLock,clientToProposerQueueLock,configToProposerQueueLock,currLeader):
		Thread.__init__(self)
		self.pid = pid
		self.consoleToProposerQueueLock = consoleToProposerQueueLock
		self.proposerToServerQueueLock  = proposerToServerQueueLock
		self.clientToProposerQueueLock  = clientToProposerQueueLock
		self.configToProposerQueueLock  = configToProposerQueueLock
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
				##print "proposer received a command from console"
				self.consoleToProposerQueueLock.acquire()
				msg = config.consoleToProposerQueue.get()
				self.consoleToProposerQueueLock.release()

				if (config.currLeader == self.pid):
					## Request came from the console which the leader is present
					##print "Current Leader got the client request"
					config.currLogEntry = config.currLogEntry+1
					self.handle_client_request(msg,config.currLogEntry)
				else:
					## Request came from the console which is not the leader so send it to the leader
					##print "Send the client request to the corresponding leader"
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
				##print "proposer received a command from client"
				self.clientToProposerQueueLock.acquire()
				msg = config.clientToProposerQueue.get()
				self.clientToProposerQueueLock.release()
				if isinstance(msg,sendAcceptedValueToLeader):
					## Message from another acceptor to the leader
					self.handle_accepted_value_from_acceptor(msg)

				if isinstance(msg,sendClientMessageToLeader) :
					## Console message obtained from other process which is not the leader
					##print "Current Leader has got the console message from another process"
					## Check if the message has already been commited to the the proposer. Iterate over all the instances entries
					resp = self.instancesContainsMessage(msg)
					if (resp == None):
						config.currLogEntry = config.currLogEntry + 1
						self.handle_client_request(msg.clientMsg,config.currLogEntry)
					else:
						##print "Got the same message again ............"
						self.handle_client_request(msg.clientMsg,resp)


				if isinstance(msg,configurationMessageToProcess):
					## Configuration message need to be sent to the leader
					if (config.currLeader == self.pid):
						## Request came from the console which the leader is present
						##print "Current Leader got the configuration request from new process"
						## Need to check if the configuration request is sent by existing process
						resp = self.instancesContainsMessage(msg)
						if (resp == None):
							config.currLogEntry = config.currLogEntry + 1
							self.handle_configuration_request(msg,config.currLogEntry)
						else:
							##print "Got the same configuration again ............"
							self.handle_configuration_request(msg,resp)
					else:
						##print "Send the configuration request to the corresponding leader"
						sendMsg = configurationMessageToLeader(msg.clientMsg,msg.newId,config.currLeader,time.time())
						config.requestLeaderLock.acquire()
						config.requestSentToLeaderQueue.append(sendMsg)
						config.requestLeaderLock.release()
						self.leader_check_config(sendMsg)

				if isinstance(msg,configurationMessageToLeader):
					##print "Current leader got the configuration request from existing process"
					resp = self.instancesContainsMessage(msg)
					if (resp == None):
						config.currLogEntry = config.currLogEntry + 1
						self.handle_configuration_request(msg,config.currLogEntry)
					else:
						##print "Got the same configuration again ............"
						self.handle_configuration_request(msg,resp)

				time.sleep(0)



			## If there is a configuration change then we get the message from queue
			while(not config.configToProposerQueue.empty()):
				###print "A new configuration message has been obtained"
				### Waiting so the appropriate leader will send the heart beat and the configuration message can be sent to it.
				### The current process cannot act as leader when the leader is down because it doesnt have updated index numbers.

				self.configToProposerQueueLock.acquire()
				newId = config.configToProposerQueue.get()
				self.configToProposerQueueLock.release()

				clientMsg = clientMessage(self.pid,time.time(),0,self.pid+str(0))
				for recv_id in config.connections_made :
					##print "Configuration message sent to the everyone except for itself"
					if recv_id != self.pid:
						configMessage = configurationMessageToProcess(clientMsg,self.pid,recv_id)
						self.proposerToServerQueueLock.acquire()
						config.proposerToServerQueue.put(configMessage)
						self.proposerToServerQueueLock.release()



			config.requestLeaderLock.acquire()
			if(len(config.requestSentToLeaderQueue) > 0):
				##print "Checking if request retry is needed"
				msg = config.requestSentToLeaderQueue[0]
				if(time.time() - msg.timeStamp > config.checkTimeout):
					##print "No response yet from the leader to the message"
					### Before retrying check if the leader is still intact
					msg.timeStamp = time.time()
					if isinstance(msg,configurationMessageToLeader):
						self.leader_check_config(msg)
					if isinstance(msg,clientMessage):
						self.leader_check(msg)
			config.requestLeaderLock.release()
					
			self.send_hearbeat()
			time.sleep(0)


	def instancesContainsMessage(self,msg):
		for key in self.instances.keys():
			instMessage = self.instances[key].initMsg
			if(msg.clientMsg.msgId == instMessage.msgId):
				return key
		return None


	def handle_client_request(self,msg,logEntry):
		## Proposer gets a client request
		## For now we assume there is no checking of available log entries for multi paxos
		## The Proposer fixes on a log Entry and runs the Basic Paxos Protocol
		if logEntry not in self.instances.keys():
			self.instances[logEntry] = PaxosProposerProtocol(self,msg)
		self.instances[logEntry].sendProposedValueToAllAcceptors(msg)

	def handle_configuration_request(self,msg,logEntry):
		## Creating this client message just to maintain consisitency with normal messages not required actually.
		clientMsg = msg.clientMsg
		newId = msg.newId
		if logEntry not in self.instances.keys():
			self.instances[logEntry] = PaxosProposerProtocol(self,clientMsg)
		self.instances[logEntry].sendConfigurationMessageToAllAcceptors(newId,clientMsg)
			


	def handle_accepted_value_from_acceptor(self,msg):
		self.instances[msg.logEntry].accepted_value_from_acceptor(msg)

	def send_to_current_leader(self,msg):
   		self.proposerToServerQueueLock.acquire()
   		config.proposerToServerQueue.put(msg)
   		self.proposerToServerQueueLock.release()	

	def leader_check(self,msg):
		if(config.prevRecvHeartBeat + config.timeout > time.time()):
			##print "Leader is still intact"
			sendMsg = sendClientMessageToLeader(msg,config.currLeader)
			self.send_to_current_leader(sendMsg)
		else:	
			##print "Leader is down"
			config.currLeader = None
			config.phase1Leader = None
			config.currLogEntry = config.currLogEntry + 1
			self.select_leader(msg)	


	def leader_check_config(self,msg):
		if(config.prevRecvHeartBeat + config.timeout > time.time()):
			##print "Leader is still intact"
			sendMsg = configurationMessageToLeader(msg.clientMsg,msg.clientMsg.clientSource,config.currLeader,time.time())
			self.send_to_current_leader(sendMsg)
		else:	
			##print "Leader is down"
			config.currLeader = None
			config.phase1Leader = None
			config.currLogEntry = config.currLogEntry + 1
			self.select_leader(msg)	



	def select_leader(self,msg):
		## Combining the phase 1 and phase 2 of paxos 
		##print "Process is initiating the start of the leader - Phase 1"
		if config.currLogEntry not in self.instances.keys():
			self.instances[config.currLogEntry] = PaxosProposerProtocol(self,msg)
		self.instances[config.currLogEntry].sendProposedLeaderToAllAcceptors()
		##print "Please Wait......"
		###time.sleep(15)
		while(config.phase1Leader == None and config.currLeader == None):
			continue
		##print "Checking after time out"
		if(config.phase1Leader == self.pid):
			##print "Current process has been chosen as a leader after time out"
			if isinstance(msg,configurationMessageToLeader):
				self.handle_configuration_request(msg,config.currLogEntry)
			else:
				self.handle_client_request(msg,config.currLogEntry)
		else :
			##print "Current process has not be chosen as a leader"
			pass
		
	
	def send_hearbeat(self):
		## Sending Heartbeat
		if(config.currLeader == self.pid):
			if (config.prevSentHeartBeat + config.timeout < time.time()):
				config.prevSentHeartBeat = time.time()
				##print "Sending HeartBeat to all the acceptors"
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
				##print "acceptor received a message from client"
				self.clientToAcceptorQueueLock.acquire()	
				recvdMsg = config.clientToAcceptorQueue.get()
				self.clientToAcceptorQueueLock.release()
				
				if isinstance(recvdMsg,sendProposedValueToAcceptors):
					self.handle_value_from_proposer(recvdMsg)
				

				if isinstance(recvdMsg,configurationMessageToAcceptors):
					self.handle_configuration_from_proposer(recvdMsg)

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


	def handle_configuration_from_proposer(self,msg):
		logEntry = msg.logEntry
		## if logEntry is already present
		if logEntry not in self.instances.keys(): 
			self.instances[logEntry] = PaxosAcceptorProtocol(self)
			##print "Log Entry created for configuration : " +str(logEntry)
		self.instances[logEntry].sendAcceptedConfigurationToAllLearners(msg)
	
	def handle_leaderMsg_from_proposer(self,msg):
		## Acceptor has got a proposal from another process which wants to be a leader
		logEntry = msg.logEntry
		##print self.instances	
		if logEntry not in self.instances.keys(): 
			self.instances[logEntry] = PaxosAcceptorProtocol(self)
			##print "Log Entry doesn't exist : " +str(logEntry)
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
				##print "learner received a message from client"
				self.clientToLearnerQueueLock.acquire()
				msg = config.clientToLearnerQueue.get()
				self.clientToLearnerQueueLock.release()
				if isinstance(msg,sendAcceptedValueToLearners):
					self.handle_accepted_value_from_acceptors(msg)
				if isinstance(msg,sendRequestForLogEntries):
					self.send_log_entries(msg)
				if isinstance(msg,sendLogEntriesMessage):
					self.update_log(msg)
				if isinstance(msg,configurationMessageToLearners):
					self.handle_accepted_value_from_acceptors(msg)
			time.sleep(0)


	def handle_accepted_value_from_acceptors(self,msg):
		logEntry = msg.logEntry
		if logEntry not in self.instances.keys():
			self.instances[logEntry] = PaxosLearnerAcceptingValueProtocol(self)
		self.instances[logEntry].updateResponse(msg)
	

	def send_log_entries(self,msg):
		## if the process has the requested log entry then it will send the message back to the requestor
		index = msg.requestedIndex
		##print "In sending Log Entries"
		if index in config.msgLog.keys():
			##print "Process has the index and sending the index : " + str(index)
			if isinstance(config.msgLog[index],configurationMessageToLearners) or (isinstance(config.msgLog[index],sendLogEntriesMessage) and (config.msgLog[index].typeMessage == "config")):
				##print "Sending with config"
				sendMsg = sendLogEntriesMessage(config.msgLog[index].clientMsg,msg.senderId,config.msgLog[index].value,msg.requestedIndex,"config")
			else:
				##print "Sending without config"
				sendMsg = sendLogEntriesMessage(config.msgLog[index].clientMsg,msg.senderId,config.msgLog[index].value,msg.requestedIndex,"notconfig")
			self.learnerToServerQueueLock.acquire()
			config.learnerToServerQueue.put(sendMsg)
			self.learnerToServerQueueLock.release()

	def update_log(self,msg):
		key = msg.requestedIndex
		if key not in config.msgLog.keys():
			##print "Updated Log Entry"
			##print type(msg)
			##print msg.typeMessage
			config.msgLog[key] = msg
			config.log[key] = msg.value
			##print config.msgLog
		

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
					print "State Machine Processing Index : " + str(self.currIndex)
					print config.msgLog
					print  "Available Tickets : " +str(config.totalNumTickets - self.numOfTickets)
					print  "Current Leader : " +str(config.currLeader)
			
			## if there is an index i.e previous check Index + 1 then process it.
			## if there is a gap in the index then we request for data from other active process
			for key in config.msgLog.keys():			
				if(key > self.currIndex):
					self.nextAvailableIndex = key
					##print "Next Available Index = %s , Curr Index : %s" %(str(self.nextAvailableIndex), str(self.currIndex))
					##print "Has an extra key to process ....."
					if(self.nextAvailableIndex == self.currIndex + 1):
						## Now process the log entry
						##print "State Machine processing the log entry......"
						self.currIndex = self.nextAvailableIndex
						msg = config.msgLog[self.currIndex]
						if not isinstance(msg,configurationMessageToLearners):			
							if (msg.clientMsg.clientSource == self.pid):
								print "Tickets requested....."
								if(self.numOfTickets + msg.value <= config.totalNumTickets):
									print "Please take the requested tickets : " + str(msg.value)
									self.numOfTickets = self.numOfTickets + msg.value
								else:
									print "Declined Transaction : Avaiable Tickets : " +str(config.totalNumTickets - self.numOfTickets)
							else:
								if(self.numOfTickets + msg.value <= config.totalNumTickets):
									self.numOfTickets = self.numOfTickets + msg.value
		
					else:	
						self.requestedIndex = self.currIndex + 1
						## we still didnt get the index which we have requested for, send the request again
						if((self.prevRequestedIndex == self.requestedIndex) and (self.prevRequestTime + config.timeout < time.time())):
							##print "Same request trying again"
							##print "Requested Index : " + str(self.requestedIndex)
							self.prevRequestTime = time.time()
							self.requestForLogEntries()
						elif(self.prevRequestedIndex != self.requestedIndex):
							##print "Trying another request"	
							##print "Requested Index : " + str(self.requestedIndex)
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
