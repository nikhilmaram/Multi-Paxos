from messages import *
import config

class PaxosProposerProtocol:
	def __init__(self,agent,msg):
		self.agent = agent
		self.acceptedResponses = {} ## Responses obtained from process for the value proposed by proposer
		self.sequenceNum = 0
		self.initMsg = msg

	def sendProposedValueToAllAcceptors(self,clientMsg):
		## Sending the proposed value to all the acceptors by 
		## Creating the corresponding message and putting them in the server Queue
		ballotNumInst = BallotNum(self.agent.pid,self.sequenceNum)
		for recv_id in config.connections_made :
			print "Protocol : proposer sending proposed values to acceptors"
			proposedValueMessage = sendProposedValueToAcceptors(clientMsg,ballotNumInst,clientMsg.value,config.currLogEntry,self.agent.pid,recv_id)
			self.agent.proposerToServerQueueLock.acquire()
			config.proposerToServerQueue.put(proposedValueMessage)
			self.agent.proposerToServerQueueLock.release()


	def sendConfigurationMessageToAllAcceptors(self,newId,clientMsg):
		## Doing it exactly like the normal messages
		ballotNumInst = BallotNum(self.agent.pid,self.sequenceNum)
		for recv_id in config.connections_made :
			print "Protocol : proposer sending configuration to acceptors"
			proposedConfigurationMessage = configurationMessageToAcceptors(clientMsg,config.currLogEntry,newId,recv_id)
			self.agent.proposerToServerQueueLock.acquire()
			config.proposerToServerQueue.put(proposedConfigurationMessage)
			self.agent.proposerToServerQueueLock.release()


	def accepted_value_from_acceptor(self,msg):
		self.acceptedResponses[msg.senderId] = msg.value

	def sendProposedLeaderToAllAcceptors(self):
		self.sequenceNum += 1
		ballotNumInst = BallotNum(self.agent.pid,self.sequenceNum)
		for recv_id in config.connections_made:
			print "Proposer proposing itself to be the leader"
			self.agent.proposerToServerQueueLock.acquire()
			proposedLeaderMessage = sendProposedLeaderToAcceptors(self.agent.pid,recv_id,ballotNumInst,config.currLogEntry)
			config.proposerToServerQueue.put(proposedLeaderMessage) 
			self.agent.proposerToServerQueueLock.release()
			print "Log Value : " + str(config.currLogEntry)



class PaxosAcceptorProtocol:
	def __init__(self,agent):
		self.agent = agent
		self.highestBallotAccepted = BallotNum(self.agent.pid,-1)
		self.acceptedVal= None
		self.responses = {}

	def sendAcceptedValueToProposer(self,msg):
		if(msg.ballotNum >= self.highestBallotAccepted):
			print "Protocol :acceptor sending accepted valuee to leader"
			self.highestBallotAccepted = msg.ballotNum
			if(self.acceptedVal == None):
				## Only when the accepted value before is none. This is required to disable rewriting log.
				self.acceptedVal = msg.value
				self.agent.acceptorToServerQueueLock.acquire()
				## leader Id needs to be changed from leader Election
				config.acceptorToServerQueue.put(sendAcceptedValueToLeader(msg.clientMsg,msg.ballotNum,msg.logEntry,self.acceptedVal,self.agent.pid,msg.leaderId))
				self.agent.acceptorToServerQueueLock.release()
				## sending accepted value to all the learners
				self.sendAcceptedValueToAllLearners(msg)

			else:
				## Send the NAK to the process since the value is already present in the log. Currently not required.
				## Since if the value is sent to majority of the acceptors then learners would have know about and entered into log
				pass

	def sendAcceptedValueToAllLearners(self,msg):
		for recv_id in config.connections_made:
			print "Protocol :acceptor sending accepted value to learner"
			acceptedValueMessage = sendAcceptedValueToLearners(msg.clientMsg,msg.ballotNum,msg.logEntry,self.acceptedVal,self.agent.pid,msg.leaderId,recv_id)
			self.agent.acceptorToServerQueueLock.acquire()
			config.acceptorToServerQueue.put(acceptedValueMessage)
			self.agent.acceptorToServerQueueLock.release()


	def sendAcceptedConfigurationToAllLearners(self,msg):
		for recv_id in config.connections_made:
			print "Protocol :acceptor sending accepted configuration to learner"
			acceptedConfigurationMessage = configurationMessageToLearners(msg.clientMsg,msg.logEntry,self.agent.pid,msg.newId,recv_id)
			self.agent.acceptorToServerQueueLock.acquire()
			config.acceptorToServerQueue.put(acceptedConfigurationMessage)
			self.agent.acceptorToServerQueueLock.release()

	def sendAcceptedLeaderToProposer(self,msg):
		print "Message : Num Value :" + str(msg.ballotNum.num) + " Agent Id : " + str(msg.ballotNum.id)
		print "Self : Num Value :" + str(self.highestBallotAccepted.num) + " Agent Id : " + str(self.highestBallotAccepted.id)
		if(msg.ballotNum > self.highestBallotAccepted):
			print "Protocol : acceptor sending to leader that it has accepted it to be leader"
			self.highestBallotAccepted = msg.ballotNum
			self.agent.acceptorToServerQueueLock.acquire()
			config.acceptorToServerQueue.put(sendAcceptedLeaderToProposer(self.agent.pid,msg.leaderId,msg.ballotNum,msg.logEntry))
			self.agent.acceptorToServerQueueLock.release()	

	def recvdAcceptedLeaderToProposer(self,msg):
		## Store the responses obtained from other process w.r.t Ballot Number value because 
		## incase of multiple Iteration Ballot number is only changed
		if msg.ballotNum.num not in self.responses:
			self.responses[msg.ballotNum.num] = {}
		self.responses[msg.ballotNum.num][msg.senderId] = 1
		self.hasMajority(msg)
	
	def hasMajority(self,msg):
		majority = len(config.connections_made)/2 + 1
		if (len(self.responses[msg.ballotNum.num]) >= majority):
			print "Current Process has been elected as the leader in Phase 1"
			config.phase1Leader = self.agent.pid
		
		
			


class PaxosLearnerAcceptingValueProtocol:
	def __init__(self,agent):
		self.agent = agent
		self.responses = {}

	def hasMajority(self,msg):
		majority = len(config.connections_made)/2 + 1
		if (len(self.responses) >= majority):
			config.log[msg.logEntry] = msg.value
			## storing the complete msg in another log
			config.msgLog[msg.logEntry] = msg
			## The log entry is set here because it is way of telling the proposer that log has entries till this entry if the process becomes a leader.
			config.currLogEntry = msg.logEntry
			## During the leader election, if there is a race,server getting majority in the second round is winner
			config.currLeader = msg.leaderId
			print "value written in log" 
			print config.log
			## Check the message that is committed in the log is the one which you have sent to the Leader
			
			if(len(config.requestSentToLeaderQueue) > 0):
				msgSentToLeader = config.requestSentToLeaderQueue[0]
				if(msgSentToLeader.msgId == msg.clientMsg.msgId):
					print "Request sent to leader is committed, removing from the queue"
					config.requestLeaderLock.acquire()
					config.requestSentToLeaderQueue.pop()
					config.requestLeaderLock.release()

			
	def updateResponse(self,msg):
		print "Protocol : learner has received response"
		self.responses[msg.senderId] = msg.value
		self.hasMajority(msg)
			

			

