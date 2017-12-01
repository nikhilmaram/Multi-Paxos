## Message class

class logMessage:
	def __init__(self,clientSource,value,timeStamp,numProcess,msgType="Normal"):
		self.clientSource = clientSource
		self.value = value
		self.timeStamp = timeStamp
		self.numProcess = numProcess
		self.msgType = msgType

class clientMessage:
	def __init__(self,clientSource,timeStamp,value,msgId):
		self.clientSource = clientSource
		self.timeStamp = timeStamp
		self.value = value
		self.msgId = msgId
	def __repr__(self):
		return "Client Source = %s, MsgId = %s," %(self.clientSource,self.MsgId)



class hearBeatMessage:
	def __init__(self,leaderId,recvId):	
		self.leaderId = leaderId
		self.recvId = recvId


#########################################################
 ## Messages corresponding to the leader Election

class proposeLeaderMessageToAcceptors:
	def __init__(self,currLogEntry,leaderId,recvId):
		self.currLogEntry = currLogEntry
		self.leaderId = leaderId
		self.recvId = recvId

class denyProposedLeaderMessageToProposer:
	def __init__(self,senderId,recvId):
		self.senderId = senderId
		self.recvId = recvId

class acceptProposedLeaderMessageToProposer:
	def __init__(self,senderId,recvId):
		self.senderId = senderId
		seld.recvId = recvId

class proposeLeaderMessageToLearners:
	def __init__(self,leaderId, recvId):
		self.leaderId = leaderId
		self.recvId = recvId
#########################################################
class Message(object):
	def __init__(self,clientMsg):
		## Id of the sender
		self.clientMsg = clientMsg


class configurationMessageToAcceptors(Message):
	def __init__(self,clientMsg,logEntry,newId,recvId):
		super(configurationMessageToAcceptors,self).__init__(clientMsg)
		self.logEntry = logEntry
		self.newId = newId
		self.recvId = recvId
	def __repr__(self):
		return "New Configuration Added : %s" %(self.newId)


class configurationMessageToLearners(Message):
	def __init__(self,clientMsg,logEntry,senderId,newId,recvId):
		super(configurationMessageToLearners,self).__init__(clientMsg)
		self.logEntry = logEntry
		self.newId = newId
		self.recvId = recvId
		self.senderId = senderId
		self.value = clientMsg.value
		self.leaderId = clientMsg.clientSource

	def __repr__(self):
		return "New Configuration Added : %s" %(self.newId)

class BallotNum:
	## Ballot Numbers to be sent
	def __init__(self,leaderId,leaderNum):
		self.id = leaderId
		self.num = leaderNum
	def __cmp__(self,other):
		if(self.num > other.num):
			return 1
		elif (self.num == other.num and self.id[0]  > other.id[0]):
			return 1
		elif (self.num == other.num and self.id[0] ==  other.id[0]):
			return 0
		else:
			return -1


class sendClientMessageToLeader(Message):
	## Send Client Message to the corresponding Leader
	def __init__(self,clientMsg,leaderId):
		super(sendClientMessageToLeader,self).__init__(clientMsg)
		## Here the leader ID is the receiver ID.
		self.recvId = leaderId
		self.value  = clientMsg.value		


class sendProposedLeaderToAcceptors:
	def __init__(self,leaderId,recvId,ballotNum,logEntry):
		self.leaderId = leaderId
		self.recvId = recvId
		self.ballotNum = ballotNum
		self.logEntry = logEntry
			
class sendAcceptedLeaderToProposer:
	def __init__(self,senderId,recvId,ballotNum,logEntry):
		self.senderId = senderId
		self.recvId = recvId
		self.ballotNum = ballotNum
		self.logEntry = logEntry

class sendProposedValueToAcceptors(Message):
	## Send accept message in Phase 2 of Paxos to send the value to acceptors
	def __init__(self,clientMsg,ballotNum,value,logEntry,leaderId,recvId):
		super(sendProposedValueToAcceptors,self).__init__(clientMsg)
		self.ballotNum = ballotNum
		self.value = value
		self.logEntry = logEntry
		self.leaderId = leaderId
		self.recvId = recvId

class sendAcceptedValueToLeader(Message):
	## Acceptor accepts the value, it sends to Leader that it has accepted
	def __init__(self,clientMsg,ballotNum,logEntry,value,senderId,recvId):
		super(sendAcceptedValueToLeader,self).__init__(clientMsg)
		self.ballotNum = ballotNum
		self.value = value
		self.logEntry = logEntry
		self.senderId = senderId
		self.recvId = recvId ## This will be the leader ID. It is redundant because ballotNum contains the information of leader


class sendAcceptedValueToLearners(Message):
	## Once the acceptor accepts the values then it sends to all the learners.
	def __init__(self,clientMsg,ballotNum,logEntry,value,senderId,leaderId,recvId):
		super(sendAcceptedValueToLearners,self).__init__(clientMsg)
		self.ballotNum = ballotNum
		self.value = value
		self.logEntry = logEntry
		self.senderId = senderId
		self.leaderId = leaderId
		self.recvId = recvId

	def __repr__(self):
		return "Client Requested = %s, Value = %s" %(self.clientMsg.clientSource,self.value)
	##def __str__(self):
	##	return "Client Requested = %s, Value = %s" %(self.clientMsg.clientSource,self.value)



class sendRequestForLogEntries():
	def __init__(self,senderId,requestedIndex,recvId):
		self.senderId = senderId
		self.requestedIndex = requestedIndex
		self.recvId = recvId



class sendLogEntriesMessage(Message):
	def __init__(self,clientMsg,recvId,value,requestedIndex):
		super(sendLogEntriesMessage,self).__init__(clientMsg)
		self.value = value
		self.recvId = recvId
		self.requestedIndex = requestedIndex	

	def __repr__(self):
		return "Client Requested = %s, Value = %s" %(self.clientMsg.clientSource,self.value)
	def __str__(self):
		return "Client Requested = %s, Value = %s" %(self.clientMsg.clientSource,self.value)


