import Queue as queue
import threading


from collections import deque

clientToProposerQueue  = queue.Queue()
clientToAcceptorQueue  = queue.Queue()
clientToLearnerQueue   = queue.Queue()
proposerToServerQueue  = queue.Queue()
acceptorToServerQueue  = queue.Queue()
learnerToServerQueue   = queue.Queue()
consoleToProposerQueue = queue.Queue()
consoleToServerQueue   = queue.Queue()
stateMachineToServerQueue = queue.Queue()
consoleToStateMachineQueue = queue.Queue()
configToProposerQueue = queue.Queue()


requestSentToLeaderQueue = deque()
requestLeaderLock = threading.RLock()


connections_made=[]
ref_client_info = {}


currLogEntry = 0


currLeader = "Srinu"
phase1Leader = "Srinu"


prevSentHeartBeat = 0
prevRecvHeartBeat = 0

log = {}

## for testing purpose
msgLog = {}

active = True
totalNumTickets = 200
