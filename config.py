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

requestSentToLeaderQueue = deque()
requestLeaderLock = threading.RLock()



connections_made=[]
currLeader = "Srinu"
currLogEntry = 0


phase1Leader = "Srinu"


prevSentHeartBeat = 0
prevRecvHeartBeat = 0

log = {}


active = True
