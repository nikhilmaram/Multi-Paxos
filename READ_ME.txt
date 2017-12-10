Project is split into multiple parts:
Important points to be noted.
	- The current leader always sents heartbeats.
	- The process checks if the leader is down or up only whent they get the requests from clients. i.e if the leader is down and there is no requests from clients the leader election doesnt take place.
	- Only adding a process to the system is considered as reconfiguration.
		- The leader will get to know if a new process has been added to the system based on socket connection and configuration file.
		- The leader broas
	- In phase 2 of paxos.
		- The leader sends the message to all acceptors. Once the acceptor accepts it.
		- The acceptor send the message to all the learners.
		- Learner if it receives the same request from majority of acceptors then it commits in the log (This is done because there are no holes in the log message).


1. Implemented Basic Paxos
	- Leader is known to all the processes and leader is not down, so only Phase 2 runs.
	- Basic Paxos protocol is run for each entry of the log.
	- Initially only client associated with the leader can request for tickets and that log is replicated across all the processes.

2. Implemented the functionality where a client associated with a non-leader requests for tickets to its corresponding process, then the process will send the request to the associated leader.
	- Three different sceanrios in this case.
		- If the leader is still intact(based on the previous heartbeat), then the process will send it to the leader and leader implements the first part.
		- If the leader is down between the last heartbeat and the time where process sends the message. The process can't know that the leader went down. So the process should keep checking its log that the message it requested has been committed after timeout. If the message is still not committed(knows this from the message ID in the log), then it checks for the leader again(based on heartbeat). Now it knows the leader is down. Initiates the election.
		- If the process know that the leader is down. Then initite the election.

3. Implemented Leader Election
	- Two scenarios in this case.
		- Only one processes initiating the leader election.
			1. The process will send the leader request to all the processes.
			2. If the processes has not yet received any request from other process with priority greater than current requested process, they accept the request else they reject it.
			3. After getting the response from majority, the current process will initiate the phase2.
		- Two process initiating the leader election.
			1. All the 3 steps like above are implemented.
			2. The process whose value is accepted will become the leader.
				- There is a chance where the processes will accept the requests made by both competing process in phase 1. This happens when low proirity process requests first and then high priority requests.
				- This is the reason that the leader is decided after phase2.
		
4. Implemented State Machine functionality.
	- The state machine at each process, processes the logs that are commited.
	- It checks if the next available index is one greater than current index.
		- If yes, the state machine processes the log entry.
		- Else, it requests the log entry from other processes.
			- Current process will request log from all the existing processes.

5. Reconfiguration Mechanism.
	- Two scenarios to be considered
		- The leader is up. So it just sends the reconfiguration message to all the process.
		- If the leader is down, then the reconfiguration message is just lost. So the following is implemented.
			- Since all processes know a new process is added to the system from socket connection and configuration file. All the process sends the leader configuration message.
			- Now, this configuration message is treated like a normal request message, If we dont get the log entry for this configuration message after sometime, then the current process initiates a leader election.



Hard parts to implement.
1. No proper documentation for Multi-Paxos. All we have is Paxos replication for one log entry.
2. Leader being down and multiple process initiating the leader election.
3. Reconfiguration mechanism when the leader is down.



User Commands:
To run :
python paxos.py <Client Name> <configuration file>

To Buy the tickets:
Buy <number of Tickets>

To Show the state:
Show
