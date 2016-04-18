Austin Colcord and Nick Scheuring

This is the project for creating a distributed key value store.

High-Level Approach:
Following direction based on the Raft paper, we implemented a basic version of a
key value store. By holding different 'states' of Leader, Follower, and Candidate,
the requests from clients are replicated in an organized manner between each replica
before the client requests can be completed. It was started by simply implementing the
election protocol, ensuring that the replicas were communicating and holding a leader
properly. Then, log replication was implemented, creating specific scenarios for
how log entries should be shifted and executed.

Issues:
As this is a complicated implementation, creating the distributed key value store system
was complex, and many issues were encountered throughout the process. Figuring out how
to properly implement elections and log replication, in addition to the subtle conditions
that occur with each, much trial and error was attempted to resolve issues.

Testing:
Tests were run using the run.py simulator and test.py script, using those to pinpoint specific
error cases while we developed the program. Heavy print debugging, and writing log entries into
a file were used to help solve specific issues in different test cases.
    - Set DEBUG global variable in 3700kvstore and Server.py to utilize print statements.