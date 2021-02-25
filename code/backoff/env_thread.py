import os, signal, sys, time, re
from acceptor import Acceptor
from leader import Leader
from message import RequestMessage
from process import Process
from replica import Replica
from utils import *

from concurrent.futures import ThreadPoolExecutor
import threading

NACCEPTORS = 3
NREPLICAS = 2
NLEADERS = 2
NREQUESTS = 1
NCONFIGS = 3

class Env:
    """
    This is the main code in which all processes are created and run. This
    code also simulates a set of clients submitting requests.
    """
    def __init__(self, clusterSize, clientSize):
        self.procs = {}
        self.clusterSize = clusterSize
        self.clientSize = clientSize
        self.acceptedProposalCount = 0
        self.totalProposalCount = 0
        self.start_time = 0

    def sendRequest(self, initialconfig, c):
        # Send client requests to replicas
        for i in range(NREQUESTS):
        # for i in range(self.clientSize):
            print "self.clientSize ", self.clientSize
            print "Task Executed ", format(threading.current_thread())
            print "threading.current_thread() ", threading.current_thread()
            print "threading.active_count() ", threading.active_count()
            pid = "client %d.%d" % (c,i)
            for r in initialconfig.replicas:
                cmd = Command(pid,0,"operation %d.%d" % (c,i))
                self.sendMessage(r, RequestMessage(pid,cmd))
                time.sleep(1)

    def addAcceptedProposalCount(self):
        self.acceptedProposalCount = self.acceptedProposalCount + 1
        # print "self.acceptedProposalCount:", self.acceptedProposalCount

    def addTotalProposalCount(self):
        self.totalProposalCount = self.totalProposalCount + 1
        print "self.totalProposalCount:", self.totalProposalCount, ";  self.acceptedProposalCount: ",  self.acceptedProposalCount
        print "--- %s seconds ---" % (time.time() - self.start_time)

    def sendMessage(self, dst, msg):
        if dst in self.procs:
            self.procs[dst].deliver(msg)

    def addProc(self, proc):
        self.procs[proc.id] = proc
        proc.start()

    def removeProc(self, pid):
        del self.procs[pid]

    def run(self):
        initialconfig = Config([], [], [])
        c = 0

        # Create replicas
        for i in range(self.clusterSize):
            pid = "replica %d" % i
            Replica(self, pid, initialconfig)
            initialconfig.replicas.append(pid)
        # Create acceptors (initial configuration)
        for i in range(self.clusterSize):
            pid = "acceptor %d.%d" % (c,i)
            Acceptor(self, pid)
            initialconfig.acceptors.append(pid)
        # Create leaders (initial configuration)
        for i in range(self.clusterSize):
            pid = "leader %d.%d" % (c,i)
            Leader(self, pid, initialconfig)
            initialconfig.leaders.append(pid)
        
        self.start_time = time.time()
        executor = ThreadPoolExecutor(max_workers=self.clientSize)

        executor.submit(
            self.sendRequest(initialconfig, c)
        )

        self.sendRequest(initialconfig, c)

        # # Create new configurations. The configuration contains the
        # # leaders and the acceptors (but not the replicas).
        # for c in range(1, NCONFIGS):
        #     config = Config(initialconfig.replicas, [], [])
        #     # Create acceptors in the new configuration
        #     for i in range(self.clusterSize):
        #         pid = "acceptor %d.%d" % (c,i)
        #         Acceptor(self, pid)
        #         config.acceptors.append(pid)
        #     # Create leaders in the new configuration
        #     for i in range(self.clusterSize):
        #         pid = "leader %d.%d" % (c,i)
        #         Leader(self, pid, config)
        #         config.leaders.append(pid)
        #     # Send reconfiguration request
        #     for r in config.replicas:
        #         pid = "master %d.%d" % (c,i)
        #         cmd = ReconfigCommand(pid,0,str(config))
        #         self.sendMessage(r, RequestMessage(pid, cmd))
        #         time.sleep(1)
        #     # Send WINDOW noops to speed up reconfiguration
        #     for i in range(WINDOW-1):
        #         pid = "master %d.%d" % (c,i)
        #         for r in config.replicas:
        #             cmd = Command(pid,0,"operation noop")
        #             self.sendMessage(r, RequestMessage(pid, cmd))
        #             time.sleep(1)
        #     # Send client requests to replicas
        #     for i in range(NREQUESTS):
        #         pid = "client %d.%d" % (c,i)
        #         for r in config.replicas:
        #             cmd = Command(pid,0,"operation %d.%d"%(c,i))
        #             self.sendMessage(r, RequestMessage(pid, cmd))
        #             time.sleep(1)

    def terminate_handler(self, signal, frame):
        self._graceexit()

    def _graceexit(self, exitcode=0):
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(exitcode)

def main():
    size = int(re.findall("(?!size=)\d", sys.argv[1])[0])
    client = int(re.findall("(?!client=)\d", sys.argv[2])[0])
    e = Env(size, client)
    e.run()
    signal.signal(signal.SIGINT, e.terminate_handler)
    signal.signal(signal.SIGTERM, e.terminate_handler)
    signal.pause()

if __name__=='__main__':
    main()
