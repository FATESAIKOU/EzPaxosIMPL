#!/usr/bin/env python3
"""
Request runer

@author: FATESAIKOU
@date  : 07/18/2018
"""

import sys
import random

from PaxosLib.Proposer import Proposer
from threading import Thread

HOST = sys.argv[1]
TASK_NUM = int(sys.argv[2])
VARS_NUM = int(sys.argv[3])
CLIENT_NUM = int(sys.argv[4]) or 1
SERVER_NUM = int(sys.argv[5]) or 1


def GenTasks(task_num, vars_num):
    tasks = {}
    for task_id in range(task_num):
        tasks['task_' + str(task_id)] = random.randint(1, vars_num)

    return tasks

def SendTask(proposer, server_ids, tasks):
    for (task_id, value) in tasks.items():
        sys.stderr.write( "===[Send]===\nName: %s\nValue: %s\nProposer Info: %r\n===\n" %
                (task_id, value, proposer.GetInfo()) )
        result = proposer.PushIssue(server_ids, task_id, value, max_retry=1)
        sys.stderr.write( "===[Out]===\nName: %s\nValue: %s\nResult: %r\nProposer Info: %r\n===\n\n" %
                (task_id, value, result, proposer.GetInfo()) )

    proposer.Close()

def main():
    # Init servers/clients
    server_ids = list(range(SERVER_NUM))
    proposers = [
        Proposer(HOST, i)
        for i in range(CLIENT_NUM)
    ]

    # Gen test tasks
    tasks = GenTasks(TASK_NUM, VARS_NUM)

    # Do request
    client_threads = []
    for p in proposers:
        t = Thread(target=SendTask, args=(p, server_ids, tasks))
        t.start()
        client_threads.append(t)

    for c in client_threads:
        c.join()

    # End of test
    sys.stderr.write("End of test\n")


if __name__ == '__main__':
    main()
