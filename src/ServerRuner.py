#!/usr/bin/env python3
"""
Server Runer

@author: FATESAIKOU
@date  : 07/18/2018
"""

import signal
import json
import sys
import os

from PaxosLib.Acceptor import Acceptor
from pprint import pprint

""" System config """
HOST         = sys.argv[1]
ACCEPTOR_NUM = int(sys.argv[2])


""" Main """
def main():
    global HOST, ACCEPTOR_NUM

    acceptors = []
    for i in range(ACCEPTOR_NUM):
        a = Acceptor(HOST, i)
        a.start()
        acceptors.append(a)

    try:
        acceptors[0].join()
    except KeyboardInterrupt:
        print("Start Deleting Queue")
        for a in acceptors:
            a.stop()

    print("End Server")
    os.kill(os.getpid(), signal.SIGTERM)

if __name__ == '__main__':
    main()
