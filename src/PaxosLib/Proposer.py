"""
Proposer

@author: FATESAIKOU
@date  : 07/18/2018
"""

import itertools


def MostCommonOrDefault( L, gap, default_value ):
    groups = itertools.groupby(sorted(L))
    counts = [(value, len(list(iterable)))
        for value, iterable in groups]

    max_ele = max(counts, key=lambda x: x[1])

    return max_ele[0] if max_ele[1] > gap else default_value


class Proposer():
    def __init__( self, client_id ):
        """
        status
            ON_START
            ON_PROMISE
            PROMISED
            ON_ACCEPT
            ACCEPTED
        """
        self.__status = 'ON_START'
        self.__client_id = client_id


    def PushIssue( self, server_ids, task_id, value, max_retry=10 ):
        tx_id = 1
        for i in range(max_retry):
            """ Promise """
            (promised_ids, values) = self.Prepare(
                    server_ids, task_id, tx_id)

            if (len(promised_ids) > len(server_ids) / 2):
                continue

            """ Accept """
            accept_msg = self.Accept(
                    promised_ids, task_id, tx_id,
                    MostCommonOrDefault(
                        values, len(server_ids/2, value), value))

            if (self.__status == 'ACCEPTED'):
                break


    def Prepare( self, server_ids, task_id, tx_id):
        """
        do prepare
        """

    def Accept( self, server_ids, task_id, tx_id, value ):
        """
        do promise
        """
