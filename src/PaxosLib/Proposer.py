"""
Proposer

@author: FATESAIKOU
@date  : 07/18/2018
"""

import pika
import json
import itertools

from threading import Timer, Thread

def MostCommonOrDefault( L, gap, default_value ):
    groups = itertools.groupby(sorted(L))
    counts = [(value, len(list(iterable)))
        for value, iterable in groups]

    max_ele = max(counts, key=lambda x: x[1])

    return max_ele[0] if max_ele[1] > gap else default_value

class Proposer():
    def __init__( self, host, client_id ):
        self.__host = host
        self.__client_id = 'client_' + str(client_id)

        self.__conn = self.ConnHost()
        self.__channel = self.InitChannel()

    """ Connection Toolkits """
    def ConnHost( self ):
        return pika.BlockingConnection(
                pika.ConnectionParameters(host=self.__host))

    def InitChannel( self ):
        channel = self.__conn.channel()
        channel.queue_delete(queue=self.__client_id)
        channel.queue_declare(queue=self.__client_id)
        channel.basic_qos(prefetch_count=1)
        return channel

    def Close( self ):
        self.__channel.stop_consuming()
        self.__channel.queue_delete(queue=self.__client_id)

    """ Main function """
    def PushIssue( self, server_ids, task_id, value, max_retry=10 ):
        for tx_id in range(1, max_retry + 1):
            """ Promise """
            (promised_ids, values) = self.Prepare(
                    server_ids, task_id, tx_id)

            if len(promised_ids) > len(server_ids) / 2:
                continue

            """ Accept """
            (accept_msg, status) = self.Accept(
                    promised_ids, task_id, tx_id,
                    MostCommonOrDefault(
                        values, len(server_ids/2, value), value))

            if status == 'ACCEPTED':
                break

    """ Sub function for prepare and accept """
    def Prepare( self, server_ids, task_id, tx_id ):
        """ Initalize consume callback """
        resps = {}
        def consume_callback(channel, method, properties, body):
            nonlocal self, resps, server_ids, task_id
            resp = json.loads(body)

            if resp[0] == 'Promise' and \
                    resp[1] not in resps.keys() and \
                    resp[2] == task_id:
                resps[resp[1]] = resp[3]

            if len(resps) == len(server_ids):
                channel.stop_consuming()

            channel.basic_ack(delivery_tag=method.delivery_tag)

        """ Send message """
        self.SendMsg(
            server_ids,
            json.dumps(['Prepare', self.__client_id, task_id, tx_id]),
            consume_callback,
            10.0,
            lambda: self.__channel.stop_consuming()
        )

        return list(resps.keys()), list(resps.values())

    def Accept( self, server_ids, task_id, tx_id, value ):
        """
        do promise
        """

    """ Utils """
    def DoConsuming( self, consume_callback ):
        self.__channel.basic_consuming(consume_callback, self.__client_id)

    def SendMsg( self, server_ids, message, consume_callback, timeout, timeout_callback ):
        """ Recreate consumer """
        print("1")
        self.__channel.basic_cancel(consumer_tag='prepare')
        self.__channel.basic_consume(consume_callback,
                queue=self.__client_id, consumer_tag='prepare')

        print("2")
        """ Regist Timer & Consumer Thread """
        self.__timer = Timer(timeout, timeout_callback)
        t = Thread(target=self.__channel.start_consuming)

        """ Send message """
        print("3")
        self.__timer.start()
        t.start()

        for i in server_ids:
            print("send: " + str(i))
            self.__channel.basic_publish(exchange='',
                    routing_key=('acceptor_' + str(i)), body=message)

        t.join()

        print("4")
        self.__timer.cancel()

