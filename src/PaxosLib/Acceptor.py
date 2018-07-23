"""
Acceptor

@author: FATESAIKOU
@date  : 07/18/2018
"""

import pika
import json
import threading


class Acceptor(threading.Thread):
    def __init__( self, host, acceptor_id ):
        threading.Thread.__init__(self)

        self.__host = host
        self.__acceptor_id = 'acceptor_' + str(acceptor_id)

        self.__conn = self.ConnHost()
        self.__channel = self.InitChannel()
        self.__tasks = {}
        """
        tasks: {
            task_id: {
                max_tx_id: id,
                value: v
            }
        }
        """

    """ Connection Toolkits """
    def ConnHost( self ):
        return pika.BlockingConnection(
                pika.ConnectionParameters(host=self.__host))

    def InitChannel( self ):
        channel = self.__conn.channel()
        channel.queue_delete(queue=self.__acceptor_id)
        channel.queue_declare(queue=self.__acceptor_id)
        channel.basic_qos(prefetch_count=1)

        def consume_callback(channel, method, properties, body):
            # Decode
            req = json.loads(body)

            # Routing
            nonlocal self
            if req[0] == 'Prepare':
                resp = self.handle_prepare(*req[1:])
            elif req[0] == 'Accept':
                resp = self.handle_accept(*req[1:])
            else:
                resp = None

            # Send response
            if resp != None:
                channel.basic_publish(
                    exchange='',
                    routing_key=req[1],
                    body=resp
                )

            # Ack
            channel.basic_ack(delivery_tag = method.delivery_tag)

        channel.basic_consume(consume_callback, self.__acceptor_id)
        return channel

    """ Handle request """
    def handle_prepare( self, client_id, task_id, tx_id ):
        if task_id not in self.__tasks.keys():
            self.__tasks[task_id] = {
                'max_tx_id': 0,
                'value': None
            }
        elif not self.__tasks[task_id]['max_tx_id'] < tx_id:
            return None

        self.__tasks[task_id]['max_tx_id'] = tx_id
        value = self.__tasks[task_id]['value']

        resp = json.dumps([
            'Promise',
            self.__acceptor_id,
            task_id,
            tx_id,
            value
        ])

        return resp

    def handle_accept( self, client_id, task_id, tx_id, value ):
        if (task_id not in self.__tasks.keys()) or \
            (tx_id < self.__tasks[task_id]['max_tx_id']):
            return None

        self.__tasks[task_id]['value'] = value

        resp = json.dumps([
            'Accepted',
            self.__acceptor_id,
            task_id,
            tx_id,
            value
        ])

        return resp

    """ Monitor """
    def GetTasks( self ):
        return self.__tasks

    """ Flow control """
    def run( self ):
        self.__channel.start_consuming()

    def stop( self ):
        self.__channel.stop_consuming()
        self.__channel.queue_delete(queue=self.__acceptor_id)
