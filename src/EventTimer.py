#!/usr/bin/env python3
"""
Event timer

@author: FATESAIKOU
@date  : 07/18/2018
"""

import pika
import json
import sys

from pprint import pprint

""" System config """
LOGPATH = sys.argv[1]
HOST    = sys.argv[2]


""" Temp & Result DS """
processing = {}
result     = {}


""" Command handler """
def StartTimer(event_id, start_time):
    global processing
    processing[event_id] = start_time

def StopTimer(event_id, stop_time, message):
    global processing, result
    start_time = processing.pop(event_id, None)

    if start_time != None:
        result[event_id] = {
            'interval': stop_time - start_time,
            'message' : message
        }

def ExitTimer():
    global LOGPATH, result
    dest = open(LOGPATH, 'w')
    dest.write(json.dumps(result))
    dest.close()
    sys.exit(0)


""" Regist command router """
router = {
    'StartTimer': StartTimer,
    'StopTimer' : StopTimer,
    'ExitTimer' : ExitTimer
}


""" Callback """
def callback(ch, method, properties, body):
    cmd = json.loads(body)
    pprint(cmd)    

    if len(cmd) > 1:
        router[cmd[0]](*cmd[1])
        ch.basic_ack(delivery_tag = method.delivery_tag)
    elif len(cmd) == 1:
        ch.basic_ack(delivery_tag = method.delivery_tag)
        router[cmd[0]]()



def main():
    global HOST

    connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=HOST))
    channel = connection.channel()
    channel.queue_declare(queue='timer_queue')
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(callback, 'timer_queue')

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("End Timer")


if __name__ == '__main__':
    main()
