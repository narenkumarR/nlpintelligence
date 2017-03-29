__author__ = 'joswin'
import boto3

import logging
import time
from optparse import OptionParser

from sqs_credentials import region_name,aws_access_key_id,aws_secret_access_key,in_queue_name,out_queue_name
from predictor_wrapper import get_prediction_jsoninput

sqs = boto3.resource('sqs',region_name=region_name,aws_access_key_id=aws_access_key_id,
                     aws_secret_access_key=aws_secret_access_key)

max_queue_messages = 10

def run_queue(in_queue_name_arg,out_queue_name_arg):
    if not in_queue_name_arg or not out_queue_name_arg:
        in_queue_name_arg = in_queue_name
        out_queue_name_arg = out_queue_name
    in_queue = sqs.get_queue_by_name(QueueName=in_queue_name_arg)
    out_queue = sqs.get_queue_by_name(QueueName=out_queue_name_arg)
    logging.basicConfig(filename='sqs_processlog_'+in_queue_name_arg+'.log', level=logging.INFO,format='%(asctime)s %(message)s')
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('nose').setLevel(logging.CRITICAL)
    logging.info('started')
    while True:
        for message in in_queue.receive_messages(MaxNumberOfMessages=max_queue_messages):
            if message:
                # pdb.set_trace()
                message_body = message.body
                message_body_new = get_prediction_jsoninput(message_body)
                out_queue.send_message(MessageBody=message_body_new)
                message.delete()
            else:
                time.sleep(3)

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-i', '--in',
                         dest='in_queue',
                         help='name of in queue',
                         default=None)
    optparser.add_option('-o', '--out',
                         dest='out_queue',
                         help='name of out queue',
                         default=None)
    (options, args) = optparser.parse_args()
    in_queue_name_arg = options.in_queue
    out_queue_name_arg = options.out_queue
    run_queue(in_queue_name_arg,out_queue_name_arg)

