__author__ = 'joswin'
import boto3
import json
import pdb
import logging
import time
from sqs_credentials import region_name,aws_access_key_id,aws_secret_access_key,in_queue_name,out_queue_name
from intent_class_models.naive_bayes.naive_bayes_model import NaiveBayesModel

nb = NaiveBayesModel()
nb.load_model()

sqs = boto3.resource('sqs',region_name=region_name,aws_access_key_id=aws_access_key_id,
                     aws_secret_access_key=aws_secret_access_key)
in_queue = sqs.get_queue_by_name(QueueName=in_queue_name)
out_queue = sqs.get_queue_by_name(QueueName=out_queue_name)

max_queue_messages = 10

logging.basicConfig(filename='sqs_processlog.log', level=logging.INFO,format='%(asctime)s %(message)s')
logging.getLogger('boto3').setLevel(logging.CRITICAL)
while True:
    for message in in_queue.receive_messages(MaxNumberOfMessages=max_queue_messages):
        if message:
            # pdb.set_trace()
            message_body = message.body
            body_json = json.loads(message_body)
            # print body_json
            try:
                prediction = str(nb.predict_textinput(body_json['content'])[0])
                # body_json['NLPClass'] = prediction
                logging.info('Worked properly. content:{},output:{}'.format(body_json['content'].encode('utf8'),prediction))
            except:
                logging.exception('The following error happened. Returning "random" as output. message_body:{}'.format(message_body))
                # body_json['NLPClass'] = 'random'
                prediction = 'random'
            message_body_new = message_body[:-1]+',"NLPClass":"'+prediction+'"}'
            out_queue.send_message(MessageBody=message_body_new)
            message.delete()
        else:
            time.sleep(10)