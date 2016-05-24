__author__ = 'joswin'
import boto3
import json
from sqs_credentials import region_name,aws_access_key_id,aws_secret_access_key,in_queue_name,out_queue_name
from intent_class_models.naive_bayes.naive_bayes_model import NaiveBayesModel

nb = NaiveBayesModel()
nb.load_model()

sqs = boto3.resource('sqs',region_name=region_name,aws_access_key_id=aws_access_key_id,
                     aws_secret_access_key=aws_secret_access_key)
in_queue = sqs.get_queue_by_name(QueueName=in_queue_name)
out_queue = sqs.get_queue_by_name(QueueName=out_queue_name)

max_queue_messages = 10

while True:
    for message in in_queue.receive_messages(MaxNumberOfMessages=max_queue_messages):
        # pdb.set_trace()
        body_json = json.loads(message.body)
        prediction = str(nb.predict_textinput(body_json['text'])[0])
        try:
            message_id = body_json['id']
        except:
            message_id = 'no id'
        out_json = '"id":{},"prediction":{}'.format(message_id,prediction)
        out_queue.send_message(MessageBody=out_json)
        message.delete()
