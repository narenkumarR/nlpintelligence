__author__ = 'joswin'
import boto3
import json
import pdb
import logging
import time
import re
from sqs_credentials import region_name,aws_access_key_id,aws_secret_access_key,in_queue_name,out_queue_name
from intent_class_models.naive_bayes.naive_bayes_model import NaiveBayesModel
from intent_class_models.text_processing.word_transformations import Tokenizer
from nltk.corpus import stopwords

nb = NaiveBayesModel()
nb.load_model()
tkz = Tokenizer()
stop_words = stopwords.words('english')

# regex for detecting bounced mails
bounce_regex = r"Delivery to the following recipient failed|message that you sent could not be delivered"\
                "|Google tried to deliver your message, but it was rejected by the server"\
                "|Domain name not found|You have reached a limit for sending mail"\
                "|Delivery to the following recipient has been delayed"\
                "|writing to let you know that the group you tried to contact[a-zA-Z() ]+may not exist"

# mapping model prediction classes to actual classes needed by backend
model_class_map_dic = {'Interested':'nurture',
           'Interested, schedule meeting':'actionable',
           'Not interested':'negative',
           'Not right person':'nurture',
           'do not contact':'negative',
           'mail chain':'nurture',
           'need details':'actionable',
           'out of office':'out-of-office',
           'random':'nurture',
           'Bounced' : 'bounced'
          }

sqs = boto3.resource('sqs',region_name=region_name,aws_access_key_id=aws_access_key_id,
                     aws_secret_access_key=aws_secret_access_key)
in_queue = sqs.get_queue_by_name(QueueName=in_queue_name)
out_queue = sqs.get_queue_by_name(QueueName=out_queue_name)

max_queue_messages = 10

logging.basicConfig(filename='sqs_processlog.log', level=logging.INFO,format='%(asctime)s %(message)s')
logging.getLogger('boto3').setLevel(logging.CRITICAL)
print('started')
while True:
    for message in in_queue.receive_messages(MaxNumberOfMessages=max_queue_messages):
        if message:
            # pdb.set_trace()
            message_body = message.body
            body_json = json.loads(message_body)
            # print body_json
            try:
                # finding bounce messages
                if re.search(bounce_regex,body_json['content'],re.IGNORECASE):
                    prediction = 'Bounced'
                else: 
                    prediction = str(nb.predict_textinput(body_json['content'])[0])
                # body_json['NLPClass'] = prediction
                logging.info('Worked properly. content:{},output:{}'.format(body_json['content'].encode('utf8'),prediction))
            except:
                logging.exception('The following error happened. Returning "random" as output. message_body:{}'.format(message_body))
                # body_json['NLPClass'] = 'random'
                prediction = 'random'
            # lot of small messages getting classified as schedule meeting etc, fix them
            if len(tkz.stopword_removal_textinput(body_json['content'],stop_words).split()) < 4 and prediction == 'Interested, schedule meeting':
                prediction = 'Interested'
            prediction_final = model_class_map_dic.get(prediction, 'nurture');   
            message_body_new = message_body[:-1]+',"NLPClass":"'+prediction_final+'"}'
            out_queue.send_message(MessageBody=message_body_new)
            message.delete()
        else:
            logging.info('Nothing in the queue. Wait for 10 seconds')
            time.sleep(10)

