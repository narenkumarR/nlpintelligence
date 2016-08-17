__author__ = 'joswin'
import boto3
import json
import pdb
import pickle
import logging
import time
import re
from sqs_credentials import region_name,aws_access_key_id,aws_secret_access_key,in_queue_name,out_queue_name
from nltk import sent_tokenize

import pickle
with open('models_to_prod_11Aug/naive_bayes_11Aug_short.pkl','r') as f:
    nb_l1_short = pickle.load(f)

with open('models_to_prod_11Aug/naive_bayes_11Aug_full.pkl','r') as f:
    nb_l1_full = pickle.load(f)

with open('models_to_prod_11Aug/action_models_naivebayes.pkl','r') as f:
    l2_models_dic = pickle.load(f)

nb_l2_neg_full = l2_models_dic['nb_action_neg']
nb_l2_neg_short = l2_models_dic['nb_action_neg_short']
nb_l2_neu_full = l2_models_dic['nb_action_neutral']
nb_l2_neu_short = l2_models_dic['nb_action_neutral_short']

# regex for detecting bounced mails
bounce_regex = r"Delivery to the following recipient failed|message that you sent could not be delivered"\
                "|Google tried to deliver your message, but it was rejected by the server"\
                "|Domain name not found|You have reached a limit for sending mail"\
                "|Delivery to the following recipient has been delayed"\
                "|writing to let you know that the group you tried to contact[a-zA-Z() ]+may not exist"

# mapping model prediction classes to actual classes needed by backend
model_class_map_dic = {
           'Positive':'actionable',
           'random':'nurture',
           'Bounced' : 'bounced',
           'Negative_DNC' : 'negative',
           'Negative_Negative' : 'negative',
           'Negative_Nurture' : 'nurture',
           'Negative_rest' : 'nurture',
           'Neutral_Actionable':'actionable',
           'Neutral_Nurture' : 'nurture',
           'Neutral_Out of Office' : 'out-of-office',
           'Neutral rest' : 'nurture'
          }

def get_prediction(message_body):
    body_json = json.loads(message_body)
    # print body_json
    try:
        # finding bounce messages
        inp_text = body_json['content']
        if re.search(bounce_regex,inp_text,re.IGNORECASE):
            prediction = 'Bounced'
        else:
            if len(sent_tokenize(inp_text))<=3:
                short_sent = True
            else:
                short_sent = False
            if short_sent:
                l1_pred = nb_l1_short.predict_textinput(inp_text)[0]
            else:
                l1_pred = nb_l1_full.predict_textinput(inp_text)[0]
            if l1_pred == 'Positive':
                final_pred = 'Positive'
            elif l1_pred == 'Negative':
                if short_sent:
                    l2_pred = nb_l2_neg_short.predict_textinput(inp_text)[0]
                else:
                    l2_pred = nb_l2_neg_full.predict_textinput(inp_text)[0]
                final_pred = l1_pred+'_'+l2_pred
            else:
                if short_sent:
                    l2_pred = nb_l2_neu_short.predict_textinput(inp_text)[0]
                else:
                    l2_pred = nb_l2_neu_full.predict_textinput(inp_text)[0]
                final_pred = l1_pred+'_'+l2_pred
        # body_json['NLPClass'] = prediction
        logging.info('Worked properly. content:{},output:{}'.format(body_json['content'].encode('utf8'),prediction))
    except:
        logging.exception('The following error happened. Returning "random" as output. message_body:{}'.format(message_body))
        # body_json['NLPClass'] = 'random'
        final_pred = 'random'
    # lot of small messages getting classified as schedule meeting etc, fix them
    prediction_final = model_class_map_dic.get(final_pred, 'nurture');
    message_body_new = message_body[:-1]+',"NLPClass":"'+prediction_final+'"}'
    return message_body_new

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
            message_body_new = get_prediction(message_body)
            out_queue.send_message(MessageBody=message_body_new)
            message.delete()
        else:
            logging.info('Nothing in the queue. Wait for 10 seconds')
            time.sleep(10)

