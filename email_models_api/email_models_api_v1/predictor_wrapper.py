__author__ = 'joswin'
import json
import pickle
import logging
import re
from nltk import sent_tokenize
from numpy import max as np_max,argmax as np_argmax

from settu_sir_cleaning import text_vectors
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
           'Neutral_rest' : 'nurture'
          }

with open('models_to_prod_11Aug/naive_bayes_11Aug_short.pkl','r') as f:
    nb_l1_short = pickle.load(f)

with open('models_to_prod_11Aug/naive_bayes_11Aug_full.pkl','r') as f:
    nb_l1_full = pickle.load(f)

with open('models_to_prod_11Aug/settu_sir_model.pkl','r') as f:
    tmp_dic = pickle.load(f)
    ss_l1_full,vectorizer_full = tmp_dic['model'],tmp_dic['vectorizer']
    del tmp_dic

with open('models_to_prod_11Aug/settu_sir_model_shortwords.pkl','r') as f:
    tmp_dic = pickle.load(f)
    ss_l1_short,vectorizer_short = tmp_dic['model'],tmp_dic['vectorizer']
    del tmp_dic

# for getting prediction from probability for settu sir models
ss_dv_order = ['Negative','Neutral','Positive']

with open('models_to_prod_11Aug/action_models_naivebayes.pkl','r') as f:
    l2_models_dic = pickle.load(f)

nb_l2_neg_full = l2_models_dic['nb_action_neg']
nb_l2_neg_full.default_class = ['Negative']
nb_l2_neg_short = l2_models_dic['nb_action_neg_short']
nb_l2_neg_short.default_class = ['Negative']
nb_l2_neu_full = l2_models_dic['nb_action_neutral']
nb_l2_neu_full.default_class = ['Nurture']
nb_l2_neu_short = l2_models_dic['nb_action_neutral_short']
nb_l2_neu_short.default_class = ['Nurture']

# regex for detecting bounced mails
bounce_regex_str = r"Delivery to the following recipient failed|message that you sent could not be delivered"\
                "|Google tried to deliver your message, but it was rejected by the server"\
                "|Domain name not found|You have reached a limit for sending mail"\
                "|Delivery to the following recipient has been delayed"\
                "|writing to let you know that the group you tried to contact[a-zA-Z() ]+may not exist"

bounce_regex = re.compile(bounce_regex_str,re.IGNORECASE)

# following regular expression are used for hard matching
neg_neg_regex_str = r"(\.|,|hi|^)( |\n)*we are good(\.|$)|we are good[a-z ]+current|not at this time|not for us\."
neg_dnc_regex_str = r"(got|sent to)( | the )wrong (email id|mail id|id)|"\
    "(exclude|remove) [a-z ]*from[a-z ]+list|\bi [a-z ]+ sue [a-z ]+you"\
    "|unsubscribe|stop send[a-z ]+(mail|spam)|stop spam|(send|sending) spam"
pos_regex_str = r"not averse to try|(are|am) interested in knowing|guide (me|us) [a-z ]*(how to|next steps|what to|get intro)"\
    "|please (meet|emeet|e-meet)|how (to|should|can|will)[a-z ]+proceed"
outofoffice_regex_str = r"currently not in[a-z ]+office|out of[a-z ]+office|currently on vacation|"\
    "limited access to emails|on vacation (till|until)|access to email[a-z ]+limited|"\
    "on holiday (until|till)|away for[a-z ]+(week|month)|response (will|may|can|could) be delayed"

neg_neg_regex = re.compile(neg_neg_regex_str,re.IGNORECASE)
neg_dnc_regex = re.compile(neg_dnc_regex_str,re.IGNORECASE)
pos_regex = re.compile(pos_regex_str,re.IGNORECASE)
outofoffice_regex = re.compile(outofoffice_regex_str,re.IGNORECASE)

def get_prediction_textinput(inp_text):
    if not inp_text.strip():
        final_pred = 'Neutral_Nurture'
        return final_pred
    if len(sent_tokenize(inp_text))<=3:
        short_sent = True
    else:
        short_sent = False
    if bounce_regex.search(inp_text):
        final_pred = 'Bounced'
    elif outofoffice_regex.search(inp_text) :
        final_pred = 'Neutral_Out of Office'
    elif pos_regex.search(inp_text) and short_sent:
        final_pred = 'Positive'
    elif neg_neg_regex.search(inp_text) and short_sent:
        final_pred = 'Negative_Negative'
    elif neg_dnc_regex.search(inp_text) and short_sent:
        final_pred = 'Negative_DNC'
    else:
        # get prediction from both models (Naive Bayes and Random Forest)
        # if any model predicts positive, take as positive. If any predicts -ve, take as -ve, else neutral
        # can try to use probability also here
        if short_sent:
            l1_pred_nb = nb_l1_short.predict_textinput(inp_text)[0]
            l1_pred_ss_prob = ss_l1_short.predict_proba(vectorizer_short.transform([text_vectors(inp_text)]))
        else:
            l1_pred_nb = nb_l1_full.predict_textinput(inp_text)[0]
            l1_pred_ss_prob = ss_l1_full.predict_proba(vectorizer_short.transform([text_vectors(inp_text)]))
        if np_max(l1_pred_ss_prob) >= 0.45:
            l1_pred_ss = ss_dv_order[np_argmax(l1_pred_ss_prob)]
        else:
            l1_pred_ss = l1_pred_nb
        if l1_pred_nb == 'Negative' or l1_pred_ss == 'Negative':
            l1_pred = 'Negative'
        elif l1_pred_nb == 'Positive' or l1_pred_ss == 'Positive':
            l1_pred = 'Positive'
        else:
            l1_pred = 'Neutral'
        # Based on first level prediction choose second level model and predict
        # final prediction is first leve prediction +'_'+ second level prediction
        # final prediction should be key in the model_class_map_dic
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
    return final_pred

def get_prediction_jsoninput(message_body):
    body_json = json.loads(message_body)
    # print body_json
    try:
        # finding bounce messages
        inp_text = body_json['content']
        final_pred = get_prediction_textinput(inp_text)
        logging.info('Worked properly. output:{}, content:{}'.format(final_pred,body_json['content'].encode('utf8')))
    except KeyError:
        logging.exception('Key error for json, returning "Random" as output. message_body : {}'.format(message_body))
        final_pred = 'random'
    except:
        logging.exception('The following error happened. Returning "random" as output. message_body:{}'.format(message_body))
        # body_json['NLPClass'] = 'random'
        final_pred = 'random'
    # lot of small messages getting classified as schedule meeting etc, fix them
    prediction_final = model_class_map_dic.get(final_pred, 'nurture')
    message_body_new = message_body[:-1]+',"NLPClass":"'+prediction_final+'"}'
    return message_body_new

