import nltk
import configparser
import string
import random

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from pymongo import MongoClient

config = configparser.ConfigParser()
config.read("CONFIG.ini")

clienres t = MongoClient(config['MONGODB']['CLIENT_IP'],
                     int(config['MONGODB']['CLIENT_PORT']))
db = client[config['MONGODB']['DB_NAME']]
col = db[config['MONGODB']['COLLECTION_NAME']]

raw = ""
for res in col.find({}):
    raw = raw + res['text']

sent_tokens = nltk.sent_tokenize(raw)
word_tokens = nltk.word_tokenize(raw)

print("*"*400)
print(sent_tokens)
print("*"*400)
print(word_tokens)
print("*"*400)
print(len(sent_tokens), len(word_tokens))
print("*"*400)

lemmer = nltk.stem.WordNetLemmatizer()
res 

def LemTokens(tokens):
    return [lemmer.lemmatize(token) for token in tokens]


remove_punct_dict = dict((ord(punct), None) for punct in string.punctuation)


def LemNormalize(text):
    return LemTokens(nltk.word_tokenize(raw.lower().translate(remove_punct_dict)))


def response(user_response):
    robo_response = ''
    sent_tokens.append(user_response)
    TfidfVec = TfidfVectorizer(tokenizer=LemNormalize, stop_words='english')
    tfidf = TfidfVec.fit_transform(sent_tokens)
    vals = cosine_similarity(tfidf[-1], tfidf)
    idx = vals.argsort()[0][-2]
    flat = vals.flatten()
    flat.sort()
    req_tfidf = flat[-2]
    if(req_tfidf == 0):
        robo_response = robo_response+"I am sorry! I don't understand you"
        return robo_response
    else:
        robo_response = robo_response+sent_tokens[idx]
        return robo_response


GREETING_INPUTS = ("hello", "hi", "greetings", "sup", "what's up", "hey",)
GREETING_RESPONSES = ["hi", "hey", "*nods*", "hi there",
                      "hello", "I am glad! You are talking to me"]


def greeting(sentence):

    for word in sentence.split():
        if word.lower() in GREETING_INPUTS:
            return random.choice(GREETING_RESPONSES)


flag = True
# print("ROBO: My name is Robo. I will answer your queries about Chatbots. If you want to exit, type Bye!")
while(flag):
    user_response = input()
    user_response = user_response.lower()
    if(user_response != 'bye'):
        if(user_response == 'thanks' or user_response == 'thank you'):
            flag = False
            print("ROBO: You are welcome..")
        else:
            if(greeting(user_response) != None):
                print("ROBO: "+greeting(user_response))
            else:
                print("ROBO: ", end="")
                print(response(user_response))
                sent_tokens.remove(user_response)
    else:
        flag = False
        print("ROBO: Bye! take care..")
