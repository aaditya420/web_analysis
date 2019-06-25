# import requests

# session = requests.session()
# session.proxies = {}

# request = session.get('http://httpbin.org/ip')
# print(request.text)

# session.proxies['http'] = 'socks5h://localhost:9050'
# session.proxies['https'] = 'socks5h://localhost:9050'

# request = session.get('http://httpbin.org/ip')
# print(request.text)

# headers = {}
# headers['User-agent'] = 'HotJava/1.1.2 FCS'

# request = session.get('http://httpbin.org/user-agent', headers=headers)
# print(request.text)

# request = session.get('http://httpbin.org/cookies', headers=headers)
# print(request.text)

from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['WEB_SCRAPER']
col = db['LINKS_STACK_QUESTIONS']

js = col.find_one({'tag_name': 'javascript'})

# print(js['questions'])
print(len(js['questions']))
