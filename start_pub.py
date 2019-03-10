from application.pub import Publisher
from configobj import ConfigObj
import sys


config_path, item = None, 'Pub1'
if len(sys.argv) == 1:
    config_path = 'config/publisher.ini'
if len(sys.argv) >= 2:
    config_path = sys.argv[1]
if len(sys.argv) >= 3:
    item = sys.argv[2]

config = ConfigObj(config_path)
if item == '':
    item = list(config.keys())[0]

config = config[item]

p = Publisher(mode=int(config['mode']), ip_address=config['pub_addr'],
              zk_address=config['zookeeper'], strength=config['strength'], logfile=config['logfile'],pub_name=item)

topics = []
topics_info = config['topic'] if isinstance(config['topic'], list) else [config['topic']]
for item in topics_info:
    t, h = item.split('|')
    topics.append({'topic':t, 'history': h})

p.register(topics)

while 1:
    x = input('>')
    topic, value = x.split(',')
    p.publish(topic, value)

