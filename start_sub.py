from application.sub import Subscriber
from configobj import ConfigObj
import sys
import time

config_path, item = None, 'Sub1'
if len(sys.argv) == 1:
    config_path = 'config/subscriber.ini'
if len(sys.argv) >= 2:
    config_path = sys.argv[1]
if len(sys.argv) >= 3:
    item = sys.argv[2]

config = ConfigObj(config_path)
if item == '':
    item = list(config.keys())[0]

config = config[item]

p = Subscriber(ip_self=config['sub_addr'], ip_zookeeper=config['zookeeper'],
               comm_type=int(config['mode']), logfile=config['logfile'], name=item)

topics = []
topics_info = config['topic'] if isinstance(config['topic'], list) else [config['topic']]
for item in topics_info:
    t, h = item.split('|')
    topics.append({'topic':t, 'history': int(h)})

p.register(topics)

while 1:
    p.receive()
