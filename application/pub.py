from middleware.pub import *
from logger import get_logger
from kazoo import client as kz_client
from kazoo.recipe.watchers import DataWatch
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.exceptions import NoNodeError, NodeExistsError
from functools import partial
import copy

class Publisher:
    def __init__(self, mode, ip_address=None, zk_address=None, strength=0,
                 logfile='log/pub.log', pub_name = None, zk_root=''):
        self.ip_address = ip_address
        self.zk_address = zk_address
        self.strength = strength
        self.my_client = kz_client.KazooClient(hosts=zk_address)
        self.zk_root = zk_root
        self.broker_address = None
        self.pub_mw = None
        self.mode = mode
        self.exited = False
        self.logger = get_logger(logfile)
        self.pub_name = pub_name
        self.topic_strength = {}

    def create_mw(self):
        broker_address, _ = self.get_broker_address()
        broker_address = broker_address.decode()
        self.broker_address = broker_address
        if self.mode == 1:
            self.pub_mw = PublisherDirectly(self.ip_address, self.broker_address)
        elif self.mode == 2:
            self.pub_mw = PublisherViaBroker(self.ip_address, self.broker_address)
        else:
            print("mode error, please choose approach")

    def publish(self, topic, value):
        self.logger.info('publishing a msg. topic=%s, value=%s'%(topic, value))
        self.pub_mw.publish(topic, value)
        return 0

    def watch_strength(self, topic, children):
        c = self.my_client.get_children('%s/Topic/%s/Pub'%(self.zk_root, topic['topic']))
        self.compare_strength(topic, c)

    def compare_strength(self, topic, c):
        min_s = 1000
        for x in c:
            strength = int(self.my_client.get("%s/Topic/%s/Pub/%s"%(self.zk_root, topic['topic'], x))[0].decode().split(',')[1])
            #print(type(strength))
            if strength < min_s:
                min_s = strength
        if int(self.topic_strength[topic['topic']]) <= min_s:
            self.my_client.set("%s/Topic/%s/Pub"%(self.zk_root, topic['topic']), self.ip_address.encode())
        return 0

    def register(self, topics):
        self.my_client.start()
        self.create_mw()
        self.broker_address = self.my_client.get("%s/Leader"%self.zk_root)[0].decode()
        topics_strength = []
        for topic in topics:
            try:
                c = self.my_client.get_children("%s/Topic/%s/Pub"%(self.zk_root, topic['topic']))
            except NoNodeError:
                self.my_client.create("%s/Topic/%s/Pub"%(self.zk_root, topic['topic']), makepath=True, ephemeral=False)
                c = []
            id = self.my_client.create("%s/Topic/%s/Pub/Pub"%(self.zk_root, topic['topic']), sequence=True, makepath=True, ephemeral=True)
            #print (id)
            strength = id[-3:]
            #print (strength)
            history = topic["history"]
            s_h = ','.join([self.ip_address, strength, str(history)])
            self.topic_strength[topic['topic']] = strength
            self.my_client.set(id, s_h.encode())
            self.compare_strength(topic, c)
            topic_s = copy.deepcopy(topic)
            topic_s["strength"] = strength
            topics_strength.append(topic_s)
            cw = ChildrenWatch(self.my_client, '%s/Topic/%s/Pub' % (self.zk_root, topic['topic']), partial(self.watch_strength, topic))
            self.logger.info('pub register to broker on %s. ip=%s, topic=%s' % (self.broker_address, self.ip_address, topic_s))
        node_url = "%s/Publisher/" % self.zk_root + self.pub_name
        node_data = self.ip_address
        try:
            self.my_client.create(node_url, node_data.encode(), ephemeral=True, makepath=True)
        except NodeExistsError:
            pass


        self.pub_mw.register(topics_strength)

        leader_watcher = DataWatch(self.my_client, '%s/Leader'%self.zk_root, self.update_broker_ip_socket)


        return 0

    '''
    if a leader broker dies, watcher should use this function to update connection with the new leader broker
    '''
    def update_broker_ip_socket(self, data, status, version):
        self.broker_address = data.decode()
        self.pub_mw.update_broker_bind(self.broker_address)
        return 0
    '''
    publisher cancels a topic
    '''
    def unregister_topic(self, topic):
        self.pub_mw.unregister(topic)
        return 0

    '''
    publisher wants to exit the system
    '''
    def drop_system(self):
        self.exited = True
        self.my_client.stop()
        self.my_client.close()
        self.pub_mw.drop_system()
        return 0

    def get_broker_address(self):
        return self.my_client.get("%s/Leader"%self.zk_root)

    def get_broker_address_now(self):
        return self.broker_address

    def get_ip_address(self):
        return self.ip_address

    def get_strength(self):
        return self.strength

    def set_ip_address(self, ip_address):
        self.ip_address = ip_address
        return 0

    def set_strength(self, strength):
        self.strength = strength
        return 0
