import zmq
import json
from datetime import datetime
from logger import get_logger
import threading
from kazoo.recipe.watchers import DataWatch
from functools import partial

table_lock = threading.Lock()

tf = "%Y-%m-%d %H:%M:%S"

class RegisterTable:

    def __init__(self):
        self.pubs = {}
        self.subs = {}
        self.topics = {}

    def __str__(self):
        return str(self.topics)

    def set_strengthest_pub(self, topic, pub):
        self.topics['strongest'][topic] = pub

    def add_pub(self, pub, topics):
        now = datetime.now().strftime(tf)
        if pub in self.pubs:
            self.pubs[pub]['since'] = now
            self.pubs[pub]['status'] = 0
            for item in topics:
                self.pubs[pub]['topics'][item['topic']] = item
        else:
            self.pubs[pub] = {'since': now, 'topics': dict((x['topic'], x) for x in topics), 'status':0}
        for t in topics:
            t = t['topic']
            if t not in self.topics:
                self.topics[t] = {'pub': set(), 'sub': set()}
            self.topics[t]['pub'].add(pub)
        return ''

    def add_sub(self, sub, topics):
        now = datetime.now().strftime(tf)
        if sub in self.subs:
            self.subs[sub]['since'] = now
            self.subs[sub]['status'] = 0
            for item in topics:
                self.subs[sub]['topics'][item['topic']] = item
        else:
            self.subs[sub] = {'since': now, 'topics': dict((x['topic'], x) for x in topics), 'status': 0}
        for t in topics:
            t = t['topic']
            if t not in self.topics:
                self.topics[t] = {'pub': set(), 'sub': set()}
            self.topics[t]['sub'].add(sub)
        return ''

class BrokerBase:

    def __init__(self, config, zk):
        self.config = config
        self.table = RegisterTable()
        self.req_handler = {
            'add_publisher': self._add_pub,
            'add_subscriber': self._add_sub,
        }
        self.socket = None
        self.logger = get_logger(config['logfile'])
        self.zk = zk
        self.watched_topics = {}

    def handle_req(self):
        try:
            req = self.socket.recv_json(flags=zmq.NOBLOCK)
        except zmq.Again:
            raise RuntimeError('again')
        if isinstance(req, str):
            req = json.loads(req)
        result = self.req_handler[req['type']](req)
        self.logger.info('request=%s, response=%s' % (req, result))
        self.socket.send_json({'msg': result})

    def _on_strengtheset_change(self, topic, data, status, version):
        data = data.decode()
        self.table.set_strengthest_pub(topic=topic, pub=data)

    def _add_pub(self, req):
        result = self.table.add_pub(pub=req['ip'], topics=req['topic'])
        topics = req['topic'] if isinstance(req['topic'], list) else [req['topic']]
        for t in topics:
            if not self.zk.exists('/Topic/%s/Pub'%t):
                self.zk.create('/Topic/%s/Pub' % t, makepath=True)
            if t not in self.watched_topics:
                self.watched_topics[t] = DataWatch(self.zk, '/Topic/%s/Pub'%t, partial(self._on_strengtheset_change, t))
        return result

    def _add_sub(self, req):
        result = self.table.add_sub(sub=req['ip'], topics=req['topic'])
        return result

class BrokerType1(BrokerBase):

    def __init__(self, config, zk):
        super().__init__(config, zk)
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:%s" % config['port'])
        self.socket = socket

    def _add_sub(self, req):
        super()._add_sub(req)
        for ip in self.table.topics[req['topic']]['pub']:
            sub_hisory = self.table.subs[req['ip']]['topics'][req['topic']]['history']
            pub_hisory = self.table.pubs[ip]['topics'][req['topic']]['history']
            if pub_hisory >= sub_hisory:
                return ip
        return ''

class BrokerType2(BrokerBase):

    def __init__(self, config, zk):
        super().__init__(config, zk)
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*:%s" % config['port'])
        self.socket = socket
        self.req_handler['publish_req'] = self._publish_req

    def _publish_req(self, req):
        strongest = self.table.topics[req['topic']]['strongest']
        if req['ip'] != strongest:
            self.logger.info('Message blocked by ownship strength. Strongest=%s'%strongest)
            return 'msg sent to ip=[], res=blocked by ownship strength'
        res = []
        subs = []
        for ip in self.table.topics[req['topic']]['sub']:
            sub_hisory = self.table.subs[ip]['topics'][req['topic']]['history']
            pub_hisory = self.table.pubs[req['ip']]['topics'][req['topic']]['history']
            if pub_hisory < sub_hisory:
                subs.append(ip)
                res.append('blocked by history requirement. require=%s, actual=%s'%(sub_hisory, pub_hisory))
                continue
            subs.append(ip)
            context = zmq.Context()
            sub_socket = context.socket(zmq.REQ)
            sub_socket.setsockopt(zmq.LINGER, 0)
            sub_socket.RCVTIMEO = 1000
            sub_socket.connect(ip)
            sub_socket.send_json({"topic": req['topic'], "value": req['value']})
            try:
                result = sub_socket.recv_json()
                res.append(result)
            except zmq.error.Again:
                res.append('fail to sub=%s'%ip)
            finally:
                sub_socket.close()
        return 'msg sent to ip=%s, res=%s' % (subs, res)