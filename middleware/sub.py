import zmq
import json
from kazoo.recipe.watchers import DataWatch
from functools import partial


class SubDirect:

    def __init__(self, ip_self, ip_broker, client):
        self.ip = ip_self
        self.ip_b = ip_broker
        self.context_sub = None
        self.context_rcv = None
        self.socket_sub = None
        self.socket_rcv_list = {}
        self.topics_list = set()
        self.zk_client = client

    def register(self, topics):
        self.context_sub = zmq.Context()
        self.socket_sub = self.context_sub.socket(zmq.REQ)
        self.socket_sub.connect(self.ip_b)
        
        for t in topics:
            self.socket_sub.send_json(
                json.dumps(
                    {"type": "add_subscriber",
                     "ip": self.ip,
                     "topic": t['topic'],
                     "strength": t['strength'],
                     "history": t['history']}
                )
            )
            ip = self.socket_sub.recv_json()['msg']
            if ip is not None:
                self.context_rcv = zmq.Context()
                self.socket_rcv_list[t['topic']] = self.context_rcv.socket(zmq.SUB)
                self.socket_rcv_list[t['topic']].setsockopt_string(zmq.SUBSCRIBE, '')
                self.socket_rcv_list[t['topic']].connect(ip)
                DataWatch(self.zk_client, "Topic/%s/Pub" % t['topic'], partial(self.ask_and_update, t))
            else:
                self.socket_rcv_list[t['topic']] = None

    def ask_and_update(self, topic):
        self.socket_sub.send_json(
            json.dumps(
                {"type": "add_subscriber",
                 "ip": self.ip,
                 "topic": topic['topic'],
                 "history": topic['history']}
            )
        )
        ip = self.socket_sub.recv_json()['msg']
        if ip is not None:
            self.context_rcv = zmq.Context()
            self.socket_rcv_list[topic['topic']] = self.context_rcv.socket(zmq.SUB)
            self.socket_rcv_list[topic['topic']].setsockopt_string(zmq.SUBSCRIBE, '')
            self.socket_rcv_list[topic['topic']].connect(ip)
            # DataWatch(self.zk_client, "Topic/%s/Pub" % topic['topic'], self.ask_and_update)
        else:
            self.socket_rcv_list[topic['topic']] = None

    def update_broker_ip(self, new_ip):
        self.context_sub = zmq.Context()
        self.socket_sub = self.context_sub.socket(zmq.REQ)
        self.socket_sub.connect(new_ip)
        self.ip_b = new_ip

    def receive(self):
        for socket_rcv in self.socket_rcv_list:
            if socket_rcv is not None:
                msg = socket_rcv.recv_json()
                if msg['topic'] in self.topics_list:
                    print("receive a message: topic = %s, value = %s" % (msg["topic"], msg["value"]))
    # return msg

    def unregister(self, topic):
        self.socket_sub.send_json(json.dumps({"type": "sub_unregister_topic", "ip": self.ip, "topic": topic}))

    def exit(self):
        self.socket_sub.send_json(json.dumps({"type": "sub_exit_system", "ip": self.ip, "topic": "all"}))


class SubBroker:

    def __init__(self, ip_self, ip_broker):
        # print(ip_self, ip_broker, 'XXX')
        self.ip = ip_self
        self.ip_b = ip_broker
        self.context_sub = None
        self.context_ntf = None
        self.socket_sub = None
        self.socket_ntf = None

    def register(self, topics):
        self.context_sub = zmq.Context()
        self.socket_sub = self.context_sub.socket(zmq.REQ)
        self.socket_sub.connect(self.ip_b)

        self.socket_sub.send_json(
            json.dumps(
                {"type": "add_subscriber",
                 "ip": self.ip,
                 "topic": topics}
                )
            )
        res = self.socket_sub.recv_json()
        
        self.context_ntf = zmq.Context()
        self.socket_ntf = self.context_ntf.socket(zmq.REP)
        self.socket_ntf.bind("tcp://*:%s" % self.ip.split(":")[2])

    def update_broker_ip(self, new_ip):
        self.context_sub = zmq.Context()
        self.socket_sub = self.context_sub.socket(zmq.REQ)
        self.socket_sub.connect(new_ip)
        self.ip_b = new_ip

    def notify(self):
        msg = self.socket_ntf.recv_json()
        print("receive a message: topic = %s, value = %s" % (msg["topic"], msg["value"]))
        self.socket_ntf.send_json({'msg': 'success'})
        return msg

    def unregister(self, topic):
        self.socket_sub.send_json(json.dumps({"type": "sub_unregister_topic", "ip": self.ip, "topic": topic}))

    def exit(self):
        self.socket_sub.send_json(json.dumps({"type": "sub_exit_system", "ip": self.ip, "topic": "all"}))
