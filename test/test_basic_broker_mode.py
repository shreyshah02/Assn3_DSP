from application.pub import Publisher
from application.sub import Subscriber
from application.broker import Broker
import time
import threading
from kazoo.client import KazooClient


mode=2

zk_root = '/test_temp_dir'

zookeeper = '127.0.0.1:2181'
zk = KazooClient(hosts=zookeeper)

broker = Broker({'mode': mode, 'port': 5555, 'zookeeper':zookeeper,
                  'logfile': 'log/temp_broker.log', 'broker_addr':'tcp://127.0.0.1:5555'},zk_root=zk_root)

pub1 = Publisher(mode=mode, ip_address='tcp://127.0.0.1:5000',pub_name='Pub1',
                zk_address=zookeeper, logfile='log/temp_pub.log', zk_root=zk_root)

pub2 = Publisher(mode=mode, ip_address='tcp://127.0.0.1:5001',pub_name='Pub2',
                zk_address=zookeeper, logfile='log/temp_pub.log', zk_root=zk_root)

pub3 = Publisher(mode=mode, ip_address='tcp://127.0.0.1:5002',pub_name='Pub3',
                zk_address=zookeeper, logfile='log/temp_pub.log', zk_root=zk_root)

sub1 = Subscriber(ip_self='tcp://127.0.0.1:5010', ip_zookeeper=zookeeper, name='Sub1',
                   comm_type=mode, logfile='log/temp_sub.log', zk_root=zk_root)

sub2 = Subscriber(ip_self='tcp://127.0.0.1:5011', ip_zookeeper=zookeeper, name='Sub2',
                   comm_type=mode, logfile='log/temp_sub.log', zk_root=zk_root)

def test_start():
    zk.start()
    try:
        zk.create(zk_root)
    except:
        pass
    thread1 = threading.Thread(target=broker.start)
    thread1.start()
    time.sleep(1)

def test_publisher_register():
    res1 = pub1.register([{'topic': 'hello', 'history': 3}])
    res2 = pub1.register([{'topic': 'hello', 'history': 5}])
    pubs = zk.get_children('%s/Topic/hello/Pub'%zk_root)

    assert res1 == 0 and res2 == 0
    assert len(pubs) == 2

def test_subscriber_register():
    res1 = sub1.register([{'topic': 'hello', 'history': 4}])
    res2 = sub2.register([{'topic': 'hello', 'history': 6}])
    subs = zk.get_children('%s/Topic/hello/Sub' % zk_root)

    assert res1 == 0 and res2 == 0
    assert len(subs) == 2

def test_publish_and_receive1():

    def publish():
        for i in range(3):
            pub1.publish('hello', 'world')
            pub2.publish('hello', 'world')

    pub_thread = threading.Thread(target=publish)
    pub_thread.start()

    sub_thread = threading.Thread(target=sub1.receive)
    sub_thread.start()
    sub_thread.join(timeout=1)

    assert sub_thread.is_alive() # no msg received, so the sub_thread keeps waiting
    pub_thread.join()


def test_publish_and_receive2():
    def publish():
        for i in range(3):
            pub2.publish('hello', 'world')

    pub1.drop_system()
    time.sleep(3)

    pub_thread = threading.Thread(target=publish)
    pub_thread.start()

    msg = sub1.receive()
    assert msg['topic'] == 'hello' and msg['value'] == 'world'
    pub_thread.join()


def test_publish_and_receive3():
    def publish():
        for i in range(3):
            pub2.publish('hello', 'world')

    pub_thread = threading.Thread(target=publish)
    pub_thread.start()

    sub_thread = threading.Thread(target=sub2.receive)
    sub_thread.start()
    sub_thread.join(timeout=1)

    assert sub_thread.is_alive()  # no msg received, so the sub_thread keeps waiting
    pub_thread.join()

def test_clean():
    try:
        zk.delete(zk_root, recursive=True)
    except:
        pass