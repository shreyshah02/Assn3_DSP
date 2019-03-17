from application.pub import Publisher
from application.sub import Subscriber
from application.broker import Broker
import time
import threading
from kazoo.client import KazooClient

sub1_msg = None
sub2_msg = None

def receive(sub, which):
    while keep_receive:
        msg = sub.receive()
        if which == 1:
            global sub1_msg
            sub1_msg = msg['value']
        if which == 2:
            global sub2_msg
            sub2_msg = msg['value']

keep_receive = True

mode=1

zk_root = '/test_temp_dir'

zookeeper = '127.0.0.1:2181'
zk = KazooClient(hosts=zookeeper)

broker = Broker({'mode': mode, 'port': 6555, 'zookeeper':zookeeper,
                  'logfile': 'log/temp_broker.log', 'broker_addr':'tcp://127.0.0.1:6555'},zk_root=zk_root)

pub1 = Publisher(mode=mode, ip_address='tcp://127.0.0.1:6000',pub_name='Pub1',
                zk_address=zookeeper, logfile='log/temp_pub.log', zk_root=zk_root)

pub2 = Publisher(mode=mode, ip_address='tcp://127.0.0.1:6001',pub_name='Pub2',
                zk_address=zookeeper, logfile='log/temp_pub.log', zk_root=zk_root)

pub3 = Publisher(mode=mode, ip_address='tcp://127.0.0.1:6002',pub_name='Pub3',
                zk_address=zookeeper, logfile='log/temp_pub.log', zk_root=zk_root)

sub1 = Subscriber(ip_self='tcp://127.0.0.1:6010', ip_zookeeper=zookeeper, name='Sub1',
                   comm_type=mode, logfile='log/temp_sub.log', zk_root=zk_root)

sub2 = Subscriber(ip_self='tcp://127.0.0.1:6011', ip_zookeeper=zookeeper, name='Sub2',
                   comm_type=mode, logfile='log/temp_sub.log', zk_root=zk_root)

sub_t1 = threading.Thread(target=receive, args=(sub1, 1))
sub_t2 = threading.Thread(target=receive, args=(sub2, 2))

def test_start():
    zk.start()
    try:
        zk.delete(zk_root, recursive=True)
    except:
        pass
    try:
        zk.create(zk_root)
    except:
        pass
    thread1 = threading.Thread(target=broker.start)
    thread1.start()
    time.sleep(1)


def test_publisher_register():
    res1 = pub1.register([{'topic': 'hello', 'history': 3}])
    res2 = pub2.register([{'topic': 'hello', 'history': 5}])
    pubs = zk.get_children('%s/Topic/hello/Pub'%zk_root)

    assert res1 == 0 and res2 == 0
    assert len(pubs) == 2
    time.sleep(3)

def test_subscriber_register():
    res1 = sub1.register([{'topic': 'hello', 'history': 4}])
    res2 = sub2.register([{'topic': 'hello', 'history': 6}])
    subs = zk.get_children('%s/Topic/hello/Sub' % zk_root)

    sub_t1.start()
    sub_t2.start()

    assert res1 == 0 and res2 == 0
    assert len(subs) == 2

    time.sleep(3)

# sub1 cannot receive pub1 (history) or pub2 (strength)
def test_publish_and_receive1():

    pub1.publish('hello', 'test1_pub1')
    pub1.publish('hello', 'test1_pub2')

    time.sleep(0.5)
    assert sub1_msg != 'test1_pub1'


# pub1 off line. pub2 becomes the strongest, sub1 can receive from pub2.
def test_publish_and_receive2():

    pub1.drop_system()
    time.sleep(1)

    pub2.publish('hello', 'test2_pub2')
    time.sleep(3)
    print(sub1_msg, 'xxxxxxxxxxxxxxxx')
    assert sub1_msg == 'test2_pub2'

# sub2 cannot receive from pub2 (history)
def test_publish_and_receive3():

    pub2.publish('hello', 'test3_pub2')
    time.sleep(0.5)

    assert sub2_msg != 'test3_pub2'

# pub2 off line, pub1 on. sub1 still cannot receive from pub1.
def test_publish_and_receive4():

    pub2.drop_system()
    pub1.register([{'topic': 'hello', 'history': 3}])
    time.sleep(1)

    pub1.publish('hello', 'test4_pub1')
    time.sleep(0.5)

    assert sub1_msg != 'test4_pub1'


# pub1 off, pub3 on. sub1 can receive from pub3.
def test_publish_and_receive5():

    pub1.drop_system()
    res1 = pub3.register([{'topic': 'hello', 'history': 7}])
    time.sleep(1)

    pub3.publish('hello', 'test5_pub3')
    time.sleep(0.5)
    assert sub1_msg == 'test5_pub3'

def test_pub_exit():
    pub3.drop_system()

def test_sub_exit():
    global keep_receive
    keep_receive = False
    sub1.exit()
    sub2.exit()

def test_broker_exit():
    broker.stop()

def test_clean():
    try:
        zk.delete(zk_root, recursive=True)
    except:
        pass