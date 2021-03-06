from re import T
from posthub.pubsub import PubSub
import os
import json
import time

db_url = os.environ.get('DB_URL', 'postgresql://postgres:postgres@localhost/postgres')

def test_simple_pub_sub_1():
    pub = PubSub('test_channel_1', db_url=db_url)

    sub = PubSub('test_channel_1', db_url=db_url)
    sub.subscribe()

    time.sleep(0.5)
    m = {'text':'hello'}
    pub.publish(m)

    listened = sub.listen(once=True)
    msg_list = list(listened)
    assert len(msg_list) == 1
    for msg in msg_list:
        assert type(msg) == dict
        assert msg['channel'] == 'test_channel_1'
        assert msg['payload'] == json.dumps(m)

def test_simple_pub_sub_2():
    msg_list = []
    def got_message(x):
        msg_list.append(x)
    pub = PubSub('test_channel_2', db_url=db_url)

    sub = PubSub('test_channel_2', db_url=db_url)
    sub.subscribe(got_message)

    time.sleep(0.5)

    m = {'text':'hello2'}
    pub.publish(m)

    sub.run_loop(max_got=1)
    assert len(msg_list) == 1
    for msg in msg_list:
        assert type(msg) == dict
        assert msg['channel'] == 'test_channel_2'
        assert msg['payload'] == json.dumps(m)

def test_simple_pub_multi_sub_1():
    msg_list1 = []
    def got_message1(x):
        msg_list1.append(x)

    msg_list2 = []
    def got_message2(x):
        msg_list2.append(x)
    pub = PubSub('test_channel_multi_1', db_url=db_url)

    sub1 = PubSub('test_channel_multi_1', db_url=db_url)
    sub1.subscribe(got_message1)

    sub2 = PubSub('test_channel_multi_1', db_url=db_url)
    sub2.subscribe(got_message2)

    time.sleep(0.5)

    m = {'text':'hello345'}
    pub.publish(m)

    sub1.run_loop(max_got=1)
    sub2.run_loop(max_got=1)
    assert len(msg_list1) == 1
    assert len(msg_list2) == 1
    for msg in msg_list1:
        assert type(msg) == dict
        assert msg['channel'] == 'test_channel_multi_1'
        assert msg['payload'] == json.dumps(m)

    for msg in msg_list2:
        assert type(msg) == dict
        assert msg['channel'] == 'test_channel_multi_1'
        assert msg['payload'] == json.dumps(m)