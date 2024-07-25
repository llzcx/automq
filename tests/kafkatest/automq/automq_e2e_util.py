# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import re
import time

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.performance import ProducerPerformanceService
from kafkatest.version import DEV_BRANCH


def formatted_time(msg):
    """
    formatted the current local time with milliseconds appended to the provided message.
    """
    current_time = time.time()
    local_time = time.localtime(current_time)
    formatted_time = time.strftime('%Y-%m-%d %H:%M:%S', local_time)
    milliseconds = int((current_time - int(current_time)) * 1000)
    formatted_time_with_ms = f"{formatted_time},{milliseconds:03d}"
    return msg + formatted_time_with_ms


def parse_log_entry(log_entry):
    """
    Resolve buffer usage in the server. log
    """
    log_pattern = r'\[(.*?)\] (INFO|ERROR|DEBUG|WARN) (.*)'
    match = re.match(log_pattern, log_entry)
    if not match:
        return None

    timestamp_str, log_level, message = match.groups()
    timestamp = timestamp_str

    buffer_usage_pattern = r'Buffer usage: ByteBufAllocMetric\{(.*)\} \(.*\)'
    buffer_match = re.search(buffer_usage_pattern, message)
    if not buffer_match:
        return None

    buffer_details = buffer_match.group(1).replace(';', ',')
    buffer_parts = buffer_details.split(', ')

    buffer_dict = {}
    for part in buffer_parts:
        if '=' in part:
            key, value = part.split('=', 1)
            buffer_dict[key.strip()] = value.strip()

    log_dict = {
        'timestamp': timestamp,
        'log_level': log_level,
        'buffer_usage': buffer_dict
    }

    return log_dict


def parse_producer_performance_stdout(input_string):
    """
    Resolve the last line of producer_performance.stdout
    """
    pattern = re.compile(
        r"(?P<records_sent>\d+) records sent, "
        r"(?P<records_per_sec>[\d.]+) records/sec \([\d.]+ MB/sec\), "
        r"(?P<avg_latency>[\d.]+) ms avg latency, "
        r"(?P<max_latency>[\d.]+) ms max latency, "
        r"(?P<latency_50th>\d+) ms 50th, "
        r"(?P<latency_95th>\d+) ms 95th, "
        r"(?P<latency_99th>\d+) ms 99th, "
        r"(?P<latency_999th>\d+) ms 99.9th."
    )
    match = pattern.match(input_string)

    if match:
        data = match.groupdict()
        extracted_data = {
            'records_sent': int(data['records_sent']),
            'records_per_sec': float(data['records_per_sec']),
            'avg_latency': float(data['avg_latency']),
            'max_latency': float(data['max_latency']),
            'latency_50th': int(data['latency_50th']),
            'latency_95th': int(data['latency_95th']),
            'latency_99th': int(data['latency_99th']),
            'latency_999th': int(data['latency_999th'])
        }
        return extracted_data
    else:
        raise ValueError("Input string does not match the expected format.")

def publish_broker_configuration(kafka, producer_byte_rate, consumer_byte_rate, broker_id):
    force_use_zk_connection = False
    node = kafka.nodes[0]
    cmd = "%s --alter --add-config broker.quota.produce.bytes=%d,broker.quota.fetch.bytes=%d" % \
          (kafka.kafka_configs_cmd_with_optional_security_settings(node, force_use_zk_connection), producer_byte_rate,
           consumer_byte_rate)
    cmd += " --entity-type " + 'brokers'
    cmd += " --entity-name " + broker_id
    node.account.ssh(cmd)


RECORD_SIZE = 3000
RECORD_NUM = 50000
TOPIC = 'test_topic'
CONSUMER_GROUP = "test_consume_group"
DEFAULT_CONSUMER_CLIENT_ID = 'test_producer_client_id'
DEFAULT_PRODUCER_CLIENT_ID = 'test_producer_client_id'
BATCH_SIZE = 16 * 1024
BUFFER_MEMORY = 64 * 1024 * 1024
DEFAULT_THROUGHPUT = -1
DEFAULT_CLIENT_VERSION = DEV_BRANCH
JMX_BROKER_IN = 'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec'
JMX_BROKER_OUT = 'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec'
JMX_TOPIC_IN = f'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic={TOPIC}'
JMX_TOPIC_OUT = f'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic={TOPIC}'
JMX_ONE_MIN = ':OneMinuteRate'


def run_perf_producer(test_context, kafka, num_records=RECORD_NUM, throughput=DEFAULT_THROUGHPUT,
                      client_version=DEFAULT_CLIENT_VERSION, topic=TOPIC):
    producer = ProducerPerformanceService(
        test_context, 1, kafka,
        topic=topic, num_records=num_records, record_size=RECORD_SIZE, throughput=throughput,
        client_id=DEFAULT_PRODUCER_CLIENT_ID, version=client_version,
        settings={
            'acks': 1,
            'compression.type': "none",
            'batch.size': BATCH_SIZE,
            'buffer.memory': BUFFER_MEMORY
        })
    producer.run()
    return producer


def run_console_consumer(test_context, kafka, client_version=DEFAULT_CLIENT_VERSION, topic=TOPIC):
    consumer = ConsoleConsumer(test_context, 1, topic=topic, kafka=kafka,
                               consumer_timeout_ms=60000, client_id=DEFAULT_CONSUMER_CLIENT_ID,
                               jmx_object_names=[
                                   'kafka.consumer:type=consumer-fetch-manager-metrics,client-id=%s' % DEFAULT_CONSUMER_CLIENT_ID],
                               jmx_attributes=['bytes-consumed-rate'], version=client_version)
    consumer.run()
    for idx, messages in consumer.messages_consumed.items():
        assert len(messages) > 0, "consumer %d didn't consume any message before timeout" % idx
    return consumer


def validate_num(producer, consumer, logger):
    success = True
    msg = ''
    # validate that number of consumed messages equals number of produced messages
    produced_num = sum([value['records'] for value in producer.results])
    consumed_num = sum([len(value) for value in consumer.messages_consumed.values()])
    logger.info('producer produced %d messages' % produced_num)
    logger.info('consumer consumed %d messages' % consumed_num)
    if produced_num != consumed_num:
        success = False
        msg += "number of produced messages %d doesn't equal number of consumed messages %d\n" % (
            produced_num, consumed_num)
    return success, msg


def run_simple_load(test_context, kafka, logger, topic=TOPIC, num_records=RECORD_NUM):
    producer = run_perf_producer(test_context=test_context, kafka=kafka, topic=topic, num_records=num_records)
    consumer = run_console_consumer(test_context=test_context, kafka=kafka)
    return validate_num(producer, consumer, logger)
