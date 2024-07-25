#  Copyright 2024, AutoMQ HK Limited.
#
#  Use of this software is governed by the Business Source License
#  included in the file BSL.md
#
#  As of the Change Date specified in that file, in accordance with
#  the Business Source License, use of this software will be governed
#  by the Apache License, Version 2.0
#
# Use of this software is governed by the Business Source License
# included in the file BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from kafkatest.automq.automq_e2e_util import (run_simple_load, TOPIC,
                                              JMX_BROKER_IN, JMX_BROKER_OUT, JMX_ONE_MIN)
from kafkatest.services.kafka import KafkaService

REPORT_INTERVAL = 'autobalancer.reporter.metrics.reporting.interval.ms'
DETECT_INTERVAL = 'autobalancer.controller.anomaly.detect.interval.ms'
ENABLE = 'autobalancer.controller.enable'
IN_AVG_DEVIATION = 'autobalancer.controller.network.in.distribution.detect.avg.deviation'
OUT_AVG_DEVIATION = 'autobalancer.controller.network.out.distribution.detect.avg.deviation'
OUT_THRESHOLD = 'autobalancer.controller.network.out.usage.distribution.detect.threshold'
IN_THRESHOLD = 'autobalancer.controller.network.in.usage.distribution.detect.threshold'
GOALS = 'autobalancer.controller.goals'
EXCLUDE_TOPIC = 'autobalancer.controller.exclude.topics'
EXCLUDE_BROKER = 'autobalancer.controller.exclude.broker.ids'
METRIC_REPORTERS = 'metric.reporters'


def validate(value_name, value_list, rate):
    success = True
    msg = ''
    avg = sum(value_list) / len(value_list)
    for idx, value in enumerate(value_list):
        deviation = abs(value - avg) / avg
        if deviation > rate:
            success = False
            msg += (f"{value_name} value {value}(broker_id:{idx}) deviates from the average {avg} by {deviation}, which "
                    f"exceeds the allowed range of {rate}. \n")
    return success, msg


class AutoBalancerTest(Test):
    """
    Test AutoBalancer
    """

    def __init__(self, test_context):
        super(AutoBalancerTest, self).__init__(test_context)
        self.context = test_context
        self.start = False
        self.topic = TOPIC
        self.avg_deviation = 0.2

    def create_kafka(self, num_nodes=3, partition=6, jmx_object_names=None, exclude_broker=None, exclude_topic=None):
        log_size = 256 * 1024 * 1024
        block_size = 256 * 1024 * 1024
        threshold = 512 * 1024
        server_prop_overrides = [
            ['s3.wal.cache.size', str(log_size)],
            ['s3.wal.capacity', str(log_size)],
            ['s3.wal.upload.threshold', str(log_size // 4)],
            ['s3.block.cache.size', str(block_size)],
            [ENABLE, 'true'],
            [IN_AVG_DEVIATION, str(self.avg_deviation)],
            [OUT_AVG_DEVIATION, str(self.avg_deviation)],
            [GOALS,
             'kafka.autobalancer.goals.NetworkInUsageDistributionGoal,'
             'kafka.autobalancer.goals.NetworkOutUsageDistributionGoal'],
            [IN_THRESHOLD, str(threshold)],
            [OUT_THRESHOLD, str(threshold)],
            [REPORT_INTERVAL, str(4000)],
            [DETECT_INTERVAL, str(8000)],
            [METRIC_REPORTERS, 'kafka.autobalancer.metricsreporter.AutoBalancerMetricsReporter'],
        ]

        # if not exclude_broker:
        #     server_prop_overrides.append([EXCLUDE_BROKER, exclude_broker])
        #
        # if not exclude_topic:
        #     server_prop_overrides.append([EXCLUDE_TOPIC, exclude_topic])

        self.kafka = KafkaService(self.context, num_nodes=num_nodes, zk=None,
                                  kafka_heap_opts="-Xmx2048m -Xms2048m",
                                  server_prop_overrides=server_prop_overrides,
                                  topics={
                                      self.topic: {
                                          'partitions': partition,
                                          'replication-factor': 1,
                                          "replica-assignment": "1,1,1,2,2,3"
                                      }
                                  },
                                  jmx_object_names=['kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec',
                                                    'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec'],
                                  jmx_attributes=['OneMinuteRate']
                                  )
        self.start = True

    @cluster(num_nodes=5)
    @parametrize(jmx_type=[JMX_BROKER_IN, JMX_BROKER_OUT], exclude_broker='1', exclude_topic=None)
    # @parametrize(jmx_type=[JMX_TOPIC_IN, JMX_TOPIC_OUT], exclude_broker=None, exclude_topic=TOPIC)
    def test_white_list(self, jmx_type, exclude_broker, exclude_topic):
        type_in = jmx_type[0]
        type_out = jmx_type[1]
        self.create_kafka(jmx_object_names=[type_in, type_out], exclude_broker=exclude_broker,
                          exclude_topic=exclude_topic)
        self.kafka.start()
        print(f"Partitions before reassignment:{str(self.kafka.parse_describe_topic(self.kafka.describe_topic(TOPIC)))}")
        success, msg = run_simple_load(test_context=self.context, kafka=self.kafka, logger=self.logger,
                                       topic=self.topic, num_records=20000)
        print(f"Partitions after reassignment:{str(self.kafka.parse_describe_topic(self.kafka.describe_topic(TOPIC)))}")
        broker = self.kafka
        broker.read_jmx_output_all_nodes()

        type_in += JMX_ONE_MIN
        type_out += JMX_ONE_MIN
        broker_max_in_per_node = [node[type_in] for node in broker.maximum_jmx_value_per_node]
        broker_max_out_per_node = [node[type_out] for node in broker.maximum_jmx_value_per_node]

        broker_avg_in_per_node = [node[type_in] for node in broker.average_jmx_value_per_node]
        broker_avg_out_per_node = [node[type_out] for node in broker.average_jmx_value_per_node]

        print(f'broker_max_in_per_node:{broker_max_in_per_node}')
        print(f'broker_max_out_per_node:{broker_max_out_per_node}')
        print(f'broker_avg_in_per_node:{broker_avg_in_per_node}')
        print(f'broker_avg_out_per_node:{broker_avg_out_per_node}')

        success_in, msg_in = validate('broker_max_in_per_node', broker_max_in_per_node, self.avg_deviation)
        success_out, msg_out = validate('broker_max_out_per_node', broker_max_out_per_node, self.avg_deviation)

        # Combine results
        final_success = success and success_in and success_out
        final_msg = msg
        if not success_in:
            final_msg += '\n' + msg_in
        if not success_out:
            final_msg += '\n' + msg_out

        # Final assert
        assert final_success, final_msg
