/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka

import com.automq.shell.log.S3LogConfig
import com.automq.stream.s3.operator.{ObjectStorage, ObjectStorageFactory}
import kafka.server.{KafkaConfig, KafkaRaftServer, KafkaServer}

class KafkaS3LogConfig(
  config: KafkaConfig,
  kafkaServer: KafkaServer,
  kafkaRaftServer: KafkaRaftServer
) extends S3LogConfig {

  private val _objectStorage = if (config.automq.opsBuckets().isEmpty) {
    null
  } else {
    ObjectStorageFactory.instance().builder(config.automq.opsBuckets().get(0)).threadPrefix("s3-log").build()
  }

  override def isEnabled: Boolean = config.s3OpsTelemetryEnabled

  override def isActiveController: Boolean = {

    if (kafkaServer != null) {
      false
    } else {
      kafkaRaftServer.controller.exists(controller => controller.controller != null && controller.controller.isActive)
    }
  }

  override def clusterId(): String = {
    if (kafkaServer != null) {
      kafkaServer.clusterId
    } else {
      kafkaRaftServer.getSharedServer().clusterId
    }
  }

  override def nodeId(): Int = config.nodeId

  override def objectStorage(): ObjectStorage = {
    _objectStorage
  }

}
