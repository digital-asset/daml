// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.config

import com.daml.jwt.JwtTimestampLeeway
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.{ApiServerConfig, AuthServiceConfig}
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.indexer.IndexerConfig
import com.daml.platform.store.DbSupport.{ConnectionPoolConfig, DataSourceProperties}

import scala.concurrent.duration._

final case class ParticipantConfig(
    apiServer: ApiServerConfig = ApiServerConfig(),
    authentication: AuthServiceConfig = AuthServiceConfig.Wildcard,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    dataSourceProperties: DataSourceProperties = DataSourceProperties(
      connectionPool = ConnectionPoolConfig(
        connectionPoolSize = 16,
        connectionTimeout = 250.millis,
      )
    ),
    indexService: IndexServiceConfig = IndexServiceConfig(),
    indexer: IndexerConfig = IndexerConfig(),
    participantIdOverride: Option[Ref.ParticipantId] = None,
    servicesThreadPoolSize: Int = ParticipantConfig.DefaultServicesThreadPoolSize,
)

object ParticipantConfig {
  def defaultIndexJdbcUrl(participantId: Ref.ParticipantId): String =
    s"jdbc:h2:mem:$participantId;db_close_delay=-1;db_close_on_exit=false"
  val DefaultParticipantId: Ref.ParticipantId = Ref.ParticipantId.assertFromString("default")
  val DefaultServicesThreadPoolSize: Int = Runtime.getRuntime.availableProcessors()
}
