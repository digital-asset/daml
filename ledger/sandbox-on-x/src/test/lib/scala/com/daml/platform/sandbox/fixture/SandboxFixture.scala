// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.fixture

import com.daml.ledger.api.testing.utils.{OwnedResource, Resource, SuiteResource}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common.Config.SandboxParticipantId
import com.daml.ledger.sandbox.{ConfigConverter, NewSandboxServer}
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.sandbox.AbstractSandboxFixture
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.ports.Port
import io.grpc.Channel
import org.scalatest.Suite

import scala.concurrent.duration._

trait SandboxFixture extends AbstractSandboxFixture with SuiteResource[(Port, Channel)] {
  self: Suite =>

  override protected def serverPort: Port = suiteResource.value._1

  override protected def channel: Channel = suiteResource.value._2

  override protected lazy val suiteResource: Resource[(Port, Channel)] = {
    implicit val resourceContext: ResourceContext = ResourceContext(system.dispatcher)
    new OwnedResource[ResourceContext, (Port, Channel)](
      for {
        jdbcUrl <- database
          .fold[ResourceOwner[Option[String]]](ResourceOwner.successful(None))(
            _.map(info => Some(info.jdbcUrl))
          )

        participantDataSource = jdbcUrl match {
          case Some(url) => Map(SandboxParticipantId -> ParticipantDataSourceConfig(url))
          case None =>
            Map(
              SandboxParticipantId -> ParticipantDataSourceConfig(
                ConfigConverter.defaultH2SandboxJdbcUrl()
              )
            )
        }

        cfg = newConfig.copy(
          genericConfig = newConfig.genericConfig.copy(
            dataSource = participantDataSource
          )
        )
        port <- NewSandboxServer.owner(cfg)
        channel <- GrpcClientResource.owner(port)
      } yield (port, channel),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }
}
