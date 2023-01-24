// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.fixture

import com.daml.ledger.api.testing.utils.{OwnedResource, Resource, SuiteResource}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ledger.sandbox.SandboxOnXForTest.{ConfigAdaptor, dataSource}
import com.daml.ledger.sandbox.{SandboxOnXForTest, SandboxOnXRunner}
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.sandbox.UploadPackageHelper._
import com.daml.platform.sandbox.{AbstractSandboxFixture, SandboxRequiringAuthorizationFuns}
import com.daml.ports.Port
import io.grpc.Channel
import org.scalatest.Suite

import scala.concurrent.duration._

trait SandboxFixture
    extends AbstractSandboxFixture
    with SuiteResource[(Port, Channel)]
    with SandboxRequiringAuthorizationFuns {
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

        cfg = config.withDataSource(
          dataSource(jdbcUrl.getOrElse(SandboxOnXForTest.defaultH2SandboxJdbcUrl()))
        )
        port <- SandboxOnXRunner.owner(
          ConfigAdaptor(authService, idpJwtVerifierLoader),
          cfg,
          bridgeConfig,
          registerGlobalOpenTelemetry = false,
        )
        channel <- GrpcClientResource.owner(port)
        client = adminLedgerClient(port, cfg, jwtSecret)(
          system.dispatcher,
          executionSequencerFactory,
        )
        _ <- ResourceOwner.forFuture(() => uploadDarFiles(client, packageFiles)(system.dispatcher))
      } yield (port, channel),
      acquisitionTimeout = 1.minute,
      releaseTimeout = 1.minute,
    )
  }
}
