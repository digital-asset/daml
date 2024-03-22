// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.postgresql

import java.time.{Clock, Duration}

import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.ledger.api.testtool.infrastructure.{
  LedgerTestCase,
  LedgerTestCasesRunner,
  LedgerTestSummary,
  Result,
}
import com.daml.ledger.api.testtool.{infrastructure, suites}
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.nonrepudiation.client.SigningInterceptor
import com.daml.nonrepudiation.testing._
import com.daml.nonrepudiation.{MetricsReporterOwner, NonRepudiationProxy}
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandboxnext.{Runner => Sandbox}
import com.daml.ports.Port
import com.daml.testing.postgresql.PostgresAroundAll
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.netty.NettyChannelBuilder
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

import scala.concurrent.duration.DurationInt

final class NonRepudiationProxyConformance
    extends AsyncFlatSpec
    with Matchers
    with OptionValues
    with PostgresAroundAll
    with Inside {

  behavior of "NonRepudiationProxy"

  private val defaultTestsToRun =
    suites.v1_14
      .default(timeoutScaleFactor = 1)
      .filter {
        case _: suites.v1_8.ClosedWorldIT => false
        case _ => true
      }

  private val conformanceTestCases: Vector[LedgerTestCase] =
    defaultTestsToRun
      .flatMap(_.tests)

  it should "pass all conformance tests" in {
    implicit val context: ResourceContext = ResourceContext(executionContext)
    val config = SandboxConfig.defaultConfig.copy(
      port = Port.Dynamic,
      maxDeduplicationDuration = Some(Duration.ofSeconds(5)),
    )

    val proxyName = InProcessServerBuilder.generateName()
    val proxyBuilder = InProcessServerBuilder.forName(proxyName)
    val proxyChannel = InProcessChannelBuilder.forName(proxyName).build()

    val (key, certificate) = generateKeyAndCertificate()

    val proxy =
      for {
        sandboxPort <- new Sandbox(config)
        sandboxChannelBuilder = NettyChannelBuilder
          .forAddress("localhost", sandboxPort.value)
          .usePlaintext()
        sandboxChannel <- ResourceOwner.forChannel(
          sandboxChannelBuilder,
          shutdownTimeout = 5.seconds,
        )
        _ <- MetricsReporterOwner.slf4j[ResourceContext](period = 5.seconds)
        transactor <- createTransactor(
          postgresDatabase.url,
          postgresDatabase.userName,
          postgresDatabase.password,
          maxPoolSize = 10,
          ResourceOwner,
        )
        db = Tables.initialize(transactor)(Slf4jLogHandler(getClass))
        _ = db.certificates.put(certificate)
        proxy <- NonRepudiationProxy.owner[ResourceContext](
          sandboxChannel,
          proxyBuilder,
          db.certificates,
          db.signedPayloads,
          Clock.systemUTC(),
          CommandService.scalaDescriptor.fullName,
          CommandSubmissionService.scalaDescriptor.fullName,
        )
      } yield proxy

    proxy.use { _ =>
      val runner = new LedgerTestCasesRunner(
        testCases = conformanceTestCases,
        participantChannels = Vector(infrastructure.ChannelEndpoint.forInProcess(proxyChannel)),
        commandInterceptors = Seq(
          SigningInterceptor.signCommands(key, certificate)
        ),
        clientTlsConfiguration = config.tlsConfig,
        timeoutScaleFactor = 2,
      )

      runner.runTests.map { summaries =>
        summaries.foldLeft(succeed) { case (_, LedgerTestSummary(_, name, description, result)) =>
          withClue(s"$name: $description") {
            inside(result) { case Right(r) =>
              r shouldBe a[Result.Succeeded]
            }
          }
        }
      }
    }
  }

}
