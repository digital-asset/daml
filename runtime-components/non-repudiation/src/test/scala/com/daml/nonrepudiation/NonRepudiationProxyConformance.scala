// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.KeyPairGenerator

import com.daml.ledger.api.testtool.infrastructure.{
  LedgerTestCasesRunner,
  LedgerTestSummary,
  Result,
}
import com.daml.ledger.api.testtool.suites.ClosedWorldIT
import com.daml.ledger.api.testtool.tests._
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.nonrepudiation.client.SigningInterceptor
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandboxnext.{Runner => Sandbox}
import com.daml.ports.Port
import io.grpc.netty.NettyChannelBuilder
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

final class NonRepudiationProxyConformance extends AsyncFlatSpec with Matchers with EitherValues {

  import NonRepudiationProxySpec._
  import NonRepudiationProxyConformance.ConformanceTestCases

  behavior of "NonRepudiationProxy"

  it should "pass all conformance tests" in {
    implicit val context: ResourceContext = ResourceContext(executionContext)
    val config = SandboxConfig.defaultConfig.copy(port = Port.Dynamic)
    val Setup(keys, signedPayloads, proxyBuilder, proxyChannel) = Setup.newInstance[CommandIdString]
    val keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair()
    keys.put(keyPair.getPublic)

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
        proxy <- NonRepudiationProxy.owner[ResourceContext](
          sandboxChannel,
          proxyBuilder,
          keys,
          signedPayloads,
          CommandService.scalaDescriptor.fullName,
          CommandSubmissionService.scalaDescriptor.fullName,
        )
      } yield proxy

    proxy.use { _ =>
      val runner = new LedgerTestCasesRunner(
        testCases = ConformanceTestCases,
        participants = Vector(proxyChannel),
        commandInterceptors = Seq(
          new SigningInterceptor(keyPair, AlgorithmString.SHA256withRSA)
        ),
      )

      runner.runTests.map { summaries =>
        summaries.foldLeft(succeed) { case (_, LedgerTestSummary(_, name, description, result)) =>
          withClue(s"$name: $description") {
            result.right.value shouldBe a[Result.Succeeded]
          }
        }
      }
    }
  }

}

object NonRepudiationProxyConformance {

  private val ConformanceTestCases =
    Tests
      .default(ledgerClockGranularity = 1.second)
      .filter {
        case _: ClosedWorldIT => false
        case _ => true
      }
      .flatMap(_.tests)

}
