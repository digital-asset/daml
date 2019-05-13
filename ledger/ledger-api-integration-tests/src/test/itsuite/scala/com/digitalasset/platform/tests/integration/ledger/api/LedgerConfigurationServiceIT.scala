// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.client.services.configuration.LedgerConfigurationClient
import com.digitalasset.platform.api.grpc.GrpcApiUtil
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture}
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import io.grpc.Status
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class LedgerConfigurationServiceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with TestExecutionSequencerFactory
    with AsyncTimeLimitedTests
    with Matchers
    with OptionValues {

  override def timeLimit: Span = 5.seconds

  private def client(ctx: LedgerContext): LedgerConfigurationClient =
    new LedgerConfigurationClient(getLedgerId(ctx), ctx.ledgerConfigurationService)

  "Ledger Configuration Service" when {

    "asked for ledger configuration" should {

      "return expected configuration" in allFixtures { context =>
        client(context).getLedgerConfiguration
          .runWith(Sink.head)(materializer) map { lc =>
          lc.minTtl.value shouldEqual GrpcApiUtil.durationToProto(config.timeModel.minTtl)
          lc.maxTtl.value shouldEqual GrpcApiUtil.durationToProto(config.timeModel.maxTtl)
        }
      }

      "fail with the expected status on a ledger Id mismatch" in allFixtures { context =>
        new LedgerConfigurationClient(
          "not " + getLedgerId(context),
          context.ledgerConfigurationService).getLedgerConfiguration
          .runWith(Sink.head)(materializer)
          .failed map { ex =>
          IsStatusException(Status.NOT_FOUND.getCode)(ex)
        }
      }

    }

  }

  override protected def config: Config = Config.default
}
