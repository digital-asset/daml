// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.extractor.services.ExtractorFixtureAroundAll
import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.testing.postgresql.PostgresAroundAll
import com.daml.timer.RetryStrategy
import io.grpc.Status
import org.scalatest._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class VeryLargeArchiveSpec
    extends AsyncFlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Matchers
    with Inside {
  override protected def darFile = new File(rlocation("extractor/VeryLargeArchive.dar"))

  private def runWithInboundLimit[Z](bytes: Int)(f: => Z): Future[Z] = {
    val config = baseConfig.copy(ledgerPort = serverPort, ledgerInboundMessageSizeMax = bytes)
    val extractor =
      new Extractor(config, target)()

    extractor
      .run() // as with ExtractorFixture#run
      .map(_ => f)
      .andThen { case _ => extractor.shutdown() } // as with ExtractorFixture#kill
  }

  // there are a couple goals with these choices:
  //  1. ensure that we can actually observe failure when the limit is too low
  //  2. ensure that no other system we aren't reconfiguring doesn't impose a
  //     similar limit to the original 50MiB limit
  //
  // A smaller test case (with smaller numbers below) would satisfy 1, but not 2.
  //
  // That said, the only purpose is to *ensure that failure can be observed*;
  // future editors of this test should not feel obliged to synthesize a failure
  // if the system design has really changed so failures of this nature cannot
  // happen.
  val failBytes = 1024 // 1 KB
  val successBytes = 10 * 1024 * 1024 // 10 MB

  s"running with a limit of ${failBytes}B" should "fail" in {
    runWithInboundLimit(failBytes) {
      fail("shouldn't successfully run")
    }.recover {
      case RetryStrategy.FailedRetryException(
            GrpcException((GrpcStatus(Status.Code.`RESOURCE_EXHAUSTED`, Some(description)), _))
          ) =>
        description should startWith("gRPC message exceeds maximum size")
    }
  }

  s"running with a limit of ${successBytes}B" should "succeed" in {
    runWithInboundLimit(successBytes) {
      succeed
    }
  }
}
