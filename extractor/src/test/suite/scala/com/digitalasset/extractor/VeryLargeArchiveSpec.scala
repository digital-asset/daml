// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.extractor.services.ExtractorFixture
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.testing.postgresql.PostgresAroundEach
import io.grpc.StatusRuntimeException
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class VeryLargeArchiveSpec
    extends FlatSpec
    with Suite
    with PostgresAroundEach
    with SuiteResourceManagementAroundAll
    with ExtractorFixture
    with Matchers {
  override protected def darFile = new File(rlocation("extractor/VeryLargeArchive.dar"))

  private def runWithInboundLimit[Z](bytes: Int)(f: => Z): Z = {
    val config = baseConfig.copy(ledgerPort = serverPort, ledgerInboundMessageSizeMax = bytes)
    val extractor =
      new Extractor(config, target)()

    Await.result(extractor.run(), Duration.Inf) // as with ExtractorFixture#run
    try f
    finally Await.result(extractor.shutdown(), Duration.Inf) // as with ExtractorFixture#kill
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
  val failMB = 1
  val successMB = 10

  s"${failMB}MiB" should "fail" in {
    val e = the[StatusRuntimeException] thrownBy runWithInboundLimit(failMB * 1024 * 1024) {
      fail("shouldn't successfully run")
    }
    e.getStatus.getCode should ===(io.grpc.Status.Code.RESOURCE_EXHAUSTED)
    e.getStatus.getDescription should startWith("gRPC message exceeds maximum size")
  }

  s"${successMB}MiB" should "succeed" in {
    runWithInboundLimit(successMB * 1024 * 1024) {
      ()
    }
  }
}
