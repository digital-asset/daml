// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import com.digitalasset.extractor.services.ExtractorFixture
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.platform.sandbox.persistence.PostgresAroundEach

import scala.concurrent.Await
import scala.concurrent.duration._
import io.grpc.StatusRuntimeException
import org.scalatest._

import java.io.File

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class VeryLargeArchiveSpec
    extends FlatSpec
    with Suite
    with PostgresAroundEach
    with SuiteResourceManagementAroundAll
    with ExtractorFixture
    with Matchers {
  override protected def darFile = new File("extractor/VeryLargeArchive.dar")

  private def runWithInboundLimit[Z](bytes: Int)(f: => Z): Z = {
    val config = baseConfig.copy(ledgerPort = getSandboxPort, ledgerInboundMessageSizeMax = bytes)
    val extractor = new Extractor(config, target)
    Await.result(extractor.run(), Duration.Inf) // as with ExtractorFixture#run
    try f
    finally Await.result(extractor.shutdown(), Duration.Inf) // as with ExtractorFixture#kill
  }

  val failMB = 50
  val successMB = 60

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
