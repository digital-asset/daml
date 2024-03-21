// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.client

import com.daml.platform.hello.HelloResponse
import com.google.protobuf.ByteString
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.util.Random

trait ResultAssertions { self: Matchers =>

  protected def elemCount: Int = 1024
  protected lazy val elemRange: Range = 1.to(elemCount)
  protected lazy val halfCount: Int = elemCount / 2
  protected lazy val halfRange: Range = elemRange.take(halfCount)

  protected def isCancelledException(err: Throwable): Assertion = {
    err shouldBe a[StatusRuntimeException]
    err.asInstanceOf[StatusRuntimeException].getStatus.getCode shouldEqual Status.CANCELLED.getCode
  }

  protected def assertElementsAreInOrder(expectedCount: Long)(
      results: Seq[HelloResponse]
  ): Assertion = {
    results should have length expectedCount
    results.map(_.respInt) shouldEqual (1 to expectedCount.toInt)
  }

  protected def elementsAreSummed(results: Seq[HelloResponse]): Assertion = {
    results should have length 1
    results.foldLeft(0)(_ + _.respInt) shouldEqual elemRange.sum
  }

  protected def everyElementIsDoubled(results: Seq[HelloResponse]): Assertion = {
    results should have length elemCount.toLong
    // the order does matter
    results.map(_.respInt) shouldEqual elemRange.map(_ * 2)
  }

  protected def genPayload(): ByteString = {
    val bytes = new Array[Byte](1024)
    Random.nextBytes(bytes)
    ByteString.copyFrom(bytes)
  }
}
