// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter.server.rs

import com.daml.grpc.adapter.TestExecutionSequencerFactory
import org.reactivestreams.tck.flow.support.HelperPublisher
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{Assertion, BeforeAndAfterEach}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext.global
import scala.concurrent.Future

class ServerSubscriberStressTest
    extends AsyncWordSpec
    with BeforeAndAfterEach
    with Matchers
    with AsyncTimeLimitedTests {

  private val elemCount = 10000
  private val testRunCount = 50
  private val expectedElemRange = 0.until(elemCount)

  var serverCallStreamObserver: MockServerCallStreamObserver[Int] = _
  var sut: ServerSubscriber[Int] = _
  var helperPublisher: HelperPublisher[Int] = _

  override protected def beforeEach(): Unit = {
    serverCallStreamObserver = new MockServerCallStreamObserver[Int]
    val executor = TestExecutionSequencerFactory.instance.getExecutionSequencer
    sut = new ServerSubscriber[Int](serverCallStreamObserver, executor)
    helperPublisher = new HelperPublisher[Int](0, elemCount, i => i, global)
  }

  "ServerSubscriber" should {

    for (i <- 1.to(testRunCount)) {

      s"work with $elemCount elements when they are requested one by one (test run #$i)" in {
        helperPublisher.subscribe(sut)
        expectedElemRange.foreach(_ => serverCallStreamObserver.demandResponse())
        verifyExpectedElementsArrivedInOrder()
      }

    }
    for (i <- 1.to(testRunCount)) {

      s"work with $elemCount elements when they are requested in bulk (isReady stays true) (test run #$i)" in {
        helperPublisher.subscribe(sut)
        serverCallStreamObserver.demandResponse(elemCount)
        verifyExpectedElementsArrivedInOrder()
      }
    }
  }

  private def verifyExpectedElementsArrivedInOrder(): Future[Assertion] = {
    serverCallStreamObserver.elementsWhenCompleted.map { receivedElements =>
      receivedElements should contain theSameElementsInOrderAs expectedElemRange
    }
  }

  override def timeLimit: Span = 10.seconds
}
