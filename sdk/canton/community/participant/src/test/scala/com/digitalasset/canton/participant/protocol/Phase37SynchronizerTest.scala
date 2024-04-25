// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.Phase37Synchronizer.RequestOutcome
import com.digitalasset.canton.participant.protocol.ProcessingSteps.RequestType
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.WrappedPendingRequestData
import com.digitalasset.canton.participant.protocol.TestProcessingSteps.{
  TestPendingRequestData,
  TestPendingRequestDataType,
}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.sequencing.protocol.MediatorsOfDomain
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.{BaseTest, HasExecutionContext, RequestCounter, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class Phase37SynchronizerTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private def mk(): Phase37Synchronizer =
    new Phase37Synchronizer(loggerFactory, FutureSupervisor.Noop, timeouts)

  private val requestId1 = RequestId(CantonTimestamp.ofEpochSecond(1))
  private val requestId2 = RequestId(CantonTimestamp.ofEpochSecond(2))

  private val requestType = TestPendingRequestDataType

  private def pendingRequestDataFor(
      i: Long
  ): WrappedPendingRequestData[TestPendingRequestData] =
    WrappedPendingRequestData(
      TestPendingRequestData(
        RequestCounter(i),
        SequencerCounter(i),
        MediatorsOfDomain(MediatorGroupIndex.one),
        locallyRejected = false,
      )
    )

  "return after reaching confirmed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    p37s
      .registerRequest(requestType)(requestId1)
      .complete(
        Some(pendingRequestData)
      )
    p37s
      .awaitConfirmed(requestType)(requestId1)
      .failOnShutdown
      .futureValue shouldBe RequestOutcome.Success(pendingRequestData)
  }

  "return after reaching confirmed (for request timeout)" in {
    val p37s = mk()

    p37s.registerRequest(requestType)(requestId1).complete(None)
    p37s
      .awaitConfirmed(requestType)(requestId1)
      .failOnShutdown
      .futureValue shouldBe RequestOutcome.AlreadyServedOrTimeout
  }

  "return only after reaching confirmed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val handle = p37s.registerRequest(requestType)(requestId1)
    val f = p37s.awaitConfirmed(requestType)(requestId1)
    assert(!f.isCompleted)

    handle.complete(
      Some(pendingRequestData)
    )
    f.failOnShutdown.futureValue shouldBe RequestOutcome.Success(pendingRequestData)
  }

  "return only after reaching confirmed (for request timeout)" in {
    val p37s = mk()

    val handle = p37s.registerRequest(requestType)(requestId1)
    val f = p37s.awaitConfirmed(requestType)(requestId1)
    assert(!f.isCompleted)

    handle.complete(None)
    f.failOnShutdown.futureValue shouldBe RequestOutcome.AlreadyServedOrTimeout
  }

  "return after request is marked as timeout and the memory cleaned" in {
    val p37s = mk()

    val handle = p37s.registerRequest(requestType)(requestId1)
    handle.complete(None)

    eventually() {
      p37s.memoryIsCleaned(requestType)(requestId1) shouldBe true
    }
    p37s
      .awaitConfirmed(requestType)(requestId1)
      .failOnShutdown
      .futureValue shouldBe RequestOutcome.AlreadyServedOrTimeout
  }

  "return value only once after reaching confirmed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val handle = p37s.registerRequest(requestType)(requestId1)

    val f1 = p37s.awaitConfirmed(requestType)(requestId1)
    assert(!f1.isCompleted)

    handle.complete(Some(pendingRequestData))

    val f2 = p37s.awaitConfirmed(requestType)(requestId1)

    f1.failOnShutdown.futureValue shouldBe RequestOutcome.Success(pendingRequestData)
    f2.failOnShutdown.futureValue shouldBe RequestOutcome.AlreadyServedOrTimeout
  }

  "complain if multiple registers have been called for the same requestID" in {
    val p37s = mk()
    p37s.registerRequest(requestType)(requestId1)
    loggerFactory.assertThrowsAndLogs[IllegalStateException](
      p37s.registerRequest(requestType)(requestId1),
      entry => {
        entry.throwable
          .map(
            _.getMessage should
              fullyMatch regex raw"Request RequestId(\S+) has already been registered"
          )
          .getOrElse(fail("the request is registered twice"))
      },
    )
  }

  "deal with several calls for the same request" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val handle = p37s.registerRequest(requestType)(requestId1)

    val f1 = p37s.awaitConfirmed(requestType)(requestId1)
    val f2 = p37s.awaitConfirmed(requestType)(requestId1)

    assert(!f1.isCompleted)
    assert(!f2.isCompleted)

    handle.complete(Some(pendingRequestData))

    val f3 = p37s.awaitConfirmed(requestType)(requestId1)

    f1.failOnShutdown.futureValue shouldBe RequestOutcome.Success(pendingRequestData)
    forAll(Seq(f2, f3))(fut =>
      fut.failOnShutdown.futureValue shouldBe RequestOutcome.AlreadyServedOrTimeout
    )
  }

  "no valid confirms" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val handle = p37s.registerRequest(requestType)(requestId1)

    val f1 =
      p37s.awaitConfirmed(requestType)(
        requestId1,
        _ => Future.successful(false),
      )
    val f2 =
      p37s.awaitConfirmed(requestType)(
        requestId1,
        _ => Future.successful(false),
      )
    val f3 =
      p37s.awaitConfirmed(requestType)(
        requestId1,
        _ => Future.successful(false),
      )

    assert(!f1.isCompleted)
    assert(!f2.isCompleted)
    assert(!f3.isCompleted)

    handle.complete(Some(pendingRequestData))

    forAll(Seq(f1, f2, f3))(fut => fut.failOnShutdown.futureValue shouldBe RequestOutcome.Invalid)
  }

  "deal with several calls for the same unconfirmed request with different filters" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    val handle = p37s.registerRequest(requestType)(requestId1)

    val f1 =
      p37s.awaitConfirmed(requestType)(
        requestId1,
        _ => Future.successful(true),
      )
    val f2 =
      p37s.awaitConfirmed(requestType)(
        requestId1,
        _ => Future.successful(false),
      )
    val f3 =
      p37s.awaitConfirmed(requestType)(
        requestId1,
        _ => Future.successful(true),
      )

    assert(!f1.isCompleted)
    assert(!f2.isCompleted)
    assert(!f3.isCompleted)

    handle.complete(Some(pendingRequestData))

    val f4 = p37s
      .awaitConfirmed(requestType)(
        requestId1,
        _ => Future.successful(true),
      )

    f1.failOnShutdown.futureValue shouldBe RequestOutcome.Success(pendingRequestData)
    forAll(Seq(f2, f3, f4))(fut =>
      fut.failOnShutdown.futureValue shouldBe RequestOutcome.AlreadyServedOrTimeout
    )
  }

  "deal with several calls for the same confirmed request with different filters" in {
    val p37s = mk()
    val pendingRequestData0 = pendingRequestDataFor(0)
    val pendingRequestData1 = pendingRequestDataFor(1)

    p37s
      .registerRequest(requestType)(requestId1)
      .complete(
        Some(pendingRequestData0)
      )

    val f1 = p37s
      .awaitConfirmed(requestType)(requestId1, _ => Future.successful(true))
      .failOnShutdown
    val f2 = p37s
      .awaitConfirmed(requestType)(requestId1, _ => Future.successful(false))
      .failOnShutdown
    val f3 = p37s
      .awaitConfirmed(requestType)(requestId1, _ => Future.successful(true))
      .failOnShutdown

    f1.futureValue shouldBe RequestOutcome.Success(pendingRequestData0)
    forAll(Seq(f2, f3))(fut => fut.futureValue shouldBe RequestOutcome.AlreadyServedOrTimeout)

    p37s
      .registerRequest(requestType)(requestId2)
      .complete(
        Some(pendingRequestData1)
      )
    val f4 = p37s
      .awaitConfirmed(requestType)(requestId2, _ => Future.successful(false))
      .failOnShutdown
    val f5 = p37s
      .awaitConfirmed(requestType)(requestId2, _ => Future.successful(true))
      .failOnShutdown
    val f6 = p37s
      .awaitConfirmed(requestType)(requestId2, _ => Future.successful(false))
      .failOnShutdown

    f4.futureValue shouldBe RequestOutcome.Invalid
    f5.futureValue shouldBe RequestOutcome.Success(pendingRequestData1)
    f6.futureValue shouldBe RequestOutcome.AlreadyServedOrTimeout
  }

  "memory is cleaned if a request is valid and completed" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    p37s
      .registerRequest(requestType)(requestId1)
      .complete(
        Some(pendingRequestData)
      )
    p37s
      .awaitConfirmed(requestType)(requestId1)
      .failOnShutdown
      .futureValue shouldBe RequestOutcome.Success(pendingRequestData)

    p37s
      .registerRequest(requestType)(requestId2)
      .complete(
        Some(pendingRequestData)
      )

    p37s.memoryIsCleaned(requestType)(requestId1) shouldBe true
    p37s.memoryIsCleaned(requestType)(requestId2) shouldBe false
  }

  "asynchronous interleaving of request handlers" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    p37s
      .registerRequest(requestType)(requestId1)
      .complete(Some(pendingRequestData))

    var f1: Future[RequestOutcome[TestPendingRequestData]] =
      Future.successful(RequestOutcome.Invalid)
    var f2: Future[RequestOutcome[TestPendingRequestData]] =
      Future.successful(RequestOutcome.Invalid)
    var f4: Future[RequestOutcome[TestPendingRequestData]] =
      Future.successful(RequestOutcome.Invalid)
    val f3 = p37s
      .awaitConfirmed(requestType)(
        requestId1,
        _ => {
          Future({
            f1 = p37s
              .awaitConfirmed(requestType)(
                requestId1,
                _ => {
                  Future({
                    f2 = p37s
                      .awaitConfirmed(requestType)(
                        requestId1,
                        _ =>
                          Future {
                            f4 = p37s
                              .awaitConfirmed(requestType)(
                                requestId1,
                                _ => Future.successful(true),
                              )
                              .failOnShutdown
                            true
                          },
                      )
                      .failOnShutdown
                    false
                  })
                },
              )
              .failOnShutdown
            false
          })
        },
      )
      .failOnShutdown

    eventually() {
      f1.futureValue shouldBe RequestOutcome.Invalid
      f2.futureValue shouldBe RequestOutcome.Success(pendingRequestData)
      f3.futureValue shouldBe RequestOutcome.Invalid
      f4.futureValue shouldBe RequestOutcome.AlreadyServedOrTimeout
    }
  }

  "distinguish requests of different types" in {
    val p37s = mk()
    val pendingRequestData = pendingRequestDataFor(0)

    case object AnotherTestPendingRequestDataType extends RequestType {
      override type PendingRequestData = ProcessingSteps.PendingRequestData
    }

    p37s
      .registerRequest(requestType)(requestId1)
      .complete(Some(pendingRequestData))
    p37s
      .awaitConfirmed(AnotherTestPendingRequestDataType)(requestId1)
      .failOnShutdown
      .futureValue shouldBe RequestOutcome.AlreadyServedOrTimeout
    p37s.awaitConfirmed(requestType)(requestId1).failOnShutdown.futureValue shouldBe RequestOutcome
      .Success(
        pendingRequestData
      )
  }

}
