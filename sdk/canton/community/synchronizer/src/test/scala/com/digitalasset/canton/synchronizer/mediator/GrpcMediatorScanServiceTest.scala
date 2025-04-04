// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.CantonTimestamp.Epoch
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.mediator.admin.v30.VerdictsResponse
import com.digitalasset.canton.protocol.messages.InformeeMessage
import com.digitalasset.canton.protocol.{GeneratorsProtocol, RequestId}
import com.digitalasset.canton.synchronizer.mediator.MediatorVerdict.MediatorApprove
import com.digitalasset.canton.synchronizer.mediator.service.GrpcMediatorScanService
import com.digitalasset.canton.synchronizer.mediator.store.InMemoryFinalizedResponseStore
import com.digitalasset.canton.synchronizer.service.RecordStreamObserverItems
import com.digitalasset.canton.time.TimeAwaiter
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import io.grpc.stub.ServerCallStreamObserver
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}
import scala.util.Random

class GrpcMediatorScanServiceTest extends AsyncWordSpec with BaseTest {

  // use our generators to generate a random full informee tree
  private val fullInformeeTree = new GeneratorsData(
    testedProtocolVersion,
    new GeneratorsProtocol(testedProtocolVersion),
  ).fullInformeeTreeArb.arbitrary.sample.value

  private val informeeMessage: InformeeMessage =
    InformeeMessage(
      fullInformeeTree,
      Signature.noSignature,
    )(testedProtocolVersion)

  private def mkResponse(
      ts: CantonTimestamp,
      version: CantonTimestamp,
      isAccept: Boolean = true,
  ): FinalizedResponse =
    FinalizedResponse(
      RequestId(ts),
      informeeMessage,
      version,
      if (isAccept)
        MediatorApprove.toVerdict(testedProtocolVersion)
      else
        MediatorVerdict
          .MediatorReject(
            MediatorError.Timeout.Reject(
              unresponsiveParties = DefaultTestIdentities.party1.toLf
            )
          )
          .toVerdict(testedProtocolVersion),
    )(TraceContext.empty)

  class Fixture(val batchSize: Int = 5) {
    val finalizedResponseStore = new InMemoryFinalizedResponseStore(loggerFactory)

    @volatile var latestKnownWatermark: CantonTimestamp = CantonTimestamp.MinValue
    val timeAwaiter = new TimeAwaiter(() => latestKnownWatermark, timeouts, loggerFactory)

    val scanService = new GrpcMediatorScanService(
      finalizedResponseStore,
      timeAwaiter,
      batchSize = PositiveInt.tryCreate(batchSize),
      loggerFactory = loggerFactory,
    )
    implicit val cc: CloseContext = CloseContext(new FlagCloseable {
      override protected def timeouts: ProcessingTimeout = GrpcMediatorScanServiceTest.this.timeouts
      override protected def logger: TracedLogger = GrpcMediatorScanServiceTest.this.logger
    })

    def requestVerdictsFrom(
        fromRequestExclusive: CantonTimestamp,
        numExpected: Int,
    ): (
        ServerCallStreamObserver[v30.VerdictsResponse] & RecordStreamObserverItems[
          v30.VerdictsResponse
        ],
        Future[Seq[v30.VerdictsResponse]],
    ) = {

      val promise = Promise[Seq[v30.VerdictsResponse]]()
      val counter = new AtomicInteger(numExpected)
      val observer = new ServerCallStreamObserver[v30.VerdictsResponse]
        with RecordStreamObserverItems[v30.VerdictsResponse] {
        @volatile var isCancelled_ = false
        override def isCancelled: Boolean = isCancelled_
        override def setOnCancelHandler(onCancelHandler: Runnable): Unit = ()
        override def setOnCloseHandler(onCloseHandler: Runnable): Unit = ()
        override def setCompression(compression: String): Unit = ???
        override def isReady: Boolean = ???
        override def setOnReadyHandler(onReadyHandler: Runnable): Unit = ???
        override def request(count: Int): Unit = ???
        override def setMessageCompression(enable: Boolean): Unit = ???
        override def disableAutoInboundFlowControl(): Unit = ???

        override def onNext(value: VerdictsResponse): Unit = {
          super.onNext(value)
          if (counter.decrementAndGet() == 0) {
            isCancelled_ = true
            promise.trySuccess(values)
          }
        }

        override def onError(t: Throwable): Unit = {
          super.onError(t)
          promise.tryFailure(t)
        }
      }
      scanService.verdicts(
        v30.VerdictsRequest(
          mostRecentlyReceivedRecordTime = Some(fromRequestExclusive.toProtoTimestamp)
        ),
        observer,
      )
      (observer, promise.future)
    }

    def observeWatermark(watermark: CantonTimestamp): Unit = {
      logger.info(s"observing $watermark")
      latestKnownWatermark = watermark
      timeAwaiter.notifyAwaitedFutures(watermark)
    }
  }

  "GrpcMediatorScanServiceTest" should {

    "serve verdicts from the beginning" in {
      val f = new Fixture()
      import f.*

      val responses =
        (1 to 10).map(i =>
          mkResponse(Epoch.plusSeconds(i.toLong), Epoch.plusSeconds(Random.nextLong(10) + i))
        )

      for {
        _ <- MonadUtil.sequentialTraverse_(responses)(finalizedResponseStore.store(_))
        (observer, future) = requestVerdictsFrom(
          fromRequestExclusive = CantonTimestamp.MinValue,
          numExpected = responses.size,
        )
        _ = observer.values should have size 0

        // notify the GrpcMediatorScanService of a new watermark time, so it starts emitting verdicts
        _ = observeWatermark(CantonTimestamp.Epoch.plusSeconds(50))
        // wait for the future to complete after numExpected verdicts have been emitted
        _ <- FutureUnlessShutdown.outcomeF(future)
      } yield {
        val timestamps1 = observer.values
          .flatMap(_.verdict)
          .map(r => r.recordTime.value)
        timestamps1 should have size responses.size.toLong
        timestamps1 shouldBe sorted

        timestamps1 should contain theSameElementsInOrderAs responses
          .map(r => r.requestId.unwrap.toProtoTimestamp)
          .sorted
      }
    }.failOnShutdown("Unexpected shutdown.")

    "resume the subscription" in {
      val f = new Fixture()
      import f.*

      val responses =
        (1 to 10)
          .map(i =>
            mkResponse(Epoch.plusSeconds(i.toLong), Epoch.plusSeconds(Random.nextLong(10) + i))
          )
          // sort the responses by version aka finalization time to simulate storing
          // the responses in finalization time order
          .sortBy(_.version)

      for {
        _ <- MonadUtil.sequentialTraverse_(responses)(finalizedResponseStore.store(_))
        (observer1, future1) = requestVerdictsFrom(
          fromRequestExclusive = CantonTimestamp.MinValue,
          numExpected = responses.size / 2,
        )
        _ = observer1.values should have size 0

        // notify the GrpcMediatorScanService of a new watermark, so it starts emitting verdicts
        _ = observeWatermark(CantonTimestamp.Epoch.plusSeconds(50))
        // wait for the future to complete after numExpected verdicts have been emitted
        _ <- FutureUnlessShutdown.outcomeF(future1)

        lastVerdict = observer1.values.last.verdict.value
        nextFromRequest = CantonTimestamp
          .fromProtoTimestamp(lastVerdict.recordTime.value)
          .value

        // resume subscription from the last observed verdict
        (observer2, future2) = requestVerdictsFrom(
          fromRequestExclusive = nextFromRequest,
          numExpected = responses.size - observer1.values.size,
        )
        _ <- FutureUnlessShutdown.outcomeF(future2)
      } yield {
        val timestamps1 = observer1.values
          .flatMap(_.verdict)
          .map(r => r.recordTime.value)
        timestamps1 should have size responses.size.toLong / 2
        timestamps1 shouldBe sorted

        val timestamps2 = observer2.values
          .flatMap(_.verdict)
          .map(r => r.recordTime.value)
        timestamps2 should have size responses.size.toLong - timestamps1.size.toLong
        timestamps2 shouldBe sorted

        timestamps1 ++ timestamps2 shouldBe sorted
        timestamps1 ++ timestamps2 should contain theSameElementsInOrderAs responses
          .map(r => r.requestId.unwrap.toProtoTimestamp)
          .sorted
      }
    }.failOnShutdown("Unexpected shutdown.")

    "resume the subscription when observing new watermarks" in {
      val f = new Fixture()
      import f.*

      val responses =
        (1 to 10).map(i => mkResponse(Epoch.plusSeconds(i.toLong), Epoch.plusSeconds(10L + i)))

      for {
        _ <- MonadUtil.sequentialTraverse_(responses)(finalizedResponseStore.store(_))
        (observer, future) = requestVerdictsFrom(
          fromRequestExclusive = CantonTimestamp.MinValue,
          numExpected = responses.size,
        )
        _ = observer.values should have size 0

        // notify the GrpcMediatorScanService of a new watermark, so it starts emitting verdicts
        _ = observeWatermark(CantonTimestamp.Epoch.plusSeconds(4))

        _ <- eventuallyAsync() {
          observer.values should have size 4L
        }

        // notify the GrpcMediatorScanService of a new watermark, so it continues emitting verdicts
        _ = observeWatermark(CantonTimestamp.Epoch.plusSeconds(4 + 3))
        _ <- eventuallyAsync() {
          observer.values should have size 7L
        }

        // notify the GrpcMediatorScanService of a new watermark, so it continues emitting verdicts
        _ = observeWatermark(CantonTimestamp.Epoch.plusSeconds(4 + 3 + 3))
        _ <- eventuallyAsync() {
          observer.values should have size 10L
        }

        // wait for the future to complete after numExpected verdicts have been emitted
        _ <- FutureUnlessShutdown.outcomeF(future)
      } yield {
        val receivedTimestamps = observer.values
          .flatMap(_.verdict)
          .map(r => r.recordTime.value)

        receivedTimestamps should contain theSameElementsInOrderAs responses
          .map(r => r.requestId.unwrap.toProtoTimestamp)
        receivedTimestamps shouldBe sorted

      }
    }.failOnShutdown("Unexpected shutdown.")

    // this test case makes sure that the verdicts are properly resumed from the
    // starting timestamps, if the verdicts have the same result timestamp
    "deliver verdicts at the same result timestamp in proper order" in {
      val f = new Fixture()
      import f.*

      val responses =
        (1 to 10).map(i => mkResponse(Epoch.plusSeconds(i.toLong), Epoch.plusSeconds(20)))

      for {
        _ <- MonadUtil.sequentialTraverse_(responses)(finalizedResponseStore.store(_))
        (_, future1) = requestVerdictsFrom(
          fromRequestExclusive = CantonTimestamp.MinValue,
          numExpected = 5,
        )
        // notify the GrpcMediatorScanService of a new watermark, so it starts emitting verdicts
        _ = observeWatermark(Epoch.plusSeconds(30))
        // wait for the future to complete after numExpected verdicts have been emitted
        verdicts1 <- FutureUnlessShutdown.outcomeF(future1)

        lastVerdict = verdicts1.last.verdict.value
        nextFromRequest = CantonTimestamp
          .fromProtoTimestamp(lastVerdict.recordTime.value)
          .value

        // resume subscription from the last observed verdict
        (_, future2) = requestVerdictsFrom(
          fromRequestExclusive = nextFromRequest,
          numExpected = 5,
        )
        verdicts2 <- FutureUnlessShutdown.outcomeF(future2)
      } yield {
        val timestamps1 = verdicts1
          .flatMap(_.verdict)
          .map(r => r.recordTime.value)
        timestamps1 should have size 5
        timestamps1 shouldBe sorted

        val timestamps2 = verdicts2
          .flatMap(_.verdict)
          .map(r => r.recordTime.value)
        timestamps2 should have size responses.size.toLong - timestamps1.size.toLong
        timestamps2 shouldBe sorted

        timestamps1 ++ timestamps2 shouldBe sorted
        timestamps1 ++ timestamps2 should contain theSameElementsInOrderAs responses
          .map(r => r.requestId.unwrap.toProtoTimestamp)
          .sorted
      }
    }.failOnShutdown("Unexpected shutdown.")

    "properly flatten tree like hierarchies" in {
      case class FlattenedNode(name: String, children: Seq[Int])

      // the node connections are organized such that pre-order traversal puts them
      // in alphabetical order
      val originalNodes =
        Map(
          "a" -> Seq("b", "d"),
          "b" -> Seq("c"),
          "c" -> Seq.empty,
          "d" -> Seq.empty,
          "e" -> Seq("f", "g"),
          "f" -> Seq.empty,
          "g" -> Seq("h"),
          "h" -> Seq.empty,
        )

      val (nodes, rootNodes) = GrpcMediatorScanService.flattenForrest(
        roots = Seq("a", "e"),
        getChildren = originalNodes.apply,
        (n, children) => FlattenedNode(n, children),
      )

      rootNodes should contain theSameElementsInOrderAs Vector(0, 4)
      rootNodes.map(nodes(_).name) should contain theSameElementsInOrderAs Vector("a", "e")

      locally {
        // now let's traverse the nodes according to the relationships returned by flattenForrest
        def go(indices: Seq[Int]): Seq[String] =
          indices.flatMap { i =>
            val FlattenedNode(name, children) = nodes(i)
            name +: go(children)
          }
        val flattened = go(rootNodes)
        flattened shouldBe sorted
      }

      locally {
        // now let's reconstruct the tree and compare it to the original tree
        def go(indices: Seq[Int]): Seq[(String, Seq[String])] =
          indices.flatMap { i =>
            val FlattenedNode(name, children) = nodes(i)
            (name -> children.map(nodes(_)).map(_.name)) +: go(children)
          }
        val flattened = go(rootNodes).toMap
        flattened should contain theSameElementsAs originalNodes
      }
    }
  }
}
