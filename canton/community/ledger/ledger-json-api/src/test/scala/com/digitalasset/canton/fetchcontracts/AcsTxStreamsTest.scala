// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.fetchcontracts

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v1.transaction.Transaction
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.logging.TracedLogger
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class AcsTxStreamsTest extends AsyncWordSpec with BaseTest with PekkoBeforeAndAfterAll {
  import AcsTxStreamsTest.*

  "acsFollowingAndBoundary" when {
    "ACS is active" should {
      "cancel the ACS on output cancel" in {
        val (acs, futx, out, _) = probeAcsFollowingAndBoundary(logger)
        out.cancel()
        acs.expectCancellation()
        futx.isCompleted should ===(false)
      }
    }

    "ACS is past liveBegin" should {
      "not start tx until ACS is complete" in {
        val (acs, futx, _, _) = probeAcsFollowingAndBoundary(logger)
        acs.sendNext(liveBegin)
        futx.isCompleted should ===(false)
      }

      "propagate cancellation of tx stream" in {
        val (_, _) = (liveBegin, txEnd)
        val (acs, futx, out, off) = probeAcsFollowingAndBoundary(logger)
        acs.sendNext(liveBegin).sendComplete()
        off.expectSubscription()
        out.cancel()
        futx.map { tx =>
          tx.expectCancellation()
          succeed
        }
      }
    }
  }
}

object AcsTxStreamsTest {
  import org.apache.pekko.actor.ActorSystem
  import org.apache.pekko.{NotUsed, stream as aks}
  import aks.scaladsl.{GraphDSL, RunnableGraph, Source}
  import aks.testkit as tk
  import com.daml.ledger.api.v1 as lav1
  import com.daml.logging.LoggingContextOf
  import tk.TestPublisher.Probe as InProbe
  import tk.TestSubscriber.Probe as OutProbe
  import tk.scaladsl.{TestSink, TestSource}

  private val liveBegin = lav1.active_contracts_service.GetActiveContractsResponse(offset = "42")
  private val txEnd = lav1.transaction.Transaction(offset = "84")

  private implicit val `log ctx`: LoggingContextOf[Any] =
    LoggingContextOf.newLoggingContext(LoggingContextOf.label[Any])(identity)

  private def probeAcsFollowingAndBoundary(logger: TracedLogger)(implicit
      ec: concurrent.ExecutionContext,
      as: ActorSystem,
  ) =
    probeFOS2PlusContinuation(
      AcsTxStreams.acsFollowingAndBoundary(
        _: lav1.ledger_offset.LedgerOffset => Source[Transaction, NotUsed],
        logger,
      )
    ).run()

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private def probeFOS2PlusContinuation[K, I0, I1, O0, O1](
      part: (Any => Source[I1, NotUsed]) => aks.Graph[aks.FanOutShape2[I0, O0, O1], NotUsed]
  )(implicit
      as: ActorSystem
  ): RunnableGraph[(InProbe[I0], Future[InProbe[I1]], OutProbe[O0], OutProbe[O1])] = {
    val i1 = concurrent.Promise[InProbe[I1]]()
    // filling in i1 like this is terribly hacky but is well enough for a test
    probeAll(
      part(_ =>
        TestSource.probe[I1].mapMaterializedValue { i1p =>
          i1.success(i1p)
          NotUsed
        }
      )
    )
      .mapMaterializedValue { case (i0, o0, o1) => (i0, i1.future, o0, o1) }
  }

  private def probeAll[I, O0, O1](
      part: aks.Graph[aks.FanOutShape2[I, O0, O1], NotUsed]
  )(implicit as: ActorSystem): RunnableGraph[(InProbe[I], OutProbe[O0], OutProbe[O1])] =
    RunnableGraph fromGraph GraphDSL.createGraph(
      TestSource.probe[I],
      TestSink.probe[O0],
      TestSink.probe[O1],
    )((_, _, _)) { implicit b => (i, o0, o1) =>
      import GraphDSL.Implicits.*
      val here = b add part
      // format: off
      i  ~> here.in
      o0 <~ here.out0
      o1 <~ here.out1
      // format: on
      aks.ClosedShape
    }
}
