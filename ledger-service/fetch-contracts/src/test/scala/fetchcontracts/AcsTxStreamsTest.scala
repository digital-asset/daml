// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts

import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class AcsTxStreamsTest extends AsyncWordSpec with Matchers with AkkaBeforeAndAfterAll {
  import AcsTxStreamsTest._
  import AcsTxStreams.acsFollowingAndBoundary

  "acsFollowingAndBoundary" when {
    // "ACS is active" should {}

    "ACS is past liveBegin" should {
      "propagate cancellation of tx stream" in {
        val (_, _) = (liveBegin, txEnd)
        val (acs, futx, out, off) =
          probeCodensity(acsFollowingAndBoundary)(_ => akka.stream.scaladsl.Source.never).run()
        acs sendNext liveBegin
        out.cancel()
        acs.expectCancellation()
        off.expectSubscription()
        off.expectComplete()
        futx.map { tx =>
          tx.expectCancellation()
          succeed
        }
      }
    }
  }
}

object AcsTxStreamsTest {
  import akka.NotUsed
  import akka.actor.ActorSystem
  import akka.{stream => s}
  import s.scaladsl.{GraphDSL, RunnableGraph, Source}
  import s.{testkit => tk}
  import tk.TestPublisher.{Probe => InProbe}
  import tk.TestSubscriber.{Probe => OutProbe}
  import tk.scaladsl.{TestSource, TestSink}
  import com.daml.ledger.api.{v1 => lav1}
  import com.daml.logging.LoggingContextOf

  private val liveBegin = lav1.active_contracts_service.GetActiveContractsResponse(offset = "42")
  private val txEnd = lav1.transaction.Transaction(offset = "84")

  private implicit val `log ctx`: LoggingContextOf[Any] =
    LoggingContextOf.newLoggingContext(LoggingContextOf.label[Any])(identity)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private def probeCodensity[K, I0, I1, O0, O1](
      part: (K => Source[I1, NotUsed]) => s.Graph[s.FanOutShape2[I0, O0, O1], NotUsed]
  )(
      k: K => Source[I1, NotUsed]
  )(implicit
      as: ActorSystem
  ): RunnableGraph[(InProbe[I0], Future[InProbe[I1]], OutProbe[O0], OutProbe[O1])] = {
    val i1 = concurrent.Promise[InProbe[I1]]()
    probeAll(
      part(a =>
        k(a).mergeMat(TestSource.probe[I1]) { (probes, i1p) =>
          i1.success(i1p)
          probes
        }
      )
    )
      .mapMaterializedValue { case (i0, o0, o1) => (i0, i1.future, o0, o1) }
  }

  private def probeAll[I, O0, O1](
      part: s.Graph[s.FanOutShape2[I, O0, O1], NotUsed]
  )(implicit as: ActorSystem): RunnableGraph[(InProbe[I], OutProbe[O0], OutProbe[O1])] =
    RunnableGraph fromGraph GraphDSL.createGraph(
      TestSource.probe[I],
      TestSink.probe[O0],
      TestSink.probe[O1],
    )((_, _, _)) { implicit b => (i, o0, o1) =>
      import GraphDSL.Implicits._
      val here = b add part
      // format: off
      i  ~> here.in
      o0 <~ here.out0
      o1 <~ here.out1
      // format: on
      s.ClosedShape
    }
}
