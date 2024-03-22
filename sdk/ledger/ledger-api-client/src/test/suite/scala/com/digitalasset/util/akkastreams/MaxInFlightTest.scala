// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.util.akkastreams

import akka.stream.scaladsl.{Flow, Source}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.codahale.metrics.Counter
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Minute, Span}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class MaxInFlightTest
    extends AnyWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with ScalaFutures {

  "MaxInFlight" should {

    "not interfere with elements passing through" in {
      val elemCount = 1000L
      val bidi = MaxInFlight[Long, Long](1, new Counter, new Counter)

      val result = Source.repeat(1L).take(elemCount).via(bidi.join(Flow[Long])).runFold(0L)(_ + _)

      whenReady(result)(_ shouldEqual elemCount)
    }

    "actually keep the number of in-flight elements bounded" in {
      val elemCount = 1000L
      val maxElementsInFlight = 10
      val bidi = MaxInFlight[Long, Long](maxElementsInFlight, new Counter, new Counter)

      val flow = bidi.join(new DiesOnTooManyInFlights(maxElementsInFlight, 1.second))

      val result = Source.repeat(1L).take(elemCount).via(flow).runFold(0L)(_ + _)

      whenReady(result)(_ shouldEqual elemCount)
    }
  }

  override implicit def patienceConfig: PatienceConfig =
    super.patienceConfig.copy(timeout = Span(1L, Minute))

  class DiesOnTooManyInFlights(maxInFlight: Int, flushAfter: FiniteDuration)
      extends GraphStage[FlowShape[Long, Long]] {

    private val scheduledFlushTimerKey = "scheduledFlush"
    private val replaceHandlerTimerKey = "replaceHandler"

    val in = Inlet[Long]("in")
    val out = Outlet[Long]("out")

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) {
        var accumulator: List[Long] = Nil

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              if (accumulator.lengthCompare(maxInFlight) <= 0) {
                accumulator = grab(in) :: accumulator
              } else {
                sys.error("Too many elements in flight")
              }
              pull(in)
            }

            override def onUpstreamFinish(): Unit = ()
          },
        )

        setHandler(
          out,
          new OutHandler {
            // Initial handler is noop, we keep accumulating elements until the handler is replaced.
            override def onPull(): Unit = ()
          },
        )

        private def flush() = {
          accumulator match {
            case h :: t =>
              push(out, h)
              accumulator = t
            case _ =>
              scheduleOnce(scheduledFlushTimerKey, flushAfter)
          }
        }

        override def preStart(): Unit = {
          pull(in)
          scheduleOnce(replaceHandlerTimerKey, flushAfter)
          super.preStart()
        }

        override protected def onTimer(timerKey: Any): Unit = {
          timerKey match {
            case `replaceHandlerTimerKey` =>
              setHandler(
                out,
                new OutHandler {
                  override def onPull(): Unit = {
                    flush()
                    if (isClosed(in)) completeStage()
                  }
                },
              )
              if (isAvailable(out)) flush()
            case `scheduledFlushTimerKey` =>
              flush()
          }
        }
      }

    override def shape = FlowShape(in, out)
  }
}
