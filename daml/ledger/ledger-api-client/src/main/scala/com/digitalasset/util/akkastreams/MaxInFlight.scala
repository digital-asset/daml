// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.util.akkastreams

import akka.NotUsed
import akka.stream.scaladsl.BidiFlow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import com.codahale.metrics.Counter
import org.slf4j.LoggerFactory

/** Enforces that at most [[maxInFlight]] items traverse the flow underneath this one.
  * Requires the flow underneath to always produce 1 output element for 1 input element in order to work correctly.
  * With respect to completion, failure and cancellation, the input and output stream behave like normal `Flow`s,
  * except that if the output stream is failed, cancelled or completed, the input stream is completed.
  */
// TODO(mthvedt): This should have unit tests.
class MaxInFlight[I, O](maxInFlight: Int, capacityCounter: Counter, lengthCounter: Counter)
    extends GraphStage[BidiShape[I, I, O, O]] {

  capacityCounter.inc(maxInFlight.toLong)

  private val logger = LoggerFactory.getLogger(MaxInFlight.getClass.getName)

  val in1: Inlet[I] = Inlet[I]("in1")
  val out1: Outlet[I] = Outlet[I]("out1")
  val in2: Inlet[O] = Inlet[O]("in2")
  val out2: Outlet[O] = Outlet[O]("out2")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private var freeCapacity = maxInFlight

      private var admitting = false

      // Inbound path to layer below

      setHandler(
        in1,
        new InHandler {

          // Leaving this with the default implementation would result in dropping all in-flight items as the stage would complete.
          override def onUpstreamFinish(): Unit = complete(out1)
          override def onUpstreamFailure(ex: Throwable): Unit = fail(out1, ex)

          // NOOP. We want the pulling of out1 to install new handlers when there's demand.
          override def onPush(): Unit = ()
        },
      )

      setHandler(
        out1,
        new OutHandler {
          override def onPull(): Unit = {
            if (freeCapacity > 0) {
              admitNewElement()
            } else {
              logger.trace("No free capacity left. Backpressuring...")
            }
          }
        },
      )

      private def admitNewElement(): Unit = {
        admitting = true
        read(in1)(
          { elem =>
            logger.trace("Received input.")
            push(out1, elem)
            freeCapacity -= 1
            lengthCounter.inc()
            admitting = false
          },
          () => complete(out1),
        )
      }

      // Outbound path to layer above.

      setHandler(
        in2,
        new InHandler {
          override def onPush(): Unit = {
            val elemToEmit = grab(in2)
            logger.trace("Emitting output")
            push(out2, elemToEmit)
            freeCapacity += 1
            lengthCounter.dec()

            checkMaxInFlight(elemToEmit)

            if (isAvailable(out1) && !admitting) {
              // Output has been pulled, but stage is not working on getting new element.
              // This indicates the stage is backpressuring. We get a new element to restart the pump.
              logger.trace("Backpressure lifted.")
              admitNewElement()
            }
          }

          override def onUpstreamFinish(): Unit = {
            capacityCounter.dec(maxInFlight.toLong)
            super.onUpstreamFinish()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            capacityCounter.dec(maxInFlight.toLong)
            fail(out2, ex)
            completeStage()
          }

          private def checkMaxInFlight(elemToEmit: O): Unit = {
            require(
              freeCapacity <= maxInFlight,
              s"Free capacity has risen above maxInFlight value of $maxInFlight after emitting element $elemToEmit. " +
                s"This indicates that the layer below is emitting multiple elements per input. " +
                s"Such Flows are incompatible with the MaxInFlight stage.",
            )
          }
        },
      )

      setHandler(
        out2,
        new OutHandler {
          override def onPull(): Unit = pull(in2)
        },
      )

    }

  override def shape: BidiShape[I, I, O, O] = BidiShape(in1, out1, in2, out2)
}

object MaxInFlight {

  def apply[I, O](
      maxInFlight: Int,
      capacityCounter: Counter,
      lengthCounter: Counter,
  ): BidiFlow[I, I, O, O, NotUsed] =
    BidiFlow.fromGraph(new MaxInFlight[I, O](maxInFlight, capacityCounter, lengthCounter))
}
