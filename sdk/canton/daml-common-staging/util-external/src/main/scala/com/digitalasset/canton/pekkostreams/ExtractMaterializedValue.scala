// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pekkostreams

import com.digitalasset.canton.discard.Implicits.DiscardOps
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.stage.{
  GraphStageLogic,
  GraphStageWithMaterializedValue,
  InHandler,
  OutHandler,
}
import org.apache.pekko.stream.{Attributes, FlowShape, Inlet, Outlet}

import scala.concurrent.{Future, Promise}

/** Takes the input data, applies the provided transformation function, and completes its materialized value with it.
  */
class ExtractMaterializedValue[T, Mat](toMaterialized: T => Option[Mat])
    extends GraphStageWithMaterializedValue[FlowShape[T, T], Future[Mat]] {

  val inlet: Inlet[T] = Inlet[T]("in")
  val outlet: Outlet[T] = Outlet[T]("out")

  override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
  ): (GraphStageLogic, Future[Mat]) = {
    val promise = Promise[Mat]()

    val logic = new GraphStageLogic(shape) {

      setHandler(
        inlet,
        new InHandler {
          override def onPush(): Unit = {
            val input = grab(inlet)
            push(outlet, input)
            toMaterialized(input).foreach { materialized =>
              promise.trySuccess(materialized).discard
              setSimplerHandler()
            }
          }

          private def setSimplerHandler(): Unit = {
            setHandler(
              inlet,
              new InHandler {
                override def onPush(): Unit =
                  push(outlet, grab(inlet))
              },
            )
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.tryFailure(ex).discard
            super.onUpstreamFailure(ex)
          }

          override def onUpstreamFinish(): Unit = {
            promise
              .tryFailure(
                new RuntimeException("Upstream completed before matching element arrived.")
              )
              .discard
            super.onUpstreamFinish()
          }
        },
      )

      setHandler(
        outlet,
        new OutHandler {
          override def onPull(): Unit = pull(inlet)

          override def onDownstreamFinish(cause: Throwable): Unit = {
            promise
              .tryFailure(
                new RuntimeException("Downstream completed before matching element arrived.")
              )
              .discard
            super.onDownstreamFinish(cause)
          }
        },
      )

    }

    logic -> promise.future
  }

  override def shape: FlowShape[T, T] = FlowShape(inlet, outlet)
}

object ExtractMaterializedValue {
  def apply[T, Mat](toOutputOrMaterialized: T => Option[Mat]): Flow[T, T, Future[Mat]] =
    Flow.fromGraph(new ExtractMaterializedValue[T, Mat](toOutputOrMaterialized))
}
