// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1.impl.reference

import akka.NotUsed
import akka.stream.javadsl.Flow
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.math.Ordering.Implicits._

/** Maintain the invariant on the stream that the offsets of elements are larger than a
  * lower bound, and smaller or equal an upper bound, with the ordering between elements
  * given by 'ord'. If the invariant is violated the stream is terminated.
  */
class BoundedOffsetValidation[T, O](
    getOffset: T => O,
    exclusiveLowerBound: Option[O],
    inclusiveUpperBound: Option[O])(implicit ord: Ordering[O])
    extends GraphStage[FlowShape[T, T]] {

  val input: Inlet[T] = Inlet[T](s"${this.getClass.getSimpleName}.in")
  val output: Outlet[T] = Outlet[T](s"${this.getClass.getSimpleName}.out")

  override def shape: FlowShape[T, T] = FlowShape(input, output)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      override def onPush(): Unit = {
        val element = grab(input)
        val currentOffset = getOffset(element)
        exclusiveLowerBound.foreach(elb => {
          if (currentOffset <= elb) {
            throw new RuntimeException(
              s"invariantExclusiveLowerBound: violated: $currentOffset <= $elb")
          }
        })
        inclusiveUpperBound.foreach(iub => {
          if (currentOffset > iub) {
            throw new RuntimeException(
              s"invariantInclusiveUpperBound: violated: $currentOffset > $iub")
          }
        })
        push(output, element)
      }

      override def onPull(): Unit = pull(input)

      setHandlers(input, output, this)
    }
}

object BoundedOffsetValidation {
  def apply[T, O](
      getOffset: T => O,
      exclusiveLowerBound: Option[O],
      inclusiveUpperBound: Option[O])(implicit ord: Ordering[O]): Flow[T, T, NotUsed] = {
    Flow.fromGraph(
      new BoundedOffsetValidation[T, O](getOffset, exclusiveLowerBound, inclusiveUpperBound))
  }
}
