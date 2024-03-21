// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

object DropRepeated {
  def apply[T](): GraphStage[FlowShape[T, T]] = new DropRepeated
}

final class DropRepeated[T] extends GraphStage[FlowShape[T, T]] {
  private val in = Inlet[T]("input")
  private val out = Outlet[T]("DropRepeated output")

  override def shape: FlowShape[T, T] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var currentValue: Option[T] = None

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val element = grab(in)
            if (currentValue.contains(element)) {
              pull(in)
            } else {
              currentValue = Some(element)
              push(out, element)
            }
          }
        },
      )

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            pull(in)
          }
        },
      )
    }
}
