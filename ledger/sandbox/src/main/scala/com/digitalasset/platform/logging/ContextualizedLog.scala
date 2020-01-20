// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.logging

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

final class ContextualizedLog[T, U] private (
    logger: ContextualizedLogger,
    prefix: String,
    project: T => U,
    logDemand: Boolean = false)(implicit val logCtx: LoggingContext)
    extends GraphStage[FlowShape[T, T]] {

  override def toString = "Slf4JLog"

  val in: Inlet[T] = Inlet[T]("in")
  val out: Outlet[T] = Outlet[T]("out")

  override def shape: FlowShape[T, T] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler with InHandler {

      override def onPush(): Unit = {

        val elem = grab(in)
        logger.debug(s"[$prefix] Element: ${project(elem)}")
        push(out, elem)
      }

      override def onPull(): Unit = {
        if (logDemand) logger.debug(s"[$prefix] Demand")
        pull(in)
      }

      override def onUpstreamFailure(cause: Throwable): Unit = {
        logger.warn(s"[$prefix] Upstream failed", cause)

        super.onUpstreamFailure(cause)
      }

      override def onUpstreamFinish(): Unit = {
        logger.debug(s"[$prefix] Upstream finished.")

        super.onUpstreamFinish()
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        logger.debug(s"[$prefix] Downstream finished.")

        super.onDownstreamFinish(cause)
      }

      setHandlers(in, out, this)
    }
}

object ContextualizedLog {
  def apply[T](logger: ContextualizedLogger, prefix: String)(
      implicit logCtx: LoggingContext): ContextualizedLog[T, T] =
    new ContextualizedLog(logger, prefix, identity)
  def apply[T](logger: ContextualizedLogger, prefix: String, logDemand: Boolean)(
      implicit logCtx: LoggingContext): ContextualizedLog[T, T] =
    new ContextualizedLog(logger, prefix, identity, logDemand)
}
