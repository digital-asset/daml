// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.util.TryUtil.ForFailedOps
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try

abstract class ActiveStreamCounterInterceptor() extends ServerInterceptor {

  protected def established(methodName: String): Unit
  protected def finished(methodName: String): Unit

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    val isStream = !call.getMethodDescriptor.getType.serverSendsOneMessage()
    val fullMethodName = call.getMethodDescriptor.getFullMethodName
    val delegate = next.startCall(call, headers)
    if (isStream) {
      val listener = new OnCloseCallListener(
        delegate,
        runOnceOnTermination = () => finished(fullMethodName),
      )
      established(fullMethodName) // Only do after call above has returned
      listener
    } else {
      delegate
    }
  }

  private class OnCloseCallListener[RespT](
      delegate: ServerCall.Listener[RespT],
      runOnceOnTermination: () => Unit,
  ) extends SimpleForwardingServerCallListener[RespT](delegate) {

    private val logger = LoggerFactory.getLogger(getClass)
    private val onTerminationCalled = new AtomicBoolean()

    private def runOnClose(): Unit =
      if (onTerminationCalled.compareAndSet(false, true)) {
        Try(runOnceOnTermination()).forFailed(logger.warn(s"Exception calling onClose method", _))
      }

    override def onCancel(): Unit = {
      runOnClose()
      super.onCancel()
    }

    override def onComplete(): Unit = {
      runOnClose()
      super.onComplete()
    }
  }

}
