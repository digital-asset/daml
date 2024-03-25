// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.interceptor

import io.grpc.ServerCall

import scala.collection.mutable
import scala.concurrent.blocking

/** This listener buffers all messages until `setNextListener` is called,
  * at which point all buffered messages are sent to the given listener.
  * From then on, all future messages are sent directly to the given listener.
  *
  * The target listener is usually created through `Contexts.interceptCall` or `ServerCallHandler.startCall`.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[auth] abstract class AsyncForwardingListener[ReqT] extends ServerCall.Listener[ReqT] {
  protected type Listener = ServerCall.Listener[ReqT]
  private[this] val lock = new Object
  private[this] val stash: mutable.ListBuffer[Listener => Unit] = new mutable.ListBuffer
  private[this] var nextListener: Option[Listener] = None

  private[this] def enqueueOrProcess(msg: Listener => Unit): Unit = blocking(lock.synchronized {
    val _ = nextListener.fold {
      val _ = stash.append(msg)
      ()
    }(msg)
  })

  protected def setNextListener(listener: Listener): Unit = blocking(lock.synchronized {
    nextListener = Some(listener)
    stash.foreach(msg => msg(listener))
  })

  // All methods that need to be forwarded
  override def onHalfClose(): Unit = enqueueOrProcess(i => i.onHalfClose())
  override def onCancel(): Unit = enqueueOrProcess(i => i.onCancel())
  override def onComplete(): Unit = enqueueOrProcess(i => i.onComplete())
  override def onReady(): Unit = enqueueOrProcess(i => i.onReady())
  override def onMessage(message: ReqT): Unit = enqueueOrProcess(i => i.onMessage(message))
}
