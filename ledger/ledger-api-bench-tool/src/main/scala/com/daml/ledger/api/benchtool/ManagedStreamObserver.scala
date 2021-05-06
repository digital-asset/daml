// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import io.grpc.stub.{ClientCallStreamObserver, ClientResponseObserver}

abstract class ManagedStreamObserver[T] extends ClientResponseObserver[AnyRef, T] {
  private var clientCallStreamObserver: Option[ClientCallStreamObserver[AnyRef]] = None

  override def beforeStart(requestStream: ClientCallStreamObserver[AnyRef]): Unit =
    clientCallStreamObserver = Some(requestStream)

  def shutdown(): Unit =
    clientCallStreamObserver.foreach(_.cancel("Shutdown", new Cancel))

  private class Cancel extends Throwable {
    override def fillInStackTrace(): Throwable = {
      // ignore the stack trace
      this
    }
  }
}
