// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.ErrorUtil
import io.grpc.stub.StreamObserver

/** Stream observer that will forward all received values, errors and completions to another observer, mapping
  * values to a different type.
  */
class ForwardingStreamObserver[A, B](
    targetObserver: StreamObserver[B],
    extract: A => IterableOnce[B],
)(implicit loggingContext: ErrorLoggingContext)
    extends StreamObserver[A] {

  override def onNext(value: A): Unit = {
    extract(value).iterator.foreach { t =>
      ErrorUtil.withThrowableLogging(targetObserver.onNext(t), valueOnThrowable = Some(()))
    }
  }

  override def onError(t: Throwable): Unit = targetObserver.onError(t)

  override def onCompleted(): Unit = targetObserver.onCompleted()
}
