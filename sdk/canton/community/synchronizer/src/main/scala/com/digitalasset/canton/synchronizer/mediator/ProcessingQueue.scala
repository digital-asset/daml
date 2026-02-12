// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.Nested
import cats.syntax.functor.*
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** A processing queue that allows scheduling work associated with a particular identifier.
  *
  * @tparam Ident
  *   The type of the identifiers
  */
trait ProcessingQueue[Ident] {
  def enqueueForProcessing[A](id: Ident)(
      action: => FutureUnlessShutdown[A]
  ): FutureUnlessShutdown[A]
}

/** A processing queue that immediately executes the action.
  * @tparam Ident
  *   The type of the identifiers
  */
class SynchronousProcessingQueue[Ident] extends ProcessingQueue[Ident] {
  override def enqueueForProcessing[A](id: Ident)(
      action: => FutureUnlessShutdown[A]
  ): FutureUnlessShutdown[A] = action
}

/** A processing queue that runs units of work for a particular identifier in a sequential manner,
  * but allows parallel processing of work for different identifiers. If a unit of work fails or
  * throws an exception, subsequent units of work for the same identifier are not executed.
  * @tparam Ident
  *   The type of the identifiers
  */
class ShardedSequentialProcessingQueue[Ident](implicit ec: ExecutionContext)
    extends ProcessingQueue[Ident] {
  @VisibleForTesting
  val processingQueuePerRequest = new TrieMap[Ident, FutureUnlessShutdown[Unit]]()

  override def enqueueForProcessing[A](
      id: Ident
  )(action: => FutureUnlessShutdown[A]): FutureUnlessShutdown[A] = {
    val processingPromise: PromiseUnlessShutdown[Unit] = PromiseUnlessShutdown.unsupervised()
    val processingFuture = processingPromise.futureUS

    val previousProcessingFuture: FutureUnlessShutdown[Unit] = processingQueuePerRequest
      .put(id, processingFuture)
      .getOrElse(FutureUnlessShutdown.unit)

    previousProcessingFuture.flatMap(_ => action).thereafter { result =>
      processingPromise.complete(Nested(result).void.value)
      // cleanup the processing queue
      processingQueuePerRequest
        .updateWith(id) {
          case Some(`processingFuture`) =>
            // if the "processing queue" still contains the same future that we put in, we can remove the entry from the map
            None
          case Some(other) =>
            // some other future was put into the map, retain it
            Some(other)
          case None =>
            // the entry was already removed, nothing to do
            None
        }
        .discard

    }
  }
}
