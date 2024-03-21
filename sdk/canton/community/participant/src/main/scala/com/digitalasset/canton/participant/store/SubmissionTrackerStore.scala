// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.store.PrunableByTime
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait SubmissionTrackerStore extends PrunableByTime with AutoCloseable {

  /** Register a fresh request in the store.
    * @return a `Future` that resolves to `true` if the request is indeed fresh and was added to the store,
    *         and `false` if it is a replay, i.e. another request already existed in the store for the given transactionId.
    */
  def registerFreshRequest(
      rootHash: RootHash,
      requestId: RequestId,
      maxSequencingTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean]

  /** Return the number of entries currently in the store. */
  def size(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int]

  /** Delete all entries whose sequencing time is at least `inclusive`. */
  def deleteSince(including: CantonTimestamp)(implicit traceContext: TraceContext): Future[Unit]
}
