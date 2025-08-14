// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.{EitherT, Nested}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}

package object client {

  /** Signature for callbacks provided to the send operation to take advantage of the SendTracker to
    * provide tracking of the eventual send result. Callback is ephemeral and will be lost if the
    * SequencerClient is recreated or the process exits.
    * @see
    *   [[SequencerClient.send]]
    */
  type SendCallback = UnlessShutdown[SendResult] => Unit

  type SendSyncResult[T] = EitherT[FutureUnlessShutdown, SendAsyncClientError, T]

  /** Nested future to encode result of send. The outer future contains the preparation of the
    * submission request while the inner one is about submission to the sequencer(s).
    */
  type SendAsyncResult = Nested[SendSyncResult[*], SendSyncResult[*], Unit]
}
