// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.lifecycle.UnlessShutdown

package object client {

  /** Signature for callbacks provided to the send operation to take advantage of the SendTracker to provide
    * tracking of the eventual send result. Callback is ephemeral and will be lost if the SequencerClient is recreated
    * or the process exits.
    * @see [[SequencerClient.sendAsync]]
    */
  type SendCallback = UnlessShutdown[SendResult] => Unit

}
