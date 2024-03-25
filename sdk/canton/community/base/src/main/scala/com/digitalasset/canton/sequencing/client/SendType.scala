// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

/** What type of message is being sent.
  * Used by the domain and surrounding infrastructure for prioritizing send requests.
  */
sealed trait SendType {
  private[client] val isRequest: Boolean
}

object SendType {

  /** An initial confirmation request. This is subject to throttling at the domain if resource constrained. */
  case object ConfirmationRequest extends SendType {
    override private[client] val isRequest = true
  }

  /** There's currently no requirement to distinguish other types of request */
  case object Other extends SendType {
    override private[client] val isRequest = false
  }
}
