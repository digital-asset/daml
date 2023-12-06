// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.checked
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.google.common.annotations.VisibleForTesting

import VerdictMessageId.{mkMessageId, verdictPrefix}

@Deprecated(since = "3.0, remove me once we can break compatibility with old proto versions")
private[mediator] final case class VerdictMessageId(requestId: RequestId) {
  val toMessageId: MessageId =
    mkMessageId(verdictPrefix)(requestId.unwrap.toLf.micros)
}

private[mediator] object VerdictMessageId {

  /** all message ids encoded by the mediator message id should start with this prefix */
  private val mediatorMessageIdPrefix = "mmid"
  val verdictPrefix = "verdict"
  val separator = ':'

  @VisibleForTesting
  def mkMessageId(prefix: String)(id: Long): MessageId =
    checked {
      // Total character count: 37
      // - mediatorMessageIdPrefix: 4
      // - prefix: 10
      // - id: 20 (19 digits and a sign)
      // - separators: 3
      MessageId.tryCreate(
        List(mediatorMessageIdPrefix, prefix, id.toString).mkString(separator.toString)
      )
    }
}
