// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.common

import com.daml.ledger.api.domain

abstract class MismatchException[A](
    description: String,
    val existing: A,
    val provided: A,
) extends RuntimeException(
      s"""The provided $description does not match the existing one. Existing: "$existing", Provided: "$provided".""")

object MismatchException {

  class LedgerId(existing: domain.LedgerId, provided: domain.LedgerId)
      extends MismatchException[domain.LedgerId]("ledger id", existing, provided)

  class ParticipantId(existing: domain.ParticipantId, provided: domain.ParticipantId)
      extends MismatchException[domain.ParticipantId]("participant id", existing, provided)

}
