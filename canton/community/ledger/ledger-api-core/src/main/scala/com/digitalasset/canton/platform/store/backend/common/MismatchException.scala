// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import com.digitalasset.canton.ledger.api.domain

abstract class MismatchException[A](
    description: String,
    val existing: A,
    val provided: A,
) extends RuntimeException(
      s"""The provided $description does not match the existing one. Existing: "$existing", Provided: "$provided"."""
    )

object MismatchException {

  class ParticipantId(
      override val existing: domain.ParticipantId,
      override val provided: domain.ParticipantId,
  ) extends MismatchException[domain.ParticipantId]("participant id", existing, provided)

}
