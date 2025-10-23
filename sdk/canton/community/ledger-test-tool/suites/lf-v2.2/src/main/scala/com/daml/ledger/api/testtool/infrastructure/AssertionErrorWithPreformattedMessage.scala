// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

final case class AssertionErrorWithPreformattedMessage[T](
    preformattedMessage: String,
    message: String,
) extends AssertionError(message)
