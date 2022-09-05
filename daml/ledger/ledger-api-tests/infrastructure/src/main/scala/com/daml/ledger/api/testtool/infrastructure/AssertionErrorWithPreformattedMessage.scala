// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

final case class AssertionErrorWithPreformattedMessage[T](
    preformattedMessage: String,
    message: String,
) extends AssertionError(message)
