// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

final case class AssertionErrorWithPreformattedMessage[T](
    preformattedMessage: String,
    message: String,
) extends AssertionError(message)
