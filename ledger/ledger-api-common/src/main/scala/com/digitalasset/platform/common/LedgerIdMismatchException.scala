// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.common

import com.daml.ledger.api.domain.LedgerId

class LedgerIdMismatchException(val existingLedgerId: LedgerId, val providedLedgerId: LedgerId)
    extends RuntimeException(
      s"""The provided ledger ID does not match the existing ID. Existing: "$existingLedgerId", Provided: "$providedLedgerId".""")
