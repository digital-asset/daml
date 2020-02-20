// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.common

import com.digitalasset.ledger.api.domain.LedgerId

class LedgerIdMismatchException(val existingLedgerId: LedgerId, val providedLedgerId: LedgerId)
    extends RuntimeException(
      s"""The provided ledger ID does not match the existing ID. Existing: "$existingLedgerId", Provided: "$providedLedgerId".""")
