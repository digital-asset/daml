// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.ledger

import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.resources.ProgramResource.StartupException

class LedgerIdMismatchException(existingLedgerId: LedgerId, providedLedgerId: LedgerId)
    extends StartupException(
      s"""The provided ledger ID does not match the existing ID. Existing: "$existingLedgerId", Provided: "$providedLedgerId".""")
