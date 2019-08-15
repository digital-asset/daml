// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

sealed abstract class LedgerBackend extends Product with Serializable

object LedgerBackend {
  case object SandboxInMemory extends LedgerBackend
  case object SandboxSql extends LedgerBackend
  case object RemoteApiProxy extends LedgerBackend
}
