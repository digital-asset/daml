// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

sealed abstract class LedgerBackend extends Product with Serializable

object LedgerBackend {
  case object SandboxInMemory extends LedgerBackend
  case object SandboxSql extends LedgerBackend
}
