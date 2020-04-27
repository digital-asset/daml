// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.sql

sealed abstract class SqlStartMode extends Product with Serializable

object SqlStartMode {

  /** Will continue using an initialised ledger, otherwise initialize a new one */
  final case object ContinueIfExists extends SqlStartMode

  /** Will always reset and initialize the ledger, even if it has data.  */
  final case object AlwaysReset extends SqlStartMode

}
