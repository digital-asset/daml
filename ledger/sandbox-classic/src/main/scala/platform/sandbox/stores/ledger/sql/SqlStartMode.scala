// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.sql

private[sandbox] sealed abstract class SqlStartMode extends Product with Serializable

private[sandbox] object SqlStartMode {
  /* We do not allow ResetAndStart to be set from options bubbled up to config to avoid mishaps */
  def fromString(value: String): Option[SqlStartMode] = {
    Vector(MigrateAndStart, ResetAndStart).find(_.toString == value)
  }

  /** Will continue using an initialised ledger, otherwise initialize a new one */
  final case object MigrateAndStart extends SqlStartMode

  /** Will always reset and initialize the ledger, even if it has data. */
  final case object ResetAndStart extends SqlStartMode

}
