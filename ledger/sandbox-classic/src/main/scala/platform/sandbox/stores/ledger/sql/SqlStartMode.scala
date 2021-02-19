// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.sql

private[sandbox] sealed abstract class SqlStartMode extends Product with Serializable

private[sandbox] object SqlStartMode {
  /* We do not allow ResetAndStart to be set from options bubbled up to config to avoid mishaps */
  def fromString(value: String): Option[SqlStartMode] = {
    Vector(MigrateOnly, MigrateAndStart, ValidateAndStart).find(_.toString == value)
  }

  /** Run Migration scripts but do not start the ledger api service */
  final case object MigrateOnly extends SqlStartMode

  /** Will continue using an initialised ledger, otherwise initialize a new one */
  final case object MigrateAndStart extends SqlStartMode

  /** Will always reset and initialize the ledger, even if it has data. */
  final case object ResetAndStart extends SqlStartMode

  /** Will continue using an initialised ledger and will not run any migration scripts */
  final case object ValidateAndStart extends SqlStartMode
}
