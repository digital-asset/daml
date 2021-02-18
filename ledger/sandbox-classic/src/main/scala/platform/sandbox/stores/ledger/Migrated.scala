// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

private[sandbox] sealed abstract class Migrated extends Product with Serializable

private[sandbox] object Migrated {

  /** Migration completed successfully * */
  final case object MigrationSuccessful extends Migrated

  final case object MigrationFailed extends Migrated

}
