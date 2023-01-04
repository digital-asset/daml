// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

/** This trait is useful to be mixed in if:
  * - equality semantics for the base type is not an issue
  * - class has non-comparable typed fields
  *
  * Practical usage: mixing in in DTO ADT top trait, which has case class implementors having Array fields: disables equality semantics, and prevents Wartremover warnings.
  */
trait NeverEqualsOverride extends Equals {
  override final def canEqual(o: Any) = false
  override final def equals(o: Any): Boolean = false
}
