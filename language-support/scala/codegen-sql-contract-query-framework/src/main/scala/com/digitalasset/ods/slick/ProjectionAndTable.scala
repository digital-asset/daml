// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick

import com.digitalasset.ledger.client.binding.Template

/**
  * A pair composed by a projection from the parent contract type `C`
  * to the target type `T` and a table that can store the output of
  * the projection.
  */
abstract class ProjectionAndTable[C, T] {
  type Tpl <: Template[Tpl]

  def projection: PartialFunction[C, T]

  def table: ContractSlickTableQuery[T, Tpl]
}

object ProjectionAndTable {
  def apply[C, T, Tp <: Template[Tp]](p: PartialFunction[C, T], t: ContractSlickTableQuery[T, Tp]) =
    new ProjectionAndTable[C, T] {
      type Tpl = Tp
      override def projection = p
      override def table = t
    }
}
