// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick
import com.digitalasset.ledger.client.binding.encoding.{LfEncodable, SlickTypeEncoding}
import com.digitalasset.ledger.client.binding.{Contract, Template}

import slick.lifted.ProvenShape

/** A mixin building block of [[TemplateCompanionSlickTableQuery]];
  * 99% of use cases should extend that class instead.
  */
trait LfEncodableSlickTableQuery[Tpl <: Template[Tpl]]
    extends ContractSlickTableQuery[Contract[Tpl], Tpl] {
  import profile.api._

  protected implicit def tplLfEncodable: LfEncodable[Tpl]

  type TableType = LfDerivedProjection

  private[this] val (derivedTableName, projection): (
      String,
      LfDerivedProjection => ProvenShape[Contract[Tpl]]) = {
    val enc = SlickTypeEncoding(profile)
    enc.tableComponents(LfEncodable.encoding[Tpl](enc)) match {
      case nr: enc.NominalRecord[Tpl] =>
        (nr.tableName, nr.shapeWithId((_: LfDerivedProjection).id, Contract.apply[Tpl] _) {
          case Contract(id, tpl) => (id, tpl)
        })
      case vop => sys.error(s"template's argument is not a record: $vop")
    }
  }

  override def tableNameSuffix: String = derivedTableName

  override def all: TableQuery[TableType] = TableQuery(new LfDerivedProjection(_))

  class LfDerivedProjection(tag: Tag) extends ContractTable[Contract[Tpl]](tag, tableName) {
    override val * = projection(this)
  }
}
