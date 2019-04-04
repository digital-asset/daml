// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick

import com.digitalasset.ledger.client.binding.encoding.{LfEncodable, SlickTypeEncoding}
import com.digitalasset.ledger.client.binding.{Contract, Template}

import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape

/** [[ContractSlickTableQuery]] that generates the [[ContractSlickTableQuery#TableType]]
  * and the associated `ContractTable` subclass "for free".
  *
  * {{{
  *   class IOUSlickTable(val profile: JdbcProfile, val odsId: String)
  *     extends TemplateCompanionSlickTableQuery[IOU]
  * }}}
  *
  * @tparam Tpl A Scala-codegen-generated template class.
  */
abstract class TemplateCompanionSlickTableQuery[Tpl <: Template[Tpl],
Companion <: LfEncodable.ViaFields[Tpl]](implicit val companion: Companion)
    extends ContractSlickTableQuery[Contract[Tpl], Tpl] {
  import profile.api._

  type TableType = LfDerivedProjection

  private[this] type EdslColumn[A] = Table[_] => Rep[A]
  private[this] val (derivedTableName, edslCols, projection): (
      String,
      companion.view[EdslColumn],
      LfDerivedProjection => ProvenShape[Contract[Tpl]]) = {
    val enc = SlickTypeEncoding(profile)
    val cfields = companion.fieldEncoding(enc)
    enc.tableComponents(companion.encoding(enc)(cfields)) match {
      case nr: enc.NominalRecord[Tpl] =>
        (
          nr.tableName,
          cfields.hoist[enc.EdslColumn](enc.edslColumn),
          nr.shapeWithId((_: LfDerivedProjection).id, Contract.apply[Tpl] _) {
            case Contract(id, tpl) => (id, tpl)
          })
      case vop => sys.error(s"template's argument is not a record: $vop")
    }
  }

  override def tableNameSuffix: String = derivedTableName

  override def all: TableQuery[TableType] = TableQuery(new LfDerivedProjection(_))

  class LfDerivedProjection(tag: Tag) extends ContractTable[Contract[Tpl]](tag, tableName) {
    val c: companion.view[Rep] = edslCols.hoist(SlickTypeEncoding.funApp(this))
    override val * = projection(this)
  }
}

object TemplateCompanionSlickTableQuery {
  def apply[Tpl <: Template[Tpl], Companion <: LfEncodable.ViaFields[Tpl]](
      companion: Companion with LfEncodable.ViaFields[Tpl])(
      profile0: JdbcProfile,
      odsId0: String): TemplateCompanionSlickTableQuery[Tpl, Companion] =
    new TemplateCompanionSlickTableQuery[Tpl, Companion]()(companion) {
      override val profile = profile0
      override val odsId = odsId0
    }
}
