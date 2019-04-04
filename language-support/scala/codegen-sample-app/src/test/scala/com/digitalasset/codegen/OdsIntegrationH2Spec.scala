// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import scala.language.existentials
import scala.collection.breakOut
import com.digitalasset.ledger.client.binding.{
  Contract,
  Template,
  TemplateCompanion,
  Primitive => P
}
import com.digitalasset.ledger.client.binding.encoding.{
  GenEncoding,
  LfEncodable,
  MultiTableTests,
  ShrinkEncoding,
  SlickTypeEncoding,
  EqualityEncoding,
  ShowEncoding
}
import SlickTypeEncoding.SupportedProfile
import com.digitalasset.ledger.api.refinements.ApiTypes
import com.digitalasset.ods.slick.LfEncodableSlickTableQuery
import com.digitalasset.slick.H2Db
import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalatest.time.{Millis, Seconds, Span}
import scalaz.{Equal, Show, Cord}

class OdsIntegrationH2Spec extends MultiTableTests {
  val con = new H2Db()
  val sampleSize = 10

  implicit override lazy val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(250, Millis))

  private def preserveUniqueKeys[A](cts: List[Contract[A]]): List[Contract[A]] =
    cts.groupBy(_.contractId).values.collect { case hd +: _ => hd }(breakOut)

  private def prepareTests(): Option[con.profile.SchemaDescription] = {
    import con.profile.api._, com.digitalasset.sample.{MyMain => Main}
    val encoder = SlickTypeEncoding(con.profile)
    val (arbCt, shrCt, eqCt, showCt, tableName, tq) =
      deriveContractTest(encoder)(Main.CallablePayout)
    templateTableTest(con.profile)(con.db, 10, preserveUniqueKeys[Main.CallablePayout])(
      tableName,
      tq)(arbCt, shrCt, eqCt, showCt)
    Some(tq.schema)
  }

  private val schemata = prepareTests()

  override protected def beforeAll(): Unit = beforeAllFromSchemata(con.profile)(con.db)(schemata)

  override protected def afterAll(): Unit = {
    afterAllFromSchemata(con.profile)(con.db)(schemata)
    con.close()
  }

  private def deriveContractTest[Profile <: SupportedProfile, Tpl <: Template[Tpl]](
      encoder: SlickTypeEncoding[Profile])(implicit TC: TemplateCompanion[Tpl]): (
      Arbitrary[Contract[Tpl]],
      Shrink[Contract[Tpl]],
      Equal[Contract[Tpl]],
      Show[Contract[Tpl]],
      String,
      encoder.profile.api.TableQuery[_ <: encoder.profile.Table[Contract[Tpl]]]) = {
    import encoder.profile, profile.api._
    type NameTable[A] = (String, TableQuery[_ <: profile.Table[Contract[A]]])
    import TC.`the template LfEncodable`
    val (tn, tq) = deriveTableQuery[Profile, Tpl](encoder)
    val genCt: Gen[P.ContractId[Tpl]] =
      P.ContractId.subst(ApiTypes.ContractId.subst(Gen.identifier))
    implicit val shrCt: Shrink[P.ContractId[Tpl]] =
      P.ContractId.subst(ApiTypes.ContractId.subst(implicitly[Shrink[String]]))
    val genTpl: Gen[Tpl] = LfEncodable.encoding[Tpl](GenEncoding)
    implicit val shrTpl: Shrink[Tpl] = LfEncodable.encoding[Tpl](ShrinkEncoding)
    val mkContract = (Contract.apply[Tpl] _).tupled
    implicit val equalTpl: Equal[Tpl] = Equal.equal(LfEncodable.encoding[Tpl](EqualityEncoding))
    implicit val showTpl: Show[Tpl] = LfEncodable.encoding[Tpl](ShowEncoding)
    val equalContract: Equal[Contract[Tpl]] = (a1: Contract[Tpl], a2: Contract[Tpl]) => {
      EqualityEncoding.primitive.valueContractId.apply(a1.contractId, a2.contractId) && equalTpl
        .equal(a1.value, a2.value)
    }
    val showContract: Show[Contract[Tpl]] = Show.show { c: Contract[Tpl] =>
      val showContractId: Show[P.ContractId[Tpl]] = ShowEncoding.primitive.valueContractId
      Cord("contractId = ", showContractId.show(c.contractId), ", value = ", showTpl.show(c.value))
    }
    (Arbitrary(Gen.zip(genCt, genTpl) map mkContract), Shrink.xmap(mkContract, {
      case Contract(id, tpl) => (id, tpl)
    }), equalContract, showContract, tn, tq)
  }

  private def deriveTableQuery[Profile <: SupportedProfile, Tpl <: Template[Tpl]: LfEncodable](
      encoder: SlickTypeEncoding[Profile])
    : (String, encoder.profile.api.TableQuery[_ <: encoder.profile.Table[Contract[Tpl]]]) = {
    object stq extends LfEncodableSlickTableQuery[Tpl] {
      override lazy val profile: encoder.profile.type = encoder.profile
      override lazy val tplLfEncodable = implicitly[LfEncodable[Tpl]]
      override lazy val odsId = "helloods"
    }
    (stq.tableName, stq.all)
  }
}
