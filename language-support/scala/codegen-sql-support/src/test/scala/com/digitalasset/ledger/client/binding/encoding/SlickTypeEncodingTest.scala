// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import com.digitalasset.ledger.client.binding.EncodingTestUtil.someContractId
import com.digitalasset.ledger.client.binding.encoding.{LfTypeEncodingSpec => t}
import com.digitalasset.ledger.client.binding.{ValueRef, Primitive => P}
import com.digitalasset.slick.H2Db
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import slick.jdbc.H2Profile.api._
import slick.lifted.{TableQuery, Tag}
import SlickTypeEncoding.{SupportedProfile, ViewColumns}

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SlickTypeEncodingTest
    extends AsyncWordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  val h2db = new H2Db()
  val db = h2db.db
  val profile = h2db.profile

  val enc = SlickTypeEncoding(profile)
  val encFields = t.CallablePayout.fieldEncoding(enc)
  val encOut = t.CallablePayout.encoding(enc)(encFields)
  type CPTableType = profile.Table[t.CallablePayout] with ViewColumns[t.CallablePayout.view]
  val callablePayoutTableFun: Tag => CPTableType = enc.tableWithColumns(encOut, encFields)._2
  val callablePayoutQuery = TableQuery[CPTableType](callablePayoutTableFun)

  val alice = P.Party("Alice")

  implicit override lazy val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(250, Millis))

  private def createTable[A <: ValueRef](profile: SupportedProfile)(
      table: TableQuery[_ <: profile.Table[A]]): Future[Unit] = {
    val create = DBIO.seq(table.schema.create)
    db.run(create)
  }

  private def dropTable[A <: ValueRef](profile: SupportedProfile)(
      table: TableQuery[_ <: profile.Table[A]]): Future[Unit] = {
    val drop = DBIO.seq(table.schema.drop)
    db.run(drop)
  }

  override protected def beforeAll(): Unit = {
    whenReady(createTable(profile)(callablePayoutQuery)) { _ =>
      ()
    }
  }

  override protected def afterAll(): Unit = {
    whenReady(dropTable(profile)(callablePayoutQuery)) { _ =>
      ()
    }
    h2db.close()
  }

  "should produce predictable SQL DDL statements" in {
    callablePayoutQuery.schema.createStatements.toList shouldBe List(
      """create table "MyMain.CallablePayout" ("receiver" VARCHAR NOT NULL,"subr.num" BIGINT NOT NULL,"subr.a" BIGINT NOT NULL,"lst" VARCHAR NOT NULL,"variant" VARCHAR NOT NULL)""")
  }

  "should query empty table" in {
    for {
      xs <- db.run(callablePayoutQuery.result): Future[Seq[t.CallablePayout]]
    } yield {
      xs shouldBe Seq()
    }
  }

  val contract1 =
    t.CallablePayout(
      alice,
      t.TrialSubRec(10, 100),
      List(1, 2, 3),
      t.TrialEmptyRec(),
      t.TrialVariant.TLeft("test"))

  val contract2 = t.CallablePayout(
    alice,
    t.TrialSubRec(11, 111),
    List(10, 20, 30),
    t.TrialEmptyRec(),
    t.TrialVariant.TRight(someContractId, someContractId)
  )

  "should insert two contracts, query them back and delete" in {
    val insertContracts = callablePayoutQuery ++= Seq(contract1, contract2)

    for {
      c1 <- db.run(insertContracts)
      r1 <- db.run(callablePayoutQuery.result): Future[Seq[t.CallablePayout]]
      c2 <- db.run(callablePayoutQuery.delete)
      r2 <- db.run(callablePayoutQuery.result): Future[Seq[t.CallablePayout]]
    } yield {
      c1 shouldBe Some(2)
      r1.toSet shouldBe Set(contract1, contract2)
      c2 shouldBe 2
      r2 shouldBe Seq()
    }
  }

  "should insert two contracts, filter them and delete" in {
    import profile.api._
    val insertContracts = callablePayoutQuery ++= Seq(contract1, contract2)
    // shadowing scalatest to suppress its implicits
    def convertToEqualizer(t: Any) = ()
    def convertToCheckingEqualizer(t: Any) = ()
    implicit val partyColumn: profile.BaseColumnType[P.Party] =
      P.Party.subst(profile.api.stringColumnType)

    for {
      c1 <- db.run(insertContracts)
      r1 <- db.run(callablePayoutQuery.filter(t => t.c.receiver === t.c.receiver).result): Future[
        Seq[t.CallablePayout]]
      r2 <- db.run(callablePayoutQuery.filter(t => t.c.receiver =!= t.c.receiver).result): Future[
        Seq[t.CallablePayout]]
      c2 <- db.run(callablePayoutQuery.delete)
      r3 <- db.run(callablePayoutQuery.result): Future[Seq[t.CallablePayout]]
    } yield {
      c1 shouldBe Some(2)
      r1.toSet shouldBe Set(contract1, contract2)
      r2.toSet shouldBe empty
      c2 shouldBe 2
      r3 shouldBe Seq()
    }
  }

  "any contract" when {
    "associated with an ID" should {
      "produce a structural contract table" in {
        import com.digitalasset.ledger.api.refinements.ApiTypes
        import com.digitalasset.ledger.client.binding.{Contract, Template}
        implicit def bctCid[Tpl]: profile.BaseColumnType[P.ContractId[Tpl]] =
          P.ContractId.subst(ApiTypes.ContractId.subst(implicitly[profile.BaseColumnType[String]]))
        def contractTable[Profile <: SupportedProfile, R, Tpl <: Template[Tpl]: LfEncodable](
            enc: SlickTypeEncoding[Profile])(idColumn: enc.ColumnSource => Rep[P.ContractId[Tpl]])
          : Tag => enc.profile.Table[Contract[Tpl]] = {
          enc.tableWithId[Rep[P.ContractId[Tpl]], P.ContractId[Tpl], Tpl, Contract[Tpl]](
            idColumn,
            LfEncodable.encoding[Tpl](enc))(Contract(_, _)) {
            case Contract(id, tpl) => (id, tpl)
          }
        }
        contractTable(enc)(_.column[P.ContractId[t.CallablePayout]]("id"))
        true shouldBe true
      }
    }
  }
}
