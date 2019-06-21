// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.query

import org.scalatest.{FlatSpec, Matchers}
import com.digitalasset.daml.lf.data.{Ref => DamlLfRef}
import com.digitalasset.navigator.model._
import scalaz.syntax.tag._
import com.digitalasset.navigator.query.SortDirection.{ASCENDING, DESCENDING}
import com.digitalasset.ledger.api.refinements.ApiTypes

class ContractSortSpec extends FlatSpec with Matchers {

  behavior of "ContractSort"

  val damlLfId0 = DamlLfIdentifier(
    DamlLfRef.PackageId.assertFromString("hash"),
    DamlLfQualifiedName(
      DamlLfDottedName.assertFromString("module"),
      DamlLfDottedName.assertFromString("T0")))
  val damlLfId1 = DamlLfIdentifier(
    DamlLfRef.PackageId.assertFromString("hash"),
    DamlLfQualifiedName(
      DamlLfDottedName.assertFromString("module"),
      DamlLfDottedName.assertFromString("T1")))

  val damlLfRecord0 = DamlLfDefDataType(
    DamlLfImmArraySeq(),
    DamlLfRecord(
      DamlLfImmArraySeq(
        DamlLfRef.Name.assertFromString("foo") ->
          DamlLfTypePrim(DamlLfPrimType.Text, DamlLfImmArraySeq())
      )))
  val damlLfRecord1 = DamlLfDefDataType(
    DamlLfImmArraySeq(),
    DamlLfRecord(
      DamlLfImmArraySeq(
        DamlLfRef.Name.assertFromString("int") ->
          DamlLfTypePrim(DamlLfPrimType.Int64, DamlLfImmArraySeq())
      )))

  val damlLfDefDataTypes: Map[DamlLfIdentifier, DamlLfDefDataType] = Map(
    damlLfId0 -> damlLfRecord0,
    damlLfId1 -> damlLfRecord1
  )

  val template1 = Template(damlLfId0, List.empty)
  val template2 = Template(damlLfId1, List.empty)

  val contract1 = Contract(
    ApiTypes.ContractId("id1"),
    template1,
    ApiRecord(None, List(ApiRecordField("foo", ApiText("bar")))),
    None,
    Seq("Alice"),
    Seq("Bob", "Charlie"))
  val contract2 = Contract(
    ApiTypes.ContractId("id2"),
    template2,
    ApiRecord(None, List(ApiRecordField("int", ApiInt64(1)))),
    Some(""),
    Seq("Gloria"),
    Seq("Ernest", "Francis"))
  val contract3 = Contract(
    ApiTypes.ContractId("id3"),
    template1,
    ApiRecord(None, List(ApiRecordField("foo", ApiText("bar")))),
    Some("agreement"),
    Seq("Dana"),
    Seq("Henry", "Ivy"))
  val contract4 = Contract(
    ApiTypes.ContractId("id4"),
    template2,
    ApiRecord(None, List(ApiRecordField("int", ApiInt64(2)))),
    None,
    Seq("John"),
    Seq("Kevin", "Louise"))

  val contracts = List(contract1, contract2, contract3, contract4)

  def test(criteria: List[(String, SortDirection.Value)], expected: List[Contract]): Unit = {
    it should s"return $expected on sort (${criteria.map { case (k, v) => s"$k($v)" }.mkString(" and ")})" in {
      val sorter = new ContractSorter(
        criteria.map { case (k, v) => new SortCriterion(k, v) },
        damlLfDefDataTypes.get,
        AllContractsPager)
      sorter.sort(contracts) should contain theSameElementsAs expected
    }
  }

  implicit val sortParties: Ordering[Seq[String]] =
    Ordering.fromLessThan[Seq[String]](_.mkString < _.mkString)

  test(List(), contracts)
  test(List("id" -> ASCENDING), contracts.sortBy(_.id.unwrap))
  test(List("id" -> DESCENDING), contracts.sortBy(_.id.unwrap)(Ordering[String].reverse))
  test(
    List("agreementText" -> ASCENDING),
    contracts.sortBy(_.agreementText)
  )
  test(List("signatories" -> ASCENDING), contracts.sortBy(_.signatories))
  test(List("observers" -> ASCENDING), contracts.sortBy(_.observers))
  test(List("argument.foo" -> ASCENDING), List(contract1, contract3, contract2, contract4))
  test(
    List("argument.foo" -> ASCENDING, "id" -> DESCENDING),
    List(contract3, contract1, contract2, contract4))
  test(
    List("argument.int" -> ASCENDING, "id" -> DESCENDING),
    List(contract2, contract4, contract3, contract1))
  test(
    List("argument.int" -> DESCENDING, "id" -> DESCENDING),
    List(contract1, contract3, contract2, contract4))
}
