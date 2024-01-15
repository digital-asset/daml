// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.query

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.daml.lf.data.{Ref => DamlLfRef}
import com.daml.lf.value.Value.{ValueEnum, ValueInt64, ValueText}
import com.daml.navigator.model._
import scalaz.syntax.tag._
import com.daml.navigator.DamlConstants.{name, record}
import com.daml.navigator.query.SortDirection._
import com.daml.ledger.api.refinements.ApiTypes
import scalaz.Tag

class ContractSortSpec extends AnyFlatSpec with Matchers {

  behavior of "ContractSort"

  val damlLfId0 = DamlLfIdentifier(
    DamlLfRef.PackageId.assertFromString("hash"),
    DamlLfQualifiedName(
      DamlLfDottedName.assertFromString("module"),
      DamlLfDottedName.assertFromString("T0"),
    ),
  )
  val damlLfId1 = DamlLfIdentifier(
    DamlLfRef.PackageId.assertFromString("hash"),
    DamlLfQualifiedName(
      DamlLfDottedName.assertFromString("module"),
      DamlLfDottedName.assertFromString("T1"),
    ),
  )
  val damlLfDirectionId = DamlLfIdentifier(
    DamlLfRef.PackageId.assertFromString("hash"),
    DamlLfQualifiedName(
      DamlLfDottedName.assertFromString("module"),
      DamlLfDottedName.assertFromString("Direction"),
    ),
  )

  val DirectionType: DamlLfType =
    DamlLfTypeCon(DamlLfTypeConName(damlLfDirectionId), DamlLfImmArraySeq())

  val damlLfRecord0 = DamlLfDefDataType(
    DamlLfImmArraySeq(),
    DamlLfRecord(
      DamlLfImmArraySeq(
        name("foo") ->
          DamlLfTypePrim(DamlLfPrimType.Text, DamlLfImmArraySeq()),
        name("direction") ->
          DirectionType,
      )
    ),
  )
  val damlLfRecord1 = DamlLfDefDataType(
    DamlLfImmArraySeq(),
    DamlLfRecord(
      DamlLfImmArraySeq(
        name("int") ->
          DamlLfTypePrim(DamlLfPrimType.Int64, DamlLfImmArraySeq())
      )
    ),
  )

  val damlLfId3 = DamlLfIdentifier(
    DamlLfRef.PackageId.assertFromString("hash"),
    DamlLfQualifiedName(
      DamlLfDottedName.assertFromString("module"),
      DamlLfDottedName.assertFromString("I1"),
    ),
  )

  val damlLfEnum = DamlLfDefDataType(
    DamlLfImmArraySeq(),
    DamlLfEnum(DamlLfImmArraySeq(name("North"), name("East"), name("South"), name("West"))),
  )

  val damlLfIdKey = DamlLfIdentifier(
    DamlLfRef.PackageId.assertFromString("hash"),
    DamlLfQualifiedName(
      DamlLfDottedName.assertFromString("module"),
      DamlLfDottedName.assertFromString("K1"),
    ),
  )

  val damlLfRecordKey = DamlLfDefDataType(
    DamlLfImmArraySeq(),
    DamlLfRecord(
      DamlLfImmArraySeq(
        DamlLfRef.Name
          .assertFromString("foo") -> DamlLfTypePrim(DamlLfPrimType.Text, DamlLfImmArraySeq())
      )
    ),
  )

  val damlLfKeyType =
    DamlLfTypeCon(DamlLfTypeConName(damlLfIdKey), DamlLfImmArraySeq.empty[DamlLfType])

  val damlLfDefDataTypes: Map[DamlLfIdentifier, DamlLfDefDataType] = Map(
    damlLfId0 -> damlLfRecord0,
    damlLfId1 -> damlLfRecord1,
    damlLfDirectionId -> damlLfEnum,
  )

  val template1 = Template(damlLfId0, List.empty, None, Set.empty)
  val template2 = Template(damlLfId1, List.empty, Some(damlLfKeyType), Set(damlLfId3))

  val alice = ApiTypes.Party("Alice")
  val bob = ApiTypes.Party("Bob")
  val charlie = ApiTypes.Party("Charlie")
  val dana = ApiTypes.Party("Dana")
  val ernest = ApiTypes.Party("Ernest")
  val francis = ApiTypes.Party("Francis")
  val gloria = ApiTypes.Party("Gloria")
  val henry = ApiTypes.Party("Henry")
  val ivy = ApiTypes.Party("Ivy")
  val john = ApiTypes.Party("John")
  val kevin = ApiTypes.Party("Kevin")
  val louise = ApiTypes.Party("Louise")

  val contract1 = Contract(
    id = ApiTypes.ContractId("id1"),
    template = template1,
    argument = record("foo" -> ValueText("bar"), "direction" -> ValueEnum(None, name("West"))),
    agreementText = None,
    signatories = List(alice),
    observers = List(bob, charlie),
    key = None,
  )
  val contract2 = Contract(
    id = ApiTypes.ContractId("id2"),
    template = template2,
    argument = record("int" -> ValueInt64(1)),
    agreementText = Some(""),
    signatories = List(gloria),
    observers = List(ernest, francis),
    key = Some(record("foo" -> ValueText("foo"))),
  )
  val contract3 = Contract(
    id = ApiTypes.ContractId("id3"),
    template = template1,
    argument = record("foo" -> ValueText("bar"), "direction" -> ValueEnum(None, name("East"))),
    agreementText = Some("agreement"),
    signatories = List(dana),
    observers = List(henry, ivy),
    key = None,
  )
  val contract4 = Contract(
    id = ApiTypes.ContractId("id4"),
    template = template2,
    argument = record("int" -> ValueInt64(2)),
    agreementText = None,
    signatories = List(john),
    observers = List(kevin, louise),
    key = Some(record("foo" -> ValueText("bar"))),
  )

  val contracts = List(contract1, contract2, contract3, contract4)

  def test(criteria: List[(String, SortDirection.Value)], expected: List[Contract]): Unit = {
    it should s"return $expected on sort (${criteria.map { case (k, v) => s"$k($v)" }.mkString(" and ")})" in {
      val sorter = new ContractSorter(
        criteria.map { case (k, v) => new SortCriterion(k, v) },
        damlLfDefDataTypes.get,
        AllContractsPager,
      )
      sorter.sort(contracts) should contain theSameElementsInOrderAs expected
    }
  }

  implicit val sortParties: Ordering[List[ApiTypes.Party]] =
    Ordering
      .fromLessThan[List[ApiTypes.Party]](_.map(Tag.unwrap).mkString < _.map(Tag.unwrap).mkString)

  test(List(), contracts)
  test(List("id" -> ASCENDING), contracts.sortBy(_.id.unwrap))
  test(List("id" -> DESCENDING), contracts.sortBy(_.id.unwrap)(Ordering[String].reverse))
  test(
    List("agreementText" -> ASCENDING),
    contracts.sortBy(_.agreementText.getOrElse("")),
  )
  test(List("signatories" -> ASCENDING), contracts.sortBy(_.signatories))
  test(List("observers" -> ASCENDING), contracts.sortBy(_.observers))

  // FIXME contract2 and contract4 are not compatible with the criteria and should go at the end
  test(List("argument.foo" -> ASCENDING), List(contract2, contract4, contract1, contract3))

  // FIXME contract2 and contract4 are not compatible with the criteria and should go at the end
  test(
    List("argument.foo" -> ASCENDING, "id" -> DESCENDING),
    List(contract4, contract2, contract3, contract1),
  )

  // FIXME contract1 and contract3 are not compatible with the criteria and should go at the end
  test(
    List("argument.int" -> ASCENDING, "id" -> DESCENDING),
    List(contract3, contract1, contract2, contract4),
  )

  // FIXME check this test case according to the issues signaled above
  test(
    List("argument.int" -> DESCENDING, "id" -> DESCENDING),
    List(contract4, contract2, contract3, contract1),
  )

  // FIXME contract1 and contract3 are not compatible with the criteria and should go at the end
  test(List("key.foo" -> ASCENDING), List(contract1, contract3, contract4, contract2))

  test(
    List("template.parameter.foo" -> ASCENDING),
    List(contract2, contract4, contract1, contract3),
  )

  test(
    List("template.parameter.int" -> ASCENDING),
    List(contract1, contract3, contract2, contract4),
  )

  test(
    List("template.parameter.direction" -> ASCENDING),
    List(contract2, contract4, contract1, contract3),
  )
}
