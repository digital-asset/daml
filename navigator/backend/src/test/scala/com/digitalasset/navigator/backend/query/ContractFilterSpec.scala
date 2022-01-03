// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.query

import com.daml.lf.data.{Ref => DamlLfRef}
import com.daml.lf.value.Value.{ValueText, ValueInt64}
import com.daml.navigator.graphql.GraphQLSchema
import com.daml.navigator.model.{Contract, Template}
import com.daml.ledger.api.refinements.ApiTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.daml.navigator.DamlConstants.record
import com.daml.navigator.model._
import scalaz.syntax.tag._

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
class ContractFilterSpec extends AnyFlatSpec with Matchers {

  behavior of "ContractFilter"

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

  val damlLfRecord0 = DamlLfDefDataType(
    DamlLfImmArraySeq(),
    DamlLfRecord(
      DamlLfImmArraySeq(
        DamlLfRef.Name
          .assertFromString("foo") -> DamlLfTypePrim(DamlLfPrimType.Text, DamlLfImmArraySeq())
      )
    ),
  )
  val damlLfRecord1 = DamlLfDefDataType(
    DamlLfImmArraySeq(),
    DamlLfRecord(
      DamlLfImmArraySeq(
        DamlLfRef.Name
          .assertFromString("int") -> DamlLfTypePrim(DamlLfPrimType.Int64, DamlLfImmArraySeq())
      )
    ),
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
    damlLfIdKey -> damlLfRecordKey,
  )

  val template1 = Template(damlLfId0, List.empty, None)
  val template2 = Template(damlLfId1, List.empty, Some(damlLfKeyType))

  val alice = ApiTypes.Party("Alice")
  val bob = ApiTypes.Party("Bob")
  val charlie = ApiTypes.Party("Charlie")

  val contract1 = Contract(
    ApiTypes.ContractId("id1"),
    template1,
    record("foo" -> ValueText("bar")),
    None,
    List(alice),
    List(bob, charlie),
    None,
  )
  val contract2 = Contract(
    ApiTypes.ContractId("id2"),
    template2,
    record("int" -> ValueInt64(12)),
    Some(""),
    List(alice),
    List(bob, charlie),
    Some(record("foo" -> ValueText("bar"))),
  )
  val contract3 = Contract(
    ApiTypes.ContractId("id3"),
    template1,
    record("foo" -> ValueText("bar")),
    Some("agreement"),
    List(alice),
    List(bob, charlie),
    None,
  )

  val templates = List(template1, template2)
  val contracts = List(contract1, contract2, contract3)

  def test(filters: List[(String, String)], expected: List[Contract], isAnd: Boolean): Unit = {
    it should s"return $expected on filter (${filters.map { case (k, v) => s"$k contains $v" }.mkString(" and ")})" in {
      val criteria = filters.map { case (k, v) => new FilterCriterion(k, v) }
      val criterion: FilterCriterionBase =
        if (isAnd) AndFilterCriterion(criteria) else OrFilterCriterion(criteria)
      val filter = new ContractFilter(criterion, damlLfDefDataTypes.get, AllContractsPager)
      contracts.filter(filter.isIncluded) should contain theSameElementsAs expected
    }
  }

  def testAnd(filters: List[(String, String)], expected: List[Contract]): Unit =
    test(filters, expected, true)

  def testOr(filters: List[(String, String)], expected: List[Contract]): Unit =
    test(filters, expected, false)

  testAnd(List(), contracts)
  testAnd(List("id" -> contract1.id.unwrap), List(contract1))
  testAnd(List("id" -> "id"), contracts)
  testAnd(List("id" -> "id-1"), List())
  testAnd(List("argument.foo" -> "b"), List(contract1, contract3))
  testAnd(List("argument.foo" -> "ba"), List(contract1, contract3))
  testAnd(List("argument.foo" -> "bar"), List(contract1, contract3))
  testAnd(List("argument.foo" -> "ar"), List(contract1, contract3))
  testAnd(List("argument.foo" -> "r"), List(contract1, contract3))
  testAnd(List("argument.int" -> "1"), List(contract2))
  testAnd(List("id" -> contract1.id.unwrap, "argument.foo" -> "bar"), List(contract1))
  testAnd(List("template.id" -> template1.idString), List(contract1, contract3))
  testAnd(List("template.parameter.int" -> "int64"), List(contract2))
  testAnd(List("template.topLevelDecl" -> template1.topLevelDecl), List(contract1, contract3))
  testAnd(List("argument.foo" -> "bar", "argument.int" -> "1"), Nil)
  testAnd(List("key.foo" -> "nope"), Nil)
  testAnd(List("key.foo" -> "bar"), List(contract2))

  testOr(List("argument.foo" -> "bar", "argument.int" -> "1"), contracts)

  testAnd(List("agreementText" -> ""), List(contract2, contract3))
  testAnd(List("agreementText" -> "gree"), List(contract3))
  testAnd(List("agreementText" -> "not-matching"), List())

  testAnd(List("signatories" -> "Alice"), List(contract1, contract2, contract3))
  testAnd(List("signatories" -> "Bob"), List())

  testAnd(List("observers" -> "Alice"), List())
  testAnd(List("observers" -> "Bob"), List(contract1, contract2, contract3))

  testOr(List("agreementText" -> "gree", "observers" -> "Alice"), List(contract3))

  val contractSearchFilterCriterion =
    new GraphQLSchema(Set()).contractSearchToFilter(template1.topLevelDecl)
  it should s"return contracts with id ${contract1.id} and ${contract3.id} on filter ($contractSearchFilterCriterion)" in {
    val filter =
      new ContractFilter(contractSearchFilterCriterion, damlLfDefDataTypes.get, AllContractsPager)
    contracts.filter(filter.isIncluded) should contain theSameElementsAs Seq(contract1, contract3)
  }

  val templateSearchFilterCriterion =
    new GraphQLSchema(Set()).templateSearchToFilter(template1.topLevelDecl)
  it should s"return templates with declaration ${template1.topLevelDecl} on filter (${templateSearchFilterCriterion})" in {
    val filter =
      new TemplateFilter(templateSearchFilterCriterion, damlLfDefDataTypes.get, TemplatePager)
    templates.filter(filter.isIncluded) should contain theSameElementsAs Seq(template1)
  }
}
