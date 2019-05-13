// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator

import com.digitalasset.daml.lf.data.SortedLookupList
import com.digitalasset.navigator.model._
import com.digitalasset.daml.lf.{iface => DamlLfIface}
import com.digitalasset.daml.lf.data.{Ref => DamlLfRef}

import scala.language.implicitConversions

case class DamlConstants()

/**
  * DAML related constants usable in tests
  */
case object DamlConstants {
  // ------------------------------------------------------------------------------------------------------------------
  // DAML-LF: Ids
  // ------------------------------------------------------------------------------------------------------------------
  val packageId0 = DamlLfRef.PackageId.assertFromString("hash")

  def defRef(name: String): DamlLfIdentifier = DamlLfIdentifier(
    packageId0,
    DamlLfQualifiedName(
      DamlLfDottedName.assertFromString("module"),
      DamlLfDottedName.assertFromString(name)
    )
  )
  val ref0: DamlLfIdentifier = defRef("T0")
  val ref1: DamlLfIdentifier = defRef("T1")
  val ref2: DamlLfIdentifier = defRef("T2")

  // ------------------------------------------------------------------------------------------------------------------
  // DAML-LF: simple types
  // ------------------------------------------------------------------------------------------------------------------
  val simpleTextT = DamlLfTypePrim(DamlLfPrimType.Text, DamlLfImmArraySeq())
  val simpleInt64T = DamlLfTypePrim(DamlLfPrimType.Int64, DamlLfImmArraySeq())
  val simpleDecimalT = DamlLfTypePrim(DamlLfPrimType.Decimal, DamlLfImmArraySeq())
  val simpleUnitT = DamlLfTypePrim(DamlLfPrimType.Unit, DamlLfImmArraySeq())
  val simpleDateT = DamlLfTypePrim(DamlLfPrimType.Date, DamlLfImmArraySeq())
  val simpleTimestampT = DamlLfTypePrim(DamlLfPrimType.Timestamp, DamlLfImmArraySeq())
  val simpleBoolT = DamlLfTypePrim(DamlLfPrimType.Bool, DamlLfImmArraySeq())
  val simplePartyT = DamlLfTypePrim(DamlLfPrimType.Party, DamlLfImmArraySeq())
  val simpleContractIdT = DamlLfTypePrim(DamlLfPrimType.ContractId, DamlLfImmArraySeq())
  def simpleOptionalT(typ: DamlLfIface.Type) =
    DamlLfTypePrim(DamlLfPrimType.Optional, DamlLfImmArraySeq(typ))
  def simpleListT(typ: DamlLfIface.Type) =
    DamlLfTypePrim(DamlLfPrimType.List, DamlLfImmArraySeq(typ))
  def simpleMapT(typ: DamlLfIface.Type) = DamlLfTypePrim(DamlLfPrimType.Map, DamlLfImmArraySeq(typ))

  val simpleTextV = ApiText("foo")
  val simpleInt64V = ApiInt64(100)
  val simpleDecimalV = ApiDecimal("100")
  val simpleUnitV = ApiUnit()
  val simpleDateV = ApiDate.fromIso8601("2019-01-28")
  val simpleTimestampV = ApiTimestamp.fromIso8601("2019-01-28T12:44:33.22Z")
  val simpleOptionalV = ApiOptional(Some(ApiText("foo")))
  val simpleMapV = ApiMap(
    SortedLookupList(Map("1" -> ApiInt64(1), "2" -> ApiInt64(2), "3" -> ApiInt64(3))))

  // ------------------------------------------------------------------------------------------------------------------
  // DAML-LF: empty record
  // ------------------------------------------------------------------------------------------------------------------
  val emptyRecordId: DamlLfIdentifier = defRef("EmptyRecord")
  val emptyRecordGD = DamlLfRecord(DamlLfImmArraySeq())
  val emptyRecordGC = DamlLfDefDataType(DamlLfImmArraySeq(), emptyRecordGD)
  val emptyRecordTC = DamlLfTypeCon(DamlLfTypeConName(emptyRecordId), DamlLfImmArraySeq())
  val emptyRecordT = emptyRecordTC.instantiate(emptyRecordGC).asInstanceOf[DamlLfRecord]
  val emptyRecordV = ApiRecord(Some(emptyRecordId), List())

  // ------------------------------------------------------------------------------------------------------------------
  // DAML-LF: simple record (data SimpleRecord a b = {fA: a, fB: b})
  // ------------------------------------------------------------------------------------------------------------------
  val simpleRecordId: DamlLfIdentifier = defRef("SimpleRecord")
  val simpleRecordGD = DamlLfRecord(
    DamlLfImmArraySeq(
      name("fA") -> DamlLfTypeVar("a"),
      name("fB") -> DamlLfTypeVar("b")
    ))
  val simpleRecordGC = DamlLfDefDataType(DamlLfImmArraySeq("a", "b"), simpleRecordGD)
  val simpleRecordTC = DamlLfTypeCon(
    DamlLfTypeConName(simpleRecordId),
    DamlLfImmArraySeq(simpleTextT, simpleInt64T)
  )
  val simpleRecordT = simpleRecordTC.instantiate(simpleRecordGC).asInstanceOf[DamlLfRecord]
  def simpleRecordV =
    ApiRecord(
      Some(simpleRecordId),
      List(
        ApiRecordField("fA", ApiText("foo")),
        ApiRecordField("fB", ApiInt64(100))
      ))

  // ------------------------------------------------------------------------------------------------------------------
  // DAML-LF: simple variant (data DamlLfVariant a b = fA a | fB b)
  // ------------------------------------------------------------------------------------------------------------------
  val simpleVariantId: DamlLfIdentifier = defRef("SimpleVariant")
  val simpleVariantGD = DamlLfVariant(
    DamlLfImmArraySeq(
      name("fA") -> DamlLfTypeVar("a"),
      name("fB") -> DamlLfTypeVar("b")
    ))
  val simpleVariantGC = DamlLfDefDataType(DamlLfImmArraySeq("a", "b"), simpleVariantGD)
  val simpleVariantTC = DamlLfTypeCon(
    DamlLfTypeConName(simpleVariantId),
    DamlLfImmArraySeq(simpleTextT, simpleInt64T)
  )
  val simpleVariantT = simpleVariantTC.instantiate(simpleVariantGC).asInstanceOf[DamlLfVariant]
  def simpleVariantV = ApiVariant(Some(simpleVariantId), "fA", ApiText("foo"))

  // ------------------------------------------------------------------------------------------------------------------
  // DAML-LF: recursive type (data Tree = Leaf a | Node {left: Tree a, right: Tree a})
  // ------------------------------------------------------------------------------------------------------------------
  val treeNodeId: DamlLfIdentifier = defRef("TreeNode")
  val treeId: DamlLfIdentifier = defRef("Tree")

  val treeNodeGD = DamlLfRecord(
    DamlLfImmArraySeq(
      name("left") -> DamlLfTypeCon(
        DamlLfTypeConName(treeId),
        DamlLfImmArraySeq(DamlLfTypeVar("a"))),
      name("right") -> DamlLfTypeCon(
        DamlLfTypeConName(treeId),
        DamlLfImmArraySeq(DamlLfTypeVar("a")))
    ))
  val treeNodeGC = DamlLfDefDataType(DamlLfImmArraySeq("a"), treeNodeGD)
  val treeNodeTC = DamlLfTypeCon(
    DamlLfTypeConName(treeNodeId),
    DamlLfImmArraySeq(simpleTextT)
  )
  val treeNodeT = treeNodeTC.instantiate(treeNodeGC).asInstanceOf[DamlLfRecord]

  val treeGD = DamlLfVariant(
    DamlLfImmArraySeq(
      name("Leaf") -> DamlLfTypeVar("a"),
      name("Node") -> DamlLfTypeCon(
        DamlLfTypeConName(treeNodeId),
        DamlLfImmArraySeq(DamlLfTypeVar("a")))
    ))
  val treeGC = DamlLfDefDataType(DamlLfImmArraySeq("a"), treeGD)
  val treeTC = DamlLfTypeCon(
    DamlLfTypeConName(treeId),
    DamlLfImmArraySeq(simpleTextT)
  )

  val treeV = ApiVariant(
    Some(treeId),
    "Node",
    ApiRecord(
      Some(treeNodeId),
      List(
        ApiRecordField(
          "left",
          ApiVariant(
            Some(treeId),
            "Node",
            ApiRecord(
              Some(treeNodeId),
              List(
                ApiRecordField("left", ApiVariant(Some(treeId), "Leaf", ApiText("LL"))),
                ApiRecordField("right", ApiVariant(Some(treeId), "Leaf", ApiText("LR")))
              )
            )
          )
        ),
        ApiRecordField(
          "right",
          ApiVariant(
            Some(treeId),
            "Node",
            ApiRecord(
              Some(treeNodeId),
              List(
                ApiRecordField("left", ApiVariant(Some(treeId), "Leaf", ApiText("RL"))),
                ApiRecordField("right", ApiVariant(Some(treeId), "Leaf", ApiText("RR")))
              )
            )
          )
        )
      )
    )
  )

  // ------------------------------------------------------------------------------------------------------------------
  // DAML-LF: complex record containing all DAML types
  // ------------------------------------------------------------------------------------------------------------------
  val complexRecordId: DamlLfIdentifier = defRef("ComplexRecord")
  val complexRecordGD = DamlLfRecord(
    DamlLfImmArraySeq(
      name("fText") -> simpleTextT,
      name("fBool") -> simpleBoolT,
      name("fDecimal") -> simpleDecimalT,
      name("fUnit") -> simpleUnitT,
      name("fInt64") -> simpleInt64T,
      name("fParty") -> simplePartyT,
      name("fContractId") -> simpleContractIdT,
      name("fListOfText") -> simpleListT(simpleTextT),
      name("fListOfUnit") -> simpleListT(simpleUnitT),
      name("fDate") -> simpleDateT,
      name("fTimestamp") -> simpleTimestampT,
      name("fOptionalText") -> simpleOptionalT(simpleTextT),
      name("fOptionalUnit") -> simpleOptionalT(simpleUnitT),
      name("fOptOptText") -> simpleOptionalT(simpleOptionalT(simpleTextT)),
      name("fMap") -> simpleMapT(simpleInt64T),
      name("fVariant") -> simpleVariantTC,
      name("fRecord") -> simpleRecordTC
    ))
  val complexRecordGC = DamlLfDefDataType(DamlLfImmArraySeq(), complexRecordGD)
  val complexRecordTC = DamlLfTypeCon(DamlLfTypeConName(complexRecordId), DamlLfImmArraySeq())
  val complexRecordT = complexRecordTC.instantiate(complexRecordGC).asInstanceOf[DamlLfRecord]
  val complexRecordV = ApiRecord(
    Some(complexRecordId),
    List(
      ApiRecordField("fText", simpleTextV),
      ApiRecordField("fBool", ApiBool(true)),
      ApiRecordField("fDecimal", simpleDecimalV),
      ApiRecordField("fUnit", ApiUnit()),
      ApiRecordField("fInt64", simpleInt64V),
      ApiRecordField("fParty", ApiParty("BANK1")),
      ApiRecordField("fContractId", ApiContractId("C0")),
      ApiRecordField("fListOfText", ApiList(List(ApiText("foo"), ApiText("bar")))),
      ApiRecordField("fListOfUnit", ApiList(List(ApiUnit(), ApiUnit()))),
      ApiRecordField("fDate", simpleDateV),
      ApiRecordField("fTimestamp", simpleTimestampV),
      ApiRecordField("fOptionalText", ApiOptional(None)),
      ApiRecordField("fOptionalUnit", ApiOptional(Some(ApiUnit()))),
      ApiRecordField("fOptOptText", ApiOptional(Some(ApiOptional(Some(ApiText("foo")))))),
      ApiRecordField(
        "fMap",
        ApiMap(SortedLookupList(Map("1" -> ApiInt64(1), "2" -> ApiInt64(2), "3" -> ApiInt64(3))))),
      ApiRecordField("fVariant", simpleVariantV),
      ApiRecordField("fRecord", simpleRecordV)
    )
  )

  // ------------------------------------------------------------------------------------------------------------------
  // DAML-LF: package mockup
  // ------------------------------------------------------------------------------------------------------------------
  val allTypes: Map[DamlLfIdentifier, DamlLfDefDataType] = Map(
    emptyRecordId -> emptyRecordGC,
    simpleRecordId -> simpleRecordGC,
    simpleVariantId -> simpleVariantGC,
    complexRecordId -> complexRecordGC,
    treeId -> treeGC,
    treeNodeId -> treeNodeGC
  )

  // Note: these templates may not be valid DAML templates
  val simpleRecordTemplateId: DamlLfIdentifier = defRef("SimpleRecordTemplate")
  private val ChoiceUnit = DamlLfRef.Name.assertFromString("unit")
  private val choiceText = DamlLfRef.Name.assertFromString("text")
  private val choiceNonconsuming = DamlLfRef.Name.assertFromString("nonconsuming")
  private val ChoiceReplace = DamlLfRef.Name.assertFromString("replace")

  val simpleRecordTemplate = DamlLfIface.InterfaceType.Template(
    simpleRecordT,
    DamlLfIface.DefTemplate(
      Map(
        ChoiceUnit -> DamlLfIface.TemplateChoice(simpleUnitT, false, simpleUnitT),
        choiceText -> DamlLfIface.TemplateChoice(simpleTextT, false, simpleUnitT),
        choiceNonconsuming -> DamlLfIface.TemplateChoice(simpleUnitT, true, simpleUnitT),
        ChoiceReplace -> DamlLfIface.TemplateChoice(simpleRecordTC, false, simpleUnitT)
      ))
  )
  val complexRecordTemplate = DamlLfIface.InterfaceType.Template(
    complexRecordT,
    DamlLfIface.DefTemplate(
      Map(
        ChoiceUnit -> DamlLfIface.TemplateChoice(simpleUnitT, false, simpleUnitT),
        choiceText -> DamlLfIface.TemplateChoice(simpleTextT, false, simpleUnitT),
        choiceNonconsuming -> DamlLfIface.TemplateChoice(simpleUnitT, true, simpleUnitT),
        ChoiceReplace -> DamlLfIface.TemplateChoice(complexRecordTC, false, simpleUnitT)
      ))
  )
  val treeNodeTemplate = DamlLfIface.InterfaceType.Template(
    treeNodeT,
    DamlLfIface.DefTemplate(
      Map(
        ChoiceUnit -> DamlLfIface.TemplateChoice(simpleUnitT, false, simpleUnitT),
        choiceText -> DamlLfIface.TemplateChoice(simpleTextT, false, simpleUnitT),
        choiceNonconsuming -> DamlLfIface.TemplateChoice(simpleUnitT, true, simpleUnitT),
        ChoiceReplace -> DamlLfIface.TemplateChoice(treeNodeTC, false, simpleUnitT)
      ))
  )

  val iface = DamlLfIface.Interface(
    packageId0,
    Map(
      emptyRecordId.qualifiedName -> DamlLfIface.InterfaceType.Normal(emptyRecordGC),
      simpleRecordId.qualifiedName -> DamlLfIface.InterfaceType.Normal(simpleRecordGC),
      simpleVariantId.qualifiedName -> DamlLfIface.InterfaceType.Normal(simpleVariantGC),
      treeId.qualifiedName -> DamlLfIface.InterfaceType.Normal(treeGC),
      treeNodeId.qualifiedName -> DamlLfIface.InterfaceType.Normal(treeNodeGC),
      simpleRecordTemplateId.qualifiedName -> simpleRecordTemplate,
      complexRecordId.qualifiedName -> complexRecordTemplate,
      treeNodeId.qualifiedName -> treeNodeTemplate
    )
  )

  private implicit def name(s: String): DamlLfRef.Name =
    DamlLfRef.Name.assertFromString(s)

}
