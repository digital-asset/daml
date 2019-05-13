// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator

import com.digitalasset.daml.lf.data.SortedLookupList
import com.digitalasset.navigator.model._
import com.digitalasset.daml.lf.{iface => DamlLfIface}
import com.digitalasset.daml.lf.data.{Ref => DamlLfRef}
case class DamlConstants()

/**
  * DAML related constants usable in tests
  */
case object DamlConstants {
  // ------------------------------------------------------------------------------------------------------------------
  // DAML-LF: Ids
  // ------------------------------------------------------------------------------------------------------------------
  val packageId0 = DamlLfRef.PackageId.assertFromString("hash")

  def defRef(name: String): DamlLfDefRef = DamlLfDefRef(
    packageId0,
    DamlLfQualifiedName(
      DamlLfDottedName.assertFromStrings(DamlLfImmArray("module")),
      DamlLfDottedName.assertFromStrings(DamlLfImmArray(name))
    )
  )
  val ref0: DamlLfDefRef = defRef("T0")
  val ref1: DamlLfDefRef = defRef("T1")
  val ref2: DamlLfDefRef = defRef("T2")

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
  val emptyRecordId: DamlLfDefRef = defRef("EmptyRecord")
  val emptyRecordGD = DamlLfRecord(DamlLfImmArraySeq())
  val emptyRecordGC = DamlLfDefDataType(DamlLfImmArraySeq(), emptyRecordGD)
  val emptyRecordTC = DamlLfTypeCon(DamlLfTypeConName(emptyRecordId), DamlLfImmArraySeq())
  val emptyRecordT = emptyRecordTC.instantiate(emptyRecordGC).asInstanceOf[DamlLfRecord]
  val emptyRecordV = ApiRecord(Some(emptyRecordId), List())

  // ------------------------------------------------------------------------------------------------------------------
  // DAML-LF: simple record (data SimpleRecord a b = {fA: a, fB: b})
  // ------------------------------------------------------------------------------------------------------------------
  val simpleRecordId: DamlLfDefRef = defRef("SimpleRecord")
  val simpleRecordGD = DamlLfRecord(
    DamlLfImmArraySeq(
      "fA" -> DamlLfTypeVar("a"),
      "fB" -> DamlLfTypeVar("b")
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
  val simpleVariantId: DamlLfDefRef = defRef("SimpleVariant")
  val simpleVariantGD = DamlLfVariant(
    DamlLfImmArraySeq(
      "fA" -> DamlLfTypeVar("a"),
      "fB" -> DamlLfTypeVar("b")
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
  val treeNodeId: DamlLfDefRef = defRef("TreeNode")
  val treeId: DamlLfDefRef = defRef("Tree")

  val treeNodeGD = DamlLfRecord(
    DamlLfImmArraySeq(
      "left" -> DamlLfTypeCon(DamlLfTypeConName(treeId), DamlLfImmArraySeq(DamlLfTypeVar("a"))),
      "right" -> DamlLfTypeCon(DamlLfTypeConName(treeId), DamlLfImmArraySeq(DamlLfTypeVar("a")))
    ))
  val treeNodeGC = DamlLfDefDataType(DamlLfImmArraySeq("a"), treeNodeGD)
  val treeNodeTC = DamlLfTypeCon(
    DamlLfTypeConName(treeNodeId),
    DamlLfImmArraySeq(simpleTextT)
  )
  val treeNodeT = treeNodeTC.instantiate(treeNodeGC).asInstanceOf[DamlLfRecord]

  val treeGD = DamlLfVariant(
    DamlLfImmArraySeq(
      "Leaf" -> DamlLfTypeVar("a"),
      "Node" -> DamlLfTypeCon(DamlLfTypeConName(treeNodeId), DamlLfImmArraySeq(DamlLfTypeVar("a")))
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
  val complexRecordId: DamlLfDefRef = defRef("ComplexRecord")
  val complexRecordGD = DamlLfRecord(
    DamlLfImmArraySeq(
      "fText" -> simpleTextT,
      "fBool" -> simpleBoolT,
      "fDecimal" -> simpleDecimalT,
      "fUnit" -> simpleUnitT,
      "fInt64" -> simpleInt64T,
      "fParty" -> simplePartyT,
      "fContractId" -> simpleContractIdT,
      "fListOfText" -> simpleListT(simpleTextT),
      "fListOfUnit" -> simpleListT(simpleUnitT),
      "fDate" -> simpleDateT,
      "fTimestamp" -> simpleTimestampT,
      "fOptionalText" -> simpleOptionalT(simpleTextT),
      "fOptionalUnit" -> simpleOptionalT(simpleUnitT),
      "fOptOptText" -> simpleOptionalT(simpleOptionalT(simpleTextT)),
      "fMap" -> simpleMapT(simpleInt64T),
      "fVariant" -> simpleVariantTC,
      "fRecord" -> simpleRecordTC
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
  val allTypes: Map[DamlLfDefRef, DamlLfDefDataType] = Map(
    emptyRecordId -> emptyRecordGC,
    simpleRecordId -> simpleRecordGC,
    simpleVariantId -> simpleVariantGC,
    complexRecordId -> complexRecordGC,
    treeId -> treeGC,
    treeNodeId -> treeNodeGC
  )

  // Note: these templates may not be valid DAML templates
  val simpleRecordTemplateId: DamlLfDefRef = defRef("SimpleRecordTemplate")
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
}
