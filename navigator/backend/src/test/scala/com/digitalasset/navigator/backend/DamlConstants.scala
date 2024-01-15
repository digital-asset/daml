// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import com.daml.lf.data.{
  Decimal => LfDecimal,
  FrontStack,
  ImmArray,
  SortedLookupList,
  Ref => DamlLfRef,
}
import com.daml.navigator.model._
import com.daml.lf.{typesig => DamlLfIface}
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.json.ApiValueImplicits._

import scala.language.implicitConversions

case class DamlConstants()

/** Daml related constants usable in tests
  */
case object DamlConstants {
  // ------------------------------------------------------------------------------------------------------------------
  // Daml-LF: Ids
  // ------------------------------------------------------------------------------------------------------------------
  val packageId0 = DamlLfRef.PackageId.assertFromString("hash")

  def defRef(name: String): DamlLfIdentifier = DamlLfIdentifier(
    packageId0,
    DamlLfQualifiedName(
      DamlLfDottedName.assertFromString("module"),
      DamlLfDottedName.assertFromString(name),
    ),
  )
  val ref0: DamlLfIdentifier = defRef("T0")
  val ref1: DamlLfIdentifier = defRef("T1")
  val ref2: DamlLfIdentifier = defRef("T2")

  // ------------------------------------------------------------------------------------------------------------------
  // Daml-LF: simple types
  // ------------------------------------------------------------------------------------------------------------------
  val simpleTextT = DamlLfTypePrim(DamlLfPrimType.Text, DamlLfImmArraySeq())
  val simpleInt64T = DamlLfTypePrim(DamlLfPrimType.Int64, DamlLfImmArraySeq())
  val simpleDecimalT = DamlLfTypeNumeric(LfDecimal.scale)
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
  def simpleTextMapT(typ: DamlLfIface.Type) =
    DamlLfTypePrim(DamlLfPrimType.TextMap, DamlLfImmArraySeq(typ))

  val simpleTextV = V.ValueText("foo")
  val simpleInt64V = V.ValueInt64(100)
  val simpleDecimalV = V.ValueNumeric(LfDecimal assertFromString "100")
  val simpleUnitV = V.ValueUnit
  val simpleDateV = V.ValueDate.fromIso8601("2019-01-28")
  val simpleTimestampV = V.ValueTimestamp.fromIso8601("2019-01-28T12:44:33.22Z")
  val simpleOptionalV = V.ValueOptional(Some(V.ValueText("foo")))
  val simpleTextMapV = V.ValueTextMap(
    SortedLookupList(Map("1" -> V.ValueInt64(1), "2" -> V.ValueInt64(2), "3" -> V.ValueInt64(3)))
  )

  // ------------------------------------------------------------------------------------------------------------------
  // Daml-LF: empty record
  // ------------------------------------------------------------------------------------------------------------------
  val emptyRecordId: DamlLfIdentifier = defRef("EmptyRecord")
  val emptyRecordGD = DamlLfRecord(DamlLfImmArraySeq())
  val emptyRecordGC = DamlLfDefDataType(DamlLfImmArraySeq(), emptyRecordGD)
  val emptyRecordTC = DamlLfTypeCon(DamlLfTypeConName(emptyRecordId), DamlLfImmArraySeq())
  val emptyRecordT = emptyRecordTC.instantiate(emptyRecordGC).asInstanceOf[DamlLfRecord]
  val emptyRecordV = V.ValueRecord(Some(emptyRecordId), ImmArray.Empty)

  // ------------------------------------------------------------------------------------------------------------------
  // Daml-LF: simple record (data SimpleRecord a b = {fA: a, fB: b})
  // ------------------------------------------------------------------------------------------------------------------
  val simpleRecordId: DamlLfIdentifier = defRef("SimpleRecord")
  val simpleRecordGD = DamlLfRecord(
    DamlLfImmArraySeq(
      name("fA") -> DamlLfTypeVar("a"),
      name("fB") -> DamlLfTypeVar("b"),
    )
  )
  val simpleRecordGC = DamlLfDefDataType(DamlLfImmArraySeq("a", "b"), simpleRecordGD)
  val simpleRecordTC = DamlLfTypeCon(
    DamlLfTypeConName(simpleRecordId),
    DamlLfImmArraySeq(simpleTextT, simpleInt64T),
  )
  val simpleRecordT = simpleRecordTC.instantiate(simpleRecordGC).asInstanceOf[DamlLfRecord]
  def simpleRecordV =
    V.ValueRecord(
      Some(simpleRecordId),
      ImmArray(
        (Some(name("fA")), V.ValueText("foo")),
        (Some(name("fB")), V.ValueInt64(100)),
      ),
    )

  // ------------------------------------------------------------------------------------------------------------------
  // Daml-LF: simple variant (data DamlLfVariant a b = fA a | fB b)
  // ------------------------------------------------------------------------------------------------------------------
  val simpleVariantId: DamlLfIdentifier = defRef("SimpleVariant")
  val simpleVariantGD = DamlLfVariant(
    DamlLfImmArraySeq(
      name("fA") -> DamlLfTypeVar("a"),
      name("fB") -> DamlLfTypeVar("b"),
    )
  )
  val simpleVariantGC = DamlLfDefDataType(DamlLfImmArraySeq("a", "b"), simpleVariantGD)
  val simpleVariantTC = DamlLfTypeCon(
    DamlLfTypeConName(simpleVariantId),
    DamlLfImmArraySeq(simpleTextT, simpleInt64T),
  )
  val simpleVariantT = simpleVariantTC.instantiate(simpleVariantGC).asInstanceOf[DamlLfVariant]
  def simpleVariantV = V.ValueVariant(Some(simpleVariantId), "fA", V.ValueText("foo"))

  // ------------------------------------------------------------------------------------------------------------------
  // Daml-LF: recursive type (data Tree = Leaf a | Node {left: Tree a, right: Tree a})
  // ------------------------------------------------------------------------------------------------------------------
  val treeNodeId: DamlLfIdentifier = defRef("TreeNode")
  val treeId: DamlLfIdentifier = defRef("Tree")

  val treeNodeGD = DamlLfRecord(
    DamlLfImmArraySeq(
      name("left") -> DamlLfTypeCon(
        DamlLfTypeConName(treeId),
        DamlLfImmArraySeq(DamlLfTypeVar("a")),
      ),
      name("right") -> DamlLfTypeCon(
        DamlLfTypeConName(treeId),
        DamlLfImmArraySeq(DamlLfTypeVar("a")),
      ),
    )
  )
  val treeNodeGC = DamlLfDefDataType(DamlLfImmArraySeq("a"), treeNodeGD)
  val treeNodeTC = DamlLfTypeCon(
    DamlLfTypeConName(treeNodeId),
    DamlLfImmArraySeq(simpleTextT),
  )
  val treeNodeT = treeNodeTC.instantiate(treeNodeGC).asInstanceOf[DamlLfRecord]

  val treeGD = DamlLfVariant(
    DamlLfImmArraySeq(
      name("Leaf") -> DamlLfTypeVar("a"),
      name("Node") -> DamlLfTypeCon(
        DamlLfTypeConName(treeNodeId),
        DamlLfImmArraySeq(DamlLfTypeVar("a")),
      ),
    )
  )
  val treeGC = DamlLfDefDataType(DamlLfImmArraySeq("a"), treeGD)
  val treeTC = DamlLfTypeCon(
    DamlLfTypeConName(treeId),
    DamlLfImmArraySeq(simpleTextT),
  )

  val treeLeftV = V.ValueVariant(
    Some(treeId),
    "Node",
    V.ValueRecord(
      Some(treeNodeId),
      ImmArray(
        Some(name("left")) -> V.ValueVariant(Some(treeId), "Leaf", V.ValueText("LL")),
        Some(name("right")) -> V.ValueVariant(Some(treeId), "Leaf", V.ValueText("LR")),
      ),
    ),
  )

  val treeRightV = V.ValueVariant(
    Some(treeId),
    "Node",
    V.ValueRecord(
      Some(treeNodeId),
      ImmArray(
        Some(name("left")) -> V.ValueVariant(Some(treeId), "Leaf", V.ValueText("RL")),
        Some(name("right")) -> V.ValueVariant(Some(treeId), "Leaf", V.ValueText("RR")),
      ),
    ),
  )

  val treeV = V.ValueVariant(
    Some(treeId),
    "Node",
    V.ValueRecord(
      Some(treeNodeId),
      ImmArray(Some(name("left")) -> treeLeftV, Some(name("right")) -> treeRightV),
    ),
  )

  val colorGD = DamlLfEnum(DamlLfImmArraySeq(name("Red"), name("Green"), name("Blue")))
  val colorGC = DamlLfDefDataType(DamlLfImmArraySeq.empty, colorGD)
  val colorId: DamlLfIdentifier = defRef("Color")
  val redTC = DamlLfTypeCon(
    DamlLfTypeConName(colorId),
    DamlLfImmArraySeq.empty,
  )
  val redV = V.ValueEnum(Some(colorId), "Red")

  // ------------------------------------------------------------------------------------------------------------------
  // Daml-LF: complex record containing all Daml types
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
      name("fMap") -> simpleTextMapT(simpleInt64T),
      name("fVariant") -> simpleVariantTC,
      name("fRecord") -> simpleRecordTC,
    )
  )
  val complexRecordGC = DamlLfDefDataType(DamlLfImmArraySeq(), complexRecordGD)
  val complexRecordTC = DamlLfTypeCon(DamlLfTypeConName(complexRecordId), DamlLfImmArraySeq())
  val complexRecordT = complexRecordTC.instantiate(complexRecordGC).asInstanceOf[DamlLfRecord]
  val complexRecordV = V.ValueRecord(
    Some(complexRecordId),
    ImmArray(
      ("fText", simpleTextV),
      ("fBool", V.ValueTrue),
      ("fDecimal", simpleDecimalV),
      ("fUnit", V.ValueUnit),
      ("fInt64", simpleInt64V),
      ("fParty", V.ValueParty(DamlLfRef.Party assertFromString "BANK1")),
      ("fContractId", V.ValueContractId(V.ContractId.assertFromString("00" + "00" * 32 + "c0"))),
      ("fListOfText", V.ValueList(FrontStack(V.ValueText("foo"), V.ValueText("bar")))),
      ("fListOfUnit", V.ValueList(FrontStack(V.ValueUnit, V.ValueUnit))),
      ("fDate", simpleDateV),
      ("fTimestamp", simpleTimestampV),
      ("fOptionalText", V.ValueNone),
      ("fOptionalUnit", V.ValueOptional(Some(V.ValueUnit))),
      ("fOptOptText", V.ValueOptional(Some(V.ValueOptional(Some(V.ValueText("foo")))))),
      (
        "fMap",
        V.ValueTextMap(
          SortedLookupList(
            Map("1" -> V.ValueInt64(1), "2" -> V.ValueInt64(2), "3" -> V.ValueInt64(3))
          )
        ),
      ),
      ("fVariant", simpleVariantV),
      ("fRecord", simpleRecordV),
    ).map { case (k, v) => (Some(name(k)), v) },
  )

  // ------------------------------------------------------------------------------------------------------------------
  // Daml-LF: package mockup
  // ------------------------------------------------------------------------------------------------------------------
  val allTypes: Map[DamlLfIdentifier, DamlLfDefDataType] = Map(
    emptyRecordId -> emptyRecordGC,
    simpleRecordId -> simpleRecordGC,
    simpleVariantId -> simpleVariantGC,
    complexRecordId -> complexRecordGC,
    treeId -> treeGC,
    treeNodeId -> treeNodeGC,
    colorId -> colorGC,
  )

  // Note: these templates may not be valid Daml templates
  val simpleRecordTemplateId: DamlLfIdentifier = defRef("SimpleRecordTemplate")
  private val ChoiceUnit = DamlLfRef.Name.assertFromString("unit")
  private val choiceText = DamlLfRef.Name.assertFromString("text")
  private val choiceNonconsuming = DamlLfRef.Name.assertFromString("nonconsuming")
  private val ChoiceReplace = DamlLfRef.Name.assertFromString("replace")

  val simpleRecordTemplate = DamlLfIface.PackageSignature.TypeDecl.Template(
    simpleRecordT,
    DamlLfIface.DefTemplate(
      DamlLfIface.TemplateChoices.Resolved fromDirect Map(
        ChoiceUnit -> DamlLfIface.TemplateChoice(simpleUnitT, false, simpleUnitT),
        choiceText -> DamlLfIface.TemplateChoice(simpleTextT, false, simpleUnitT),
        choiceNonconsuming -> DamlLfIface.TemplateChoice(simpleUnitT, true, simpleUnitT),
        ChoiceReplace -> DamlLfIface.TemplateChoice(simpleRecordTC, false, simpleUnitT),
      ),
      None,
      Seq.empty,
    ),
  )
  val complexRecordTemplate = DamlLfIface.PackageSignature.TypeDecl.Template(
    complexRecordT,
    DamlLfIface.DefTemplate(
      DamlLfIface.TemplateChoices.Resolved fromDirect Map(
        ChoiceUnit -> DamlLfIface.TemplateChoice(simpleUnitT, false, simpleUnitT),
        choiceText -> DamlLfIface.TemplateChoice(simpleTextT, false, simpleUnitT),
        choiceNonconsuming -> DamlLfIface.TemplateChoice(simpleUnitT, true, simpleUnitT),
        ChoiceReplace -> DamlLfIface.TemplateChoice(complexRecordTC, false, simpleUnitT),
      ),
      None,
      Seq.empty,
    ),
  )
  val treeNodeTemplate = DamlLfIface.PackageSignature.TypeDecl.Template(
    treeNodeT,
    DamlLfIface.DefTemplate(
      DamlLfIface.TemplateChoices.Resolved fromDirect Map(
        ChoiceUnit -> DamlLfIface.TemplateChoice(simpleUnitT, false, simpleUnitT),
        choiceText -> DamlLfIface.TemplateChoice(simpleTextT, false, simpleUnitT),
        choiceNonconsuming -> DamlLfIface.TemplateChoice(simpleUnitT, true, simpleUnitT),
        ChoiceReplace -> DamlLfIface.TemplateChoice(treeNodeTC, false, simpleUnitT),
      ),
      None,
      Seq.empty,
    ),
  )

  def complexGenMapT(keyTyp: DamlLfIface.Type, valueTyp: DamlLfIface.Type) =
    DamlLfTypePrim(DamlLfPrimType.GenMap, DamlLfImmArraySeq(keyTyp, valueTyp))

  val complexGenMapV = V.ValueGenMap(
    ImmArray(
      treeV -> V.ValueInt64(1),
      treeLeftV -> V.ValueInt64(2),
      treeRightV -> V.ValueInt64(3),
    )
  )

  val iface = DamlLfIface.PackageSignature(
    packageId0,
    None,
    Map(
      emptyRecordId.qualifiedName -> DamlLfIface.PackageSignature.TypeDecl.Normal(emptyRecordGC),
      simpleRecordId.qualifiedName -> DamlLfIface.PackageSignature.TypeDecl.Normal(simpleRecordGC),
      simpleVariantId.qualifiedName -> DamlLfIface.PackageSignature.TypeDecl
        .Normal(simpleVariantGC),
      treeId.qualifiedName -> DamlLfIface.PackageSignature.TypeDecl.Normal(treeGC),
      treeNodeId.qualifiedName -> DamlLfIface.PackageSignature.TypeDecl.Normal(treeNodeGC),
      simpleRecordTemplateId.qualifiedName -> simpleRecordTemplate,
      complexRecordId.qualifiedName -> complexRecordTemplate,
      treeNodeId.qualifiedName -> treeNodeTemplate,
    ),
    Map.empty,
  )

  private[navigator] implicit def name(s: String): DamlLfRef.Name =
    DamlLfRef.Name.assertFromString(s)

  @throws[IllegalArgumentException]
  private[navigator] def record(fields: (String, ApiValue)*): ApiRecord =
    V.ValueRecord(
      None,
      fields.iterator.map { case (label, value) => Some(name(label)) -> value }.to(ImmArray),
    )

}
