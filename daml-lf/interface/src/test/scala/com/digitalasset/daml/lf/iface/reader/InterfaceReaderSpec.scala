// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.iface
package reader

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref.{
  DottedName,
  Identifier,
  ModuleName,
  SimpleString,
  QualifiedName
}
import com.digitalasset.daml_lf.DamlLf1
import org.scalatest.{Inside, Matchers, WordSpec}
import scalaz.\/-
import scala.collection.JavaConverters._

class InterfaceReaderSpec extends WordSpec with Matchers with Inside {

  private def dnfs(args: String*): DottedName = DottedName.assertFromSegments(args)
  private val moduleName: ModuleName = dnfs("Main")
  private val packageId: PackageId = SimpleString.assertFromString("dummy-package-id")
  private val ctx: InterfaceReader.Context = InterfaceReader.Context(packageId)

  "variant should extract a variant with type params" in {
    val variantDataType = DamlLf1.DefDataType
      .newBuilder()
      .setName(dottedName("Option"))
      .addParams(starKindedTypeVar("call"))
      .addParams(starKindedTypeVar("put"))
      .setVariant(
        DamlLf1.DefDataType.Fields
          .newBuilder()
          .addFields(varField("Call", "call"))
          .addFields(varField("Put", "put"))
          .build())
      .build()

    val actual = InterfaceReader.variant(moduleName, ctx)(variantDataType)
    val expectedVariant =
      (
        QualifiedName(moduleName, dnfs("Option")),
        ImmArray("call", "put").toSeq,
        Variant(ImmArray(("Call", TypeVar("call")), ("Put", TypeVar("put"))).toSeq))

    actual shouldBe \/-(expectedVariant)
  }

  private[this] def nameClashRecordVariantName(tail: String): TypeConName =
    TypeConName(
      Identifier(packageId, QualifiedName(dnfs("Main"), dnfs("NameClashRecordVariant", tail))))

  "variant should extract a variant, nested records are not be resolved" in {
    val variantDataType = DamlLf1.DefDataType
      .newBuilder()
      .setName(dottedName("NameClashRecordVariant"))
      .setVariant(
        DamlLf1.DefDataType.Fields
          .newBuilder()
          .addFields(typeConstructorField(
            "NameClashRecordVariantA",
            List("NameClashRecordVariant", "NameClashRecordVariantA")))
          .addFields(typeConstructorField(
            "NameClashRecordVariantB",
            List("NameClashRecordVariant", "NameClashRecordVariantB")))
          .build())
      .build()

    val actual = InterfaceReader.variant(moduleName, ctx)(variantDataType)
    val expectedVariant: (QualifiedName, ImmArraySeq[String], Variant.FWT) =
      (
        QualifiedName(moduleName, dnfs("NameClashRecordVariant")),
        ImmArraySeq(),
        Variant(
          ImmArraySeq(
            (
              "NameClashRecordVariantA",
              TypeCon(nameClashRecordVariantName("NameClashRecordVariantA"), ImmArraySeq())),
            (
              "NameClashRecordVariantB",
              TypeCon(nameClashRecordVariantName("NameClashRecordVariantB"), ImmArraySeq()))
          )
        )
      )
    actual shouldBe \/-(expectedVariant)
  }

  "record should extract a nested record" in {
    val recordDataType = DamlLf1.DefDataType
      .newBuilder()
      .setName(dottedName("NameClashRecordVariant", "NameClashRecordVariantA"))
      .setRecord(
        DamlLf1.DefDataType.Fields
          .newBuilder()
          .addFields(primField("wait", DamlLf1.PrimType.INT64))
          .addFields(primField("wait_", DamlLf1.PrimType.INT64))
          .addFields(primField("wait__", DamlLf1.PrimType.INT64))
          .build())
      .build()

    val actual = InterfaceReader.record(moduleName, ctx)(recordDataType)
    val expectedRecord =
      (
        QualifiedName(moduleName, dnfs("NameClashRecordVariant", "NameClashRecordVariantA")),
        ImmArraySeq(),
        Record(
          ImmArraySeq(
            ("wait", TypePrim(PrimTypeInt64, ImmArraySeq())),
            ("wait_", TypePrim(PrimTypeInt64, ImmArraySeq())),
            ("wait__", TypePrim(PrimTypeInt64, ImmArraySeq())))
        ))

    actual shouldBe \/-(expectedRecord)
  }

  "map should extract a map" in {
    val recordDataType = DamlLf1.DefDataType
      .newBuilder()
      .setName(dottedName("MapRecord"))
      .setRecord(
        DamlLf1.DefDataType.Fields
          .newBuilder()
          .addFields(
            primField(
              "map",
              DamlLf1.PrimType.MAP,
              DamlLf1.Type
                .newBuilder()
                .setPrim(DamlLf1.Type.Prim.newBuilder().setPrim(DamlLf1.PrimType.INT64))
                .build()))
          .build())
      .build()

    val actual = InterfaceReader.record(moduleName, ctx)(recordDataType)
    actual shouldBe \/-(
      (
        QualifiedName(moduleName, dnfs("MapRecord")),
        ImmArraySeq(),
        Record(
          ImmArraySeq(
            ("map", TypePrim(PrimTypeMap, ImmArraySeq(TypePrim(PrimTypeInt64, ImmArraySeq()))))
          ))))
  }

  private def dottedName(str: String): DamlLf1.DottedName =
    DamlLf1.DottedName.newBuilder().addSegments(str).build()

  private def dottedName(segments: String*): DamlLf1.DottedName = {
    val b = DamlLf1.DottedName.newBuilder()
    segments.foreach(b.addSegments)
    b.build()
  }

  private def starKindedTypeVar(var_ : String): DamlLf1.TypeVarWithKind =
    DamlLf1.TypeVarWithKind
      .newBuilder()
      .setVar(var_)
      .setKind(DamlLf1.Kind.newBuilder.setStar(DamlLf1.Unit.newBuilder.build))
      .build()

  private def varField(field: String, var_ : String): DamlLf1.FieldWithType =
    DamlLf1.FieldWithType
      .newBuilder()
      .setField(field)
      .setType(DamlLf1.Type.newBuilder().setVar(DamlLf1.Type.Var.newBuilder.setVar(var_).build))
      .build()

  private def primField(
      field: String,
      primType: DamlLf1.PrimType,
      args: DamlLf1.Type*): DamlLf1.FieldWithType = {
    val typ = DamlLf1.Type.Prim.newBuilder.setPrim(primType).addAllArgs(args.asJava).build()
    DamlLf1.FieldWithType
      .newBuilder()
      .setField(field)
      .setType(DamlLf1.Type.newBuilder().setPrim(typ).build)
      .build()
  }

  private def typeConstructorField(field: String, segments: List[String]): DamlLf1.FieldWithType =
    DamlLf1.FieldWithType
      .newBuilder()
      .setField(field)
      .setType(
        DamlLf1.Type
          .newBuilder()
          .setCon(DamlLf1.Type.Con
            .newBuilder()
            .setTycon(typeConName(segments))))
      .build()

  private def typeConName(segments: List[String]): DamlLf1.TypeConName =
    DamlLf1.TypeConName
      .newBuilder()
      .setModule(moduleRef("Main"))
      .setName(dottedName(segments: _*))
      .build()

  private def moduleRef(name: String): DamlLf1.ModuleRef =
    DamlLf1.ModuleRef.newBuilder().setPackageRef(packageRef).setModuleName(dottedName(name)).build()

  private def packageRef: DamlLf1.PackageRef =
    DamlLf1.PackageRef.newBuilder().setSelf(unit).build()

  private def unit: DamlLf1.Unit = DamlLf1.Unit.getDefaultInstance
}
