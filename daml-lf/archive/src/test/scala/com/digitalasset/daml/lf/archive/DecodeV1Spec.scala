// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.math.BigDecimal
import java.nio.file.{Files, Paths}

import com.daml.bazeltools.BazelRunfiles._
import com.daml.lf.archive.Reader.ParseError
import com.daml.lf.data.{Decimal, Numeric, Ref}
import com.daml.lf.language.Util._
import com.daml.lf.language.{Ast, LanguageMinorVersion, LanguageVersion => LV}
import LanguageMinorVersion.Implicits._
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.DottedName
import com.daml.daml_lf_dev.DamlLf1
import com.daml.lf.transaction.VersionTimeline
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{Inside, OptionValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

class DecodeV1Spec
    extends AnyWordSpec
    with Matchers
    with Inside
    with OptionValues
    with ScalaCheckPropertyChecks {

  "The entries of primTypeInfos correspond to Protobuf DamlLf1.PrimType" in {

    (Set(
      DamlLf1.PrimType.UNRECOGNIZED,
      DamlLf1.PrimType.DECIMAL,
      // FIXME: https://github.com/digital-asset/daml/issues/8020
      // exception type should be in DecodeV1.builtinTypeInfos
      DamlLf1.PrimType.ANY_EXCEPTION,
      DamlLf1.PrimType.GENERAL_ERROR,
      DamlLf1.PrimType.ARITHMETIC_ERROR,
      DamlLf1.PrimType.CONTRACT_ERROR,
    ) ++
      DecodeV1.builtinTypeInfos.map(_.proto)) shouldBe
      DamlLf1.PrimType.values().toSet

  }

  "The entries of builtinFunctionInfos correspond to Protobuf DamlLf1.BuiltinFunction" in {

    (Set(DamlLf1.BuiltinFunction.UNRECOGNIZED) ++ DecodeV1.builtinFunctionInfos.map(_.proto)) shouldBe
      DamlLf1.BuiltinFunction.values().toSet
  }

  private[this] val dummyModuleStr = "dummyModule"
  private[this] val dummyModuleName = DottedName.assertFromString(dummyModuleStr)

  import VersionTimeline.Implicits._

  private[this] val lfVersions =
    List(LV.Minor.Stable("6"), LV.Minor.Stable("7"), LV.Minor.Stable("8"), LV.Minor.Dev)
      .map(LV(LV.Major.V1, _))

  private[this] def forEveryVersionSuchThat[U](cond: LV => Boolean)(f: LV => U): Unit =
    lfVersions.foreach { version =>
      if (cond(version)) f(version)
      ()
    }

  private[this] def forEveryVersion[U]: (LV => U) => Unit =
    forEveryVersionSuchThat(_ => true)

  private[this] def forEveryVersionBefore[U](maxVersion: LV): (LV => U) => Unit =
    forEveryVersionSuchThat(v => v precedes maxVersion)

  private[this] def forEveryVersionAtOrAfter[U](maxVersion: LV): (LV => U) => Unit =
    forEveryVersionSuchThat(v => !(v precedes maxVersion))

  private def moduleDecoder(
      version: LV,
      stringTable: ImmArraySeq[String] = ImmArraySeq.empty,
      dottedNameTable: ImmArraySeq[DottedName] = ImmArraySeq.empty,
  ) = {
    new DecodeV1(version.minor).Env(
      Ref.PackageId.assertFromString("noPkgId"),
      stringTable,
      dottedNameTable,
      IndexedSeq(),
      None,
      Some(dummyModuleName),
      onlySerializableDataDefs = false
    )
  }

  "decodeKind" should {

    "reject nat kind if lf version < 1.7" in {

      val input = DamlLf1.Kind.newBuilder().setNat(DamlLf1.Unit.newBuilder()).build()

      forEveryVersionBefore(LV.Features.numeric) { version =>
        an[ParseError] shouldBe thrownBy(moduleDecoder(version).decodeKind(input))
      }
    }

    "accept nat kind if lf version >= 1.7" in {
      val input = DamlLf1.Kind.newBuilder().setNat(DamlLf1.Unit.newBuilder()).build()
      forEveryVersionAtOrAfter(LV.Features.numeric) { version =>
        moduleDecoder(version).decodeKind(input) shouldBe Ast.KNat
      }
    }
  }

  "decodeType" should {

    import DamlLf1.PrimType._

    def buildNat(i: Long) = DamlLf1.Type.newBuilder().setNat(i).build()

    val validNatTypes = List(0, 1, 2, 5, 11, 35, 36, 37)
    val invalidNatTypes = List(Long.MinValue, -100, -2, -1, 38, 39, 200, Long.MaxValue)

    "reject nat type if lf version < 1.7" in {

      val testCases =
        Table("proto nat type", (validNatTypes.map(_.toLong) ++ invalidNatTypes).map(buildNat): _*)

      forEveryVersionBefore(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(testCases) { natType =>
          an[ParseError] shouldBe thrownBy(decoder.decodeType(natType))
        }
      }
    }

    "accept only valid nat types if lf version >= 1.7" in {
      val positiveTestCases =
        Table("proto nat type" -> "nat", validNatTypes.map(v => buildNat(v.toLong) -> v): _*)
      val negativeTestCases = Table("proto nat type", invalidNatTypes.map(buildNat): _*)

      forEveryVersionAtOrAfter(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(positiveTestCases) { (natType, nat) =>
          decoder.uncheckedDecodeType(natType) shouldBe Ast.TNat(Numeric.Scale.assertFromInt(nat))
        }
        forEvery(negativeTestCases) { natType =>
          an[ParseError] shouldBe thrownBy(decoder.uncheckedDecodeType(natType))
        }
      }
    }

    def buildPrimType(primType: DamlLf1.PrimType, args: DamlLf1.Type*) =
      DamlLf1.Type
        .newBuilder()
        .setPrim(DamlLf1.Type.Prim.newBuilder().setPrim(primType).addAllArgs(args.asJava))
        .build()

    val decimalTestCases = Table(
      "input" -> "expected output",
      buildPrimType(DECIMAL) ->
        TNumeric(Ast.TNat(Decimal.scale)),
      buildPrimType(DECIMAL, buildPrimType(TEXT)) ->
        Ast.TApp(TNumeric(Ast.TNat(Decimal.scale)), TText),
      buildPrimType(ARROW, buildPrimType(TEXT), buildPrimType(DECIMAL)) ->
        TFun(TText, TNumeric(Ast.TNat(Decimal.scale))),
    )

    val numericTestCases = Table(
      "input" -> "expected output",
      buildPrimType(NUMERIC) ->
        TNumeric.cons,
      buildPrimType(NUMERIC, DamlLf1.Type.newBuilder().setNat(Decimal.scale.toLong).build()) ->
        TNumeric(Ast.TNat(Decimal.scale)),
      buildPrimType(NUMERIC, buildPrimType(TEXT)) ->
        Ast.TApp(TNumeric.cons, TText),
      buildPrimType(ARROW, buildPrimType(TEXT), buildPrimType(NUMERIC)) ->
        TFun(TText, TNumeric.cons),
    )

    "translate TDecimal to TApp(TNumeric, TNat(10))" in {
      forEveryVersionBefore(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(decimalTestCases) { (input, expectedOutput) =>
          decoder.decodeType(input) shouldBe expectedOutput
        }
      }
    }

    "reject Numeric types if version < 1.7" in {
      forEveryVersionBefore(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(numericTestCases) { (input, _) =>
          a[ParseError] shouldBe thrownBy(decoder.decodeType(input))
        }
      }
    }

    "translate TNumeric as is if version >= 1.7" in {
      forEveryVersionAtOrAfter(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(numericTestCases) { (input, expectedOutput) =>
          decoder.uncheckedDecodeType(input) shouldBe expectedOutput
        }
      }
    }

    "reject Decimal types if version >= 1.7" in {
      forEveryVersionAtOrAfter(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(decimalTestCases) { (input, _) =>
          a[ParseError] shouldBe thrownBy(decoder.decodeType(input))
        }
      }
    }

    "reject Any if version < 1.7" in {
      forEveryVersionBefore(LV.Features.anyType) { version =>
        val decoder = moduleDecoder(version)
        a[ParseError] shouldBe thrownBy(decoder.decodeType(buildPrimType(ANY)))
      }
    }

    "accept Any if 1.7 <= version >= 1.dev and " in {
      forEveryVersionAtOrAfter(LV.Features.anyType) { version =>
        val decoder = moduleDecoder(version)
        decoder.uncheckedDecodeType(buildPrimType(ANY)) shouldBe TAny
      }
    }

    "reject Struct with duplicate field names" in {
      val negativeTestCases =
        Table("field names", List("a", "b", "c"))
      val positiveTestCases =
        Table("field names", List("a", "a"), List("a", "b", "c", "a"), List("a", "b", "c", "b"))

      val unit = DamlLf1.Type
        .newBuilder()
        .setPrim(DamlLf1.Type.Prim.newBuilder().setPrim(DamlLf1.PrimType.UNIT))
        .build

      def fieldWithUnitWithoutInterning(s: String) =
        DamlLf1.FieldWithType.newBuilder().setFieldStr(s).setType(unit)

      def buildTStructWithoutInterning(fields: Seq[String]) =
        DamlLf1.Type
          .newBuilder()
          .setStruct(
            fields.foldLeft(DamlLf1.Type.Struct.newBuilder())((builder, name) =>
              builder.addFields(fieldWithUnitWithoutInterning(name)))
          )
          .build()

      val stringTable = ImmArraySeq("a", "b", "c")
      val stringIdx = stringTable.zipWithIndex.toMap

      def fieldWithUnitWithInterning(s: String) =
        DamlLf1.FieldWithType.newBuilder().setFieldInternedStr(stringIdx(s)).setType(unit)

      def buildTStructWithInterning(fields: Seq[String]) =
        DamlLf1.Type
          .newBuilder()
          .setStruct(
            fields.foldLeft(DamlLf1.Type.Struct.newBuilder())((builder, name) =>
              builder.addFields(fieldWithUnitWithInterning(name)))
          )
          .build()

      forEveryVersionBefore(LV.Features.internedStrings) { version =>
        val decoder = moduleDecoder(version)
        forEvery(negativeTestCases) { fieldNames =>
          decoder.decodeType(buildTStructWithoutInterning(fieldNames))
        }
        forEvery(positiveTestCases) { fieldNames =>
          a[ParseError] shouldBe thrownBy(
            decoder.decodeType(buildTStructWithoutInterning(fieldNames)))
        }
      }

      forEveryVersionAtOrAfter(LV.Features.internedStrings) { version =>
        val decoder = moduleDecoder(version, stringTable)
        forEvery(negativeTestCases) { fieldNames =>
          decoder.uncheckedDecodeType(buildTStructWithInterning(fieldNames))
        }
        forEvery(positiveTestCases) { fieldNames =>
          a[ParseError] shouldBe thrownBy(
            decoder.uncheckedDecodeType(buildTStructWithInterning(fieldNames)))
        }
      }
    }

    "reject non interned type for LF >= 1.dev" in {

      val stringTable = ImmArraySeq("pkgId", "x")
      val dottedNameTable = ImmArraySeq("Mod", "T", "S").map(DottedName.assertFromString)

      val unit = DamlLf1.Unit.newBuilder().build()
      val pkgRef = DamlLf1.PackageRef.newBuilder().setSelf(unit).build
      val modRef =
        DamlLf1.ModuleRef.newBuilder().setPackageRef(pkgRef).setModuleNameInternedDname(0).build()
      val tyConName = DamlLf1.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(1)
      val tySynName = DamlLf1.TypeSynName.newBuilder().setModule(modRef).setNameInternedDname(2)

      def newBuilder = DamlLf1.Type.newBuilder()

      val star = DamlLf1.Kind.newBuilder().setStar(unit).build
      val xWithStar =
        DamlLf1.TypeVarWithKind.newBuilder().setVarInternedStr(1).setKind(star).build()
      val typeVar = newBuilder.setVar(DamlLf1.Type.Var.newBuilder().setVarInternedStr(0)).build()
      val typeBool =
        newBuilder.setPrim(DamlLf1.Type.Prim.newBuilder().setPrim(DamlLf1.PrimType.BOOL)).build()
      val xWithBool =
        DamlLf1.FieldWithType.newBuilder.setFieldInternedStr(1).setType(typeBool).build()

      val testCases = Table[DamlLf1.Type](
        "type",
        typeVar,
        newBuilder.setNat(10).build(),
        newBuilder.setSyn(DamlLf1.Type.Syn.newBuilder().setTysyn(tySynName)).build(),
        newBuilder.setCon(DamlLf1.Type.Con.newBuilder().setTycon(tyConName)).build(),
        typeBool,
        newBuilder
          .setForall(DamlLf1.Type.Forall.newBuilder().addVars(xWithStar).setBody(typeVar))
          .build(),
        newBuilder.setStruct(DamlLf1.Type.Struct.newBuilder().addFields(xWithBool)).build(),
      )

      forEveryVersionAtOrAfter(LV.Features.internedTypes) { version =>
        val decoder = moduleDecoder(version, stringTable, dottedNameTable)
        forEvery(testCases)(proto => an[ParseError] shouldBe thrownBy(decoder.decodeType(proto)))
      }
    }

  }

  "decodeExpr" should {

    def toProtoExpr(b: DamlLf1.BuiltinFunction) =
      DamlLf1.Expr.newBuilder().setBuiltin(b).build()

    def toDecimalProto(s: String): DamlLf1.Expr =
      DamlLf1.Expr.newBuilder().setPrimLit(DamlLf1.PrimLit.newBuilder().setDecimalStr(s)).build()

    //def toNumericProto(s: String): DamlLf1.Expr =
    //  DamlLf1.Expr.newBuilder().setPrimLit(DamlLf1.PrimLit.newBuilder().setNumeric(s)).build()

    def toNumericProto(id: Int): DamlLf1.Expr =
      DamlLf1.Expr
        .newBuilder()
        .setPrimLit(DamlLf1.PrimLit.newBuilder().setNumericInternedStr(id))
        .build()

    val decimalBuiltinTestCases = Table[DamlLf1.BuiltinFunction, LanguageMinorVersion, Ast.Expr](
      ("decimal builtins", "minVersion", "expected output"),
      (
        DamlLf1.BuiltinFunction.ADD_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BAddNumeric), TDecimalScale)),
      (
        DamlLf1.BuiltinFunction.SUB_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BSubNumeric), TDecimalScale)),
      (
        DamlLf1.BuiltinFunction.MUL_DECIMAL,
        "6",
        Ast.ETyApp(
          Ast.ETyApp(Ast.ETyApp(Ast.EBuiltin(Ast.BMulNumeric), TDecimalScale), TDecimalScale),
          TDecimalScale)),
      (
        DamlLf1.BuiltinFunction.DIV_DECIMAL,
        "6",
        Ast.ETyApp(
          Ast.ETyApp(Ast.ETyApp(Ast.EBuiltin(Ast.BDivNumeric), TDecimalScale), TDecimalScale),
          TDecimalScale)),
      (
        DamlLf1.BuiltinFunction.ROUND_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BRoundNumeric), TDecimalScale)),
      (DamlLf1.BuiltinFunction.LEQ_DECIMAL, "6", Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TDecimal)),
      (DamlLf1.BuiltinFunction.LESS_DECIMAL, "6", Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TDecimal)),
      (
        DamlLf1.BuiltinFunction.GEQ_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TDecimal)),
      (
        DamlLf1.BuiltinFunction.GREATER_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BGreater), TDecimal)),
      (
        DamlLf1.BuiltinFunction.TO_TEXT_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BToTextNumeric), TDecimalScale)),
      (
        DamlLf1.BuiltinFunction.FROM_TEXT_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BFromTextNumeric), TDecimalScale)),
      (
        DamlLf1.BuiltinFunction.INT64_TO_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BInt64ToNumeric), TDecimalScale)),
      (
        DamlLf1.BuiltinFunction.DECIMAL_TO_INT64,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BNumericToInt64), TDecimalScale)),
      (DamlLf1.BuiltinFunction.EQUAL_DECIMAL, "6", Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TDecimal)),
    )

    val numericBuiltinTestCases = Table(
      "numeric builtins" -> "expected output",
      DamlLf1.BuiltinFunction.ADD_NUMERIC -> Ast.EBuiltin(Ast.BAddNumeric),
      DamlLf1.BuiltinFunction.SUB_NUMERIC -> Ast.EBuiltin(Ast.BSubNumeric),
      DamlLf1.BuiltinFunction.MUL_NUMERIC -> Ast.EBuiltin(Ast.BMulNumeric),
      DamlLf1.BuiltinFunction.DIV_NUMERIC -> Ast.EBuiltin(Ast.BDivNumeric),
      DamlLf1.BuiltinFunction.ROUND_NUMERIC -> Ast.EBuiltin(Ast.BRoundNumeric),
      DamlLf1.BuiltinFunction.TO_TEXT_NUMERIC -> Ast.EBuiltin(Ast.BToTextNumeric),
      DamlLf1.BuiltinFunction.FROM_TEXT_NUMERIC -> Ast.EBuiltin(Ast.BFromTextNumeric),
      DamlLf1.BuiltinFunction.INT64_TO_NUMERIC -> Ast.EBuiltin(Ast.BInt64ToNumeric),
      DamlLf1.BuiltinFunction.NUMERIC_TO_INT64 -> Ast.EBuiltin(Ast.BNumericToInt64),
    )

    val comparisonBuiltinCases = Table(
      "compare builtins" -> "expected output",
      DamlLf1.BuiltinFunction.EQUAL_INT64 -> Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TInt64),
      DamlLf1.BuiltinFunction.LEQ_INT64 -> Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TInt64),
      DamlLf1.BuiltinFunction.LESS_INT64 -> Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TInt64),
      DamlLf1.BuiltinFunction.GEQ_INT64 -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TInt64),
      DamlLf1.BuiltinFunction.GREATER_INT64 -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreater), TInt64),
      DamlLf1.BuiltinFunction.EQUAL_DATE -> Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TDate),
      DamlLf1.BuiltinFunction.LEQ_DATE -> Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TDate),
      DamlLf1.BuiltinFunction.LESS_DATE -> Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TDate),
      DamlLf1.BuiltinFunction.GEQ_DATE -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TDate),
      DamlLf1.BuiltinFunction.GREATER_DATE -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreater), TDate),
      DamlLf1.BuiltinFunction.EQUAL_TIMESTAMP -> Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TTimestamp),
      DamlLf1.BuiltinFunction.LEQ_TIMESTAMP -> Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TTimestamp),
      DamlLf1.BuiltinFunction.LESS_TIMESTAMP -> Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TTimestamp),
      DamlLf1.BuiltinFunction.GEQ_TIMESTAMP -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TTimestamp),
      DamlLf1.BuiltinFunction.GREATER_TIMESTAMP -> Ast
        .ETyApp(Ast.EBuiltin(Ast.BGreater), TTimestamp),
      DamlLf1.BuiltinFunction.EQUAL_TEXT -> Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TText),
      DamlLf1.BuiltinFunction.LEQ_TEXT -> Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TText),
      DamlLf1.BuiltinFunction.LESS_TEXT -> Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TText),
      DamlLf1.BuiltinFunction.GEQ_TEXT -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TText),
      DamlLf1.BuiltinFunction.GREATER_TEXT -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreater), TText),
      DamlLf1.BuiltinFunction.EQUAL_PARTY -> Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TParty),
      DamlLf1.BuiltinFunction.LEQ_PARTY -> Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TParty),
      DamlLf1.BuiltinFunction.LESS_PARTY -> Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TParty),
      DamlLf1.BuiltinFunction.GEQ_PARTY -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TParty),
      DamlLf1.BuiltinFunction.GREATER_PARTY -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreater), TParty),
    )

    val numericComparisonBuiltinCases = Table(
      "numeric comparison builtins" -> "expected output",
      DamlLf1.BuiltinFunction.EQUAL_NUMERIC -> Ast.EBuiltin(Ast.BEqualNumeric),
      DamlLf1.BuiltinFunction.LEQ_NUMERIC -> Ast.EBuiltin(Ast.BLessEqNumeric),
      DamlLf1.BuiltinFunction.LESS_NUMERIC -> Ast.EBuiltin(Ast.BLessNumeric),
      DamlLf1.BuiltinFunction.GEQ_NUMERIC -> Ast.EBuiltin(Ast.BGreaterEqNumeric),
      DamlLf1.BuiltinFunction.GREATER_NUMERIC -> Ast.EBuiltin(Ast.BGreaterNumeric),
    )

    val genericComparisonBuiltinCases = Table(
      "generic comparison builtins" -> "expected output",
      DamlLf1.BuiltinFunction.EQUAL -> Ast.EBuiltin(Ast.BEqual),
      DamlLf1.BuiltinFunction.LESS_EQ -> Ast.EBuiltin(Ast.BLessEq),
      DamlLf1.BuiltinFunction.LESS -> Ast.EBuiltin(Ast.BLess),
      DamlLf1.BuiltinFunction.GREATER_EQ -> Ast.EBuiltin(Ast.BGreaterEq),
      DamlLf1.BuiltinFunction.GREATER -> Ast.EBuiltin(Ast.BGreater),
    )

    val negativeBuiltinTestCases = Table(
      "other builtins" -> "expected output",
      // We do not need to test all other builtin
      DamlLf1.BuiltinFunction.ADD_INT64 -> Ast.EBuiltin(Ast.BAddInt64),
      DamlLf1.BuiltinFunction.APPEND_TEXT -> Ast.EBuiltin(Ast.BAppendText)
    )

    val contractIdTextConversionCases = Table(
      "builtin" -> "expected output",
      DamlLf1.BuiltinFunction.TO_TEXT_CONTRACT_ID -> Ast.EBuiltin(Ast.BToTextContractId)
    )

    "translate non numeric/decimal builtin as is for any version" in {
      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(negativeBuiltinTestCases) { (proto, scala) =>
          decoder.decodeExpr(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    "transparently apply TNat(10) to Decimal builtins if version < 1.7" in {

      forEveryVersionBefore(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)

        forEvery(decimalBuiltinTestCases) { (proto, version, scala) =>
          if (LV.Major.V1.minorVersionOrdering.gteq(version, version))
            decoder.decodeExpr(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    "reject Numeric builtins if version < 1.7" in {

      forEveryVersionBefore(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)

        forEvery(numericBuiltinTestCases) { (proto, _) =>
          an[ParseError] shouldBe thrownBy(decoder.decodeExpr(toProtoExpr(proto), "test"))
        }
      }
    }

    "translate Numeric builtins as is if version >= 1.7" in {

      forEveryVersionAtOrAfter(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)

        forEvery(numericBuiltinTestCases) { (proto, scala) =>
          decoder.decodeExpr(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    "translate numeric comparison builtins as is if version >= 1.7" in {

      val v1_7 = LV.Minor.Stable("7")

      forEveryVersionSuchThat(version =>
        !(version precedes LV.Features.numeric) & (version precedes LV.Features.genComparison)) {
        version =>
          val decoder = moduleDecoder(version)

          forEvery(numericComparisonBuiltinCases) { (proto, scala) =>
            if (proto != DamlLf1.BuiltinFunction.EQUAL_NUMERIC || version.minor == v1_7)
              decoder.decodeExpr(toProtoExpr(proto), "test") shouldBe scala
          }
      }
    }

    "reject Decimal builtins if version >= 1.7" in {

      forEveryVersionAtOrAfter(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)

        forEvery(decimalBuiltinTestCases) { (proto, _, _) =>
          an[ParseError] shouldBe thrownBy(decoder.decodeExpr(toProtoExpr(proto), "test"))
        }
      }
    }

    "parse properly decimal literal" in {

      val testCases =
        Table(
          "string",
          "9999999999999999999999999999.9999999999",
          "0000000000000000000000000000.0000000000",
          "0.0",
          "-0.0",
          "42",
          "3.1415",
          "+1.0",
          "-9999999999999999999999999999.9999999999"
        )

      forEveryVersionBefore(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(testCases) { string =>
          decoder.decodeExpr(toDecimalProto(string), "test") match {
            case Ast.EPrimLit(Ast.PLNumeric(num)) =>
              num shouldBe new BigDecimal(string).setScale(10)
            case _ =>
              throw new Error("")
          }
        }
      }
    }

    "reject improper decimal literal" in {

      val testCases =
        Table(
          "string",
          "10000000000000000000000000000.0000000000",
          "1000000000000000000000000000000",
          "0.00000000001",
          "0.0.0",
          "0.",
          "+-0.0",
        )

      forEveryVersionBefore(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(testCases) { string =>
          a[ParseError] shouldBe thrownBy(decoder.decodeExpr(toDecimalProto(string), "test"))
        }
      }
    }

    "reject numeric literal if version < 1.7" in {

      val decoder = moduleDecoder(LV(LV.Major.V1, LV.Features.numeric.minor), ImmArraySeq("0.0"))
      decoder.decodeExpr(toNumericProto(0), "test")

      forEveryVersionBefore(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version, ImmArraySeq("0.0"))
        a[ParseError] shouldBe thrownBy(decoder.decodeExpr(toNumericProto(0), "test"))
      }
    }

    "parse properly numeric literals" in {

      val testCases =
        Table(
          "id" -> "string",
          0 -> "9999999999999999999999999999.9999999999",
          1 -> "0.0000000000",
          2 -> "1000000000000000000000000000000.",
          3 -> "99999999999999999999999999999999999999.",
          4 -> "-0.0",
          5 -> "0.",
          6 -> "3.1415",
          7 -> "-99999999999999999999.999999999999999999"
        )

      forEveryVersionAtOrAfter(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version, ImmArraySeq(testCases.map(_._2): _*))
        forEvery(testCases) { (id, string) =>
          decoder.decodeExpr(toNumericProto(id), "test") match {
            case Ast.EPrimLit(Ast.PLNumeric(num)) =>
              num shouldBe new BigDecimal(string)
            case _ =>
              throw new Error("")
          }
        }
      }
    }

    "reject improper numeric literals" in {

      val testCases =
        Table(
          "id" -> "string",
          1 -> "10000000000000000000000000000.0000000000",
          2 -> "-1000000000000000000000000000000000000000.",
          3 -> "0.000000000000000000000000000000000000001",
          4 -> "0000000000000000000000000000.0000000000",
          5 -> "0.0.0",
          6 -> "+0.0",
          7 -> "0"
        )

      forEveryVersionAtOrAfter(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version, ImmArraySeq("0." +: testCases.map(_._2): _*))
        forEvery(testCases) { (id, _) =>
          decoder.decodeExpr(toNumericProto(0), "test")
          a[ParseError] shouldBe thrownBy(decoder.decodeExpr(toNumericProto(id), "test"))
        }
      }
    }

    "reject numeric decimal if version >= 1.dev" in {

      forEveryVersionAtOrAfter(LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        a[ParseError] shouldBe thrownBy(decoder.decodeExpr(toDecimalProto("0.0"), "test"))
      }
    }

    "translate comparison builtins as is if version < 1.9" in {

      forEveryVersionBefore(LV.Features.genComparison) { version =>
        val decoder = moduleDecoder(version)

        forEvery(comparisonBuiltinCases) { (proto, scala) =>
          decoder.decodeExpr(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    "reject comparison builtins as is if version >= 1.9" in {

      forEveryVersionBefore(LV.Features.genComparison) { version =>
        val decoder = moduleDecoder(version)
        forEvery(genericComparisonBuiltinCases) { (proto, _) =>
          a[ParseError] shouldBe thrownBy(decoder.decodeExpr(toProtoExpr(proto), "test"))
        }
      }
    }

    "translate generic comparison builtins as is if version >= 1.9" in {
      forEveryVersionAtOrAfter(LV.Features.genComparison) { version =>
        val decoder = moduleDecoder(version)
        forEvery(genericComparisonBuiltinCases) { (proto, scala) =>
          decoder.decodeExpr(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    "translate generic comparison builtins as is if version < 1.9" in {
      forEveryVersionBefore(LV.Features.genComparison) { version =>
        val decoder = moduleDecoder(version)
        forEvery(genericComparisonBuiltinCases) { (proto, _) =>
          a[ParseError] shouldBe thrownBy(decoder.decodeExpr(toProtoExpr(proto), "test"))
        }
      }
    }

    "translate contract id text conversions as is if version >= 1.9" in {
      forEveryVersionAtOrAfter(LV.Features.contractIdTextConversions) { version =>
        val decoder = moduleDecoder(version)
        forEvery(contractIdTextConversionCases) { (proto, scala) =>
          decoder.decodeExpr(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    "reject contract id text conversions if version < 1.9" in {
      forEveryVersionBefore(LV.Features.contractIdTextConversions) { version =>
        val decoder = moduleDecoder(version)
        forEvery(contractIdTextConversionCases) { (proto, _) =>
          a[ParseError] shouldBe thrownBy(decoder.decodeExpr(toProtoExpr(proto), "test"))
        }
      }
    }
  }

  "decodeModuleRef" should {

    lazy val ((pkgId, dalfProto), majorVersion) = {
      val dalfFile =
        Files.newInputStream(Paths.get(rlocation("daml-lf/archive/DarReaderTest.dalf")))
      try Reader.readArchiveAndVersion(dalfFile)
      finally dalfFile.close()
    }

    lazy val extId = {
      val dalf1 = dalfProto.getDamlLf1
      val iix = dalf1
        .getModules(0)
        .getValuesList
        .asScala
        .collectFirst {
          case dv
              if dalf1.getInternedDottedNamesList
                .asScala(dv.getNameWithType.getNameInternedDname)
                .getSegmentsInternedStrList
                .asScala
                .lastOption
                .map(x => dalf1.getInternedStringsList.asScala(x)) contains "reverseCopy" =>
            val pr = dv.getExpr.getVal.getModule.getPackageRef
            pr.getSumCase shouldBe DamlLf1.PackageRef.SumCase.PACKAGE_ID_INTERNED_STR
            pr.getPackageIdInternedStr
        }
        .value
      dalf1.getInternedStringsList.asScala.lift(iix.toInt).value
    }

    "take a dalf with interned IDs" in {
      majorVersion should ===(LV.Major.V1)

      dalfProto.getMinor should !==("dev")

      extId should not be empty
      (extId: String) should !==(pkgId: String)
    }

    "decode resolving the interned package ID" in {
      val decoder = Decode.decoders(LV(majorVersion, dalfProto.getMinor))
      inside(
        decoder.decoder
          .decodePackage(pkgId, decoder.extract(dalfProto))
          .lookupDefinition(Ref.QualifiedName assertFromString "DarReaderTest:reverseCopy")) {
        case Right(
            Ast.DValue(_, _, Ast.ELocation(_, Ast.EVal(Ref.Identifier(resolvedExtId, _))), _)) =>
          (resolvedExtId: String) should ===(extId: String)
      }
    }
  }

  "decodePackageMetadata" should {
    "accept a valid package name and version" in {
      forEveryVersionAtOrAfter(LV.Features.packageMetadata) { version =>
        new DecodeV1(version.minor).decodePackageMetadata(
          DamlLf1.PackageMetadata
            .newBuilder()
            .setNameInternedStr(0)
            .setVersionInternedStr(1)
            .build(),
          ImmArraySeq("foobar", "0.0.0")) shouldBe Ast.PackageMetadata(
          Ref.PackageName.assertFromString("foobar"),
          Ref.PackageVersion.assertFromString("0.0.0"))
      }
    }

    "reject a package namewith space" in {
      forEveryVersionAtOrAfter(LV.Features.packageMetadata) { version =>
        a[ParseError] shouldBe thrownBy(
          new DecodeV1(version.minor).decodePackageMetadata(
            DamlLf1.PackageMetadata
              .newBuilder()
              .setNameInternedStr(0)
              .setVersionInternedStr(1)
              .build(),
            ImmArraySeq("foo bar", "0.0.0")))
      }
    }

    "reject a package version with leading zero" in {
      forEveryVersionAtOrAfter(LV.Features.packageMetadata) { version =>
        a[ParseError] shouldBe thrownBy(
          new DecodeV1(version.minor).decodePackageMetadata(
            DamlLf1.PackageMetadata
              .newBuilder()
              .setNameInternedStr(0)
              .setVersionInternedStr(1)
              .build(),
            ImmArraySeq("foobar", "01.0.0")))
      }
    }

    "reject a package version with a dash" in {
      forEveryVersionAtOrAfter(LV.Features.packageMetadata) { version =>
        a[ParseError] shouldBe thrownBy(
          new DecodeV1(version.minor).decodePackageMetadata(
            DamlLf1.PackageMetadata
              .newBuilder()
              .setNameInternedStr(0)
              .setVersionInternedStr(1)
              .build(),
            ImmArraySeq("foobar", "0.0.0-")))
      }
    }
  }

  "decodePackage" should {
    "reject PackageMetadata if lf version < 1.8" in {
      forEveryVersionBefore(LV.Features.packageMetadata) { version =>
        val decoder = new DecodeV1(version.minor)
        val pkgId = Ref.PackageId.assertFromString(
          "0000000000000000000000000000000000000000000000000000000000000000")
        val metadata =
          DamlLf1.PackageMetadata.newBuilder
            .setNameInternedStr(0)
            .setVersionInternedStr(1)
            .build()
        val pkg = DamlLf1.Package
          .newBuilder()
          .addInternedStrings("foobar")
          .addInternedStrings("0.0.0")
          .setMetadata(metadata)
          .build()
        a[ParseError] shouldBe thrownBy(decoder.decodePackage(pkgId, pkg, false))
      }
    }

    "require PackageMetadata to be present if lf version >= 1.8" in {
      forEveryVersionAtOrAfter(LV.Features.packageMetadata) { version =>
        val decoder = new DecodeV1(version.minor)
        val pkgId = Ref.PackageId.assertFromString(
          "0000000000000000000000000000000000000000000000000000000000000000")
        a[ParseError] shouldBe thrownBy(
          decoder.decodePackage(pkgId, DamlLf1.Package.newBuilder().build(), false))
      }
    }

    "decode PackageMetadata if lf version >= 1.8" in {
      forEveryVersionAtOrAfter(LV.Features.packageMetadata) { version =>
        val decoder = new DecodeV1(version.minor)
        val pkgId = Ref.PackageId.assertFromString(
          "0000000000000000000000000000000000000000000000000000000000000000")
        val metadata =
          DamlLf1.PackageMetadata.newBuilder
            .setNameInternedStr(0)
            .setVersionInternedStr(1)
            .build()
        val pkg = DamlLf1.Package
          .newBuilder()
          .addInternedStrings("foobar")
          .addInternedStrings("0.0.0")
          .setMetadata(metadata)
          .build()
        decoder.decodePackage(pkgId, pkg, false).metadata shouldBe Some(
          Ast.PackageMetadata(
            Ref.PackageName.assertFromString("foobar"),
            Ref.PackageVersion.assertFromString("0.0.0")))
      }
    }
  }

  "decodeInternedTypes" should {
    def pkgWithInternedTypes: DamlLf1.Package = {
      val typeNat1 = DamlLf1.Type.newBuilder().setNat(1).build()
      DamlLf1.Package
        .newBuilder()
        .addInternedTypes(typeNat1)
        .build()
    }

    "reject interned types if lf version < 1.dev" in {
      forEveryVersionBefore(LV.Features.internedTypes) { version =>
        val decoder = new DecodeV1(version.minor)
        val env = decoder.Env(
          Ref.PackageId.assertFromString("noPkgId"),
          ImmArraySeq.empty,
          ImmArraySeq.empty,
          IndexedSeq.empty,
          None,
          None,
          onlySerializableDataDefs = false
        )
        val parseError = the[ParseError] thrownBy decoder.decodeInternedTypes(
          env,
          pkgWithInternedTypes,
        )
        parseError.toString should include("interned types table is not supported")
      }
    }
  }
}
