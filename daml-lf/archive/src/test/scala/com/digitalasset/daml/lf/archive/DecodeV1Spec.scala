// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.math.BigDecimal
import java.nio.file.Paths
import com.daml.bazeltools.BazelRunfiles._
import com.daml.lf.data.{Decimal, Numeric, Ref}
import com.daml.lf.language.Util._
import com.daml.lf.language.{Ast, LanguageVersion => LV}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.daml_lf_dev.DamlLf1
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{Inside, OptionValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.Ordering.Implicits.infixOrderingOps
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class DecodeV1Spec
    extends AnyWordSpec
    with Matchers
    with Inside
    with OptionValues
    with ScalaCheckPropertyChecks {

  val unitTyp: DamlLf1.Type = DamlLf1.Type
    .newBuilder()
    .setPrim(DamlLf1.Type.Prim.newBuilder().setPrim(DamlLf1.PrimType.UNIT))
    .build()
  val boolTyp: DamlLf1.Type = DamlLf1.Type
    .newBuilder()
    .setPrim(DamlLf1.Type.Prim.newBuilder().setPrim(DamlLf1.PrimType.BOOL))
    .build()
  val textTyp: DamlLf1.Type = DamlLf1.Type
    .newBuilder()
    .setPrim(DamlLf1.Type.Prim.newBuilder().setPrim(DamlLf1.PrimType.TEXT))
    .build()

  val typeTable = ImmArraySeq(TUnit, TBool, TText)
  val unitTypInterned = DamlLf1.Type.newBuilder().setInterned(0).build()
  val boolTypInterned = DamlLf1.Type.newBuilder().setInterned(1).build()
  val textTypInterned = DamlLf1.Type.newBuilder().setInterned(2).build()

  val unitExpr: DamlLf1.Expr = DamlLf1.Expr
    .newBuilder()
    .setPrimCon(DamlLf1.PrimCon.CON_UNIT)
    .build()

  val falseExpr: DamlLf1.Expr = DamlLf1.Expr
    .newBuilder()
    .setPrimCon(DamlLf1.PrimCon.CON_FALSE)
    .build()

  "The entries of primTypeInfos correspond to Protobuf DamlLf1.PrimType" in {

    (Set(
      DamlLf1.PrimType.UNRECOGNIZED,
      DamlLf1.PrimType.DECIMAL,
    ) ++
      DecodeV1.builtinTypeInfos.map(_.proto)) shouldBe
      DamlLf1.PrimType.values().toSet

  }

  "The entries of builtinFunctionInfos correspond to Protobuf DamlLf1.BuiltinFunction" in {

    (Set(DamlLf1.BuiltinFunction.UNRECOGNIZED) ++ DecodeV1.builtinFunctionInfos.map(
      _.proto
    )) shouldBe
      DamlLf1.BuiltinFunction.values().toSet
  }

  private[this] val dummyModuleStr = "dummyModule"
  private[this] val dummyModuleName = Ref.DottedName.assertFromString(dummyModuleStr)

  private[this] val lfVersions = LV.All

  private[this] def forEveryVersionSuchThat[U](cond: LV => Boolean)(f: LV => U): Unit =
    lfVersions.foreach { version =>
      if (cond(version)) f(version)
      ()
    }

  private[this] def forEveryVersion[U]: (LV => U) => Unit =
    forEveryVersionSuchThat(_ => true)

  private def moduleDecoder(
      version: LV,
      stringTable: ImmArraySeq[String] = ImmArraySeq.empty,
      dottedNameTable: ImmArraySeq[Ref.DottedName] = ImmArraySeq.empty,
      typeTable: ImmArraySeq[Ast.Type] = ImmArraySeq.empty,
  ) = {
    new DecodeV1(version.minor).Env(
      Ref.PackageId.assertFromString("noPkgId"),
      stringTable,
      dottedNameTable,
      typeTable,
      None,
      Some(dummyModuleName),
      onlySerializableDataDefs = false,
    )
  }

  "decodeKind" should {

    "reject nat kind if lf version < 1.7" in {

      val input = DamlLf1.Kind.newBuilder().setNat(DamlLf1.Unit.newBuilder()).build()

      forEveryVersionSuchThat(_ < LV.Features.numeric) { version =>
        an[Error.Parsing] shouldBe thrownBy(moduleDecoder(version).decodeKindForTest(input))
      }
    }

    "accept nat kind if lf version >= 1.7" in {
      val input = DamlLf1.Kind.newBuilder().setNat(DamlLf1.Unit.newBuilder()).build()
      forEveryVersionSuchThat(_ >= LV.Features.numeric) { version =>
        moduleDecoder(version).decodeKindForTest(input) shouldBe Ast.KNat
      }
    }
  }

  "uncheckedDecodeType" should {

    import DamlLf1.PrimType._

    def buildNat(i: Long) = DamlLf1.Type.newBuilder().setNat(i).build()

    val validNatTypes = List(0, 1, 2, 5, 11, 35, 36, 37)
    val invalidNatTypes = List(Long.MinValue, -100, -2, -1, 38, 39, 200, Long.MaxValue)

    "reject nat type if lf version < 1.7" in {

      val testCases =
        Table("proto nat type", (validNatTypes.map(_.toLong) ++ invalidNatTypes).map(buildNat): _*)

      forEveryVersionSuchThat(_ < LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(testCases) { natType =>
          an[Error.Parsing] shouldBe thrownBy(decoder.uncheckedDecodeTypeForTest(natType))
        }
      }
    }

    "accept only valid nat types if lf version >= 1.7" in {
      val positiveTestCases =
        Table("proto nat type" -> "nat", validNatTypes.map(v => buildNat(v.toLong) -> v): _*)
      val negativeTestCases = Table("proto nat type", invalidNatTypes.map(buildNat): _*)

      forEveryVersionSuchThat(_ >= LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(positiveTestCases) { (natType, nat) =>
          decoder.uncheckedDecodeTypeForTest(natType) shouldBe Ast.TNat(
            Numeric.Scale.assertFromInt(nat)
          )
        }
        forEvery(negativeTestCases) { natType =>
          an[Error.Parsing] shouldBe thrownBy(decoder.uncheckedDecodeTypeForTest(natType))
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
      forEveryVersionSuchThat(_ < LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(decimalTestCases) { (input, expectedOutput) =>
          decoder.uncheckedDecodeTypeForTest(input) shouldBe expectedOutput
        }
      }
    }

    "reject Numeric types if version < 1.7" in {
      forEveryVersionSuchThat(_ < LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(numericTestCases) { (input, _) =>
          an[Error.Parsing] shouldBe thrownBy(decoder.uncheckedDecodeTypeForTest(input))
        }
      }
    }

    "translate TNumeric as is if version >= 1.7" in {
      forEveryVersionSuchThat(_ >= LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(numericTestCases) { (input, expectedOutput) =>
          decoder.uncheckedDecodeTypeForTest(input) shouldBe expectedOutput
        }
      }
    }

    "reject Decimal types if version >= 1.7" in {
      forEveryVersionSuchThat(_ >= LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(decimalTestCases) { (input, _) =>
          an[Error.Parsing] shouldBe thrownBy(decoder.uncheckedDecodeTypeForTest(input))
        }
      }
    }

    "reject Any if version < 1.7" in {
      forEveryVersionSuchThat(_ < LV.Features.anyType) { version =>
        val decoder = moduleDecoder(version)
        an[Error.Parsing] shouldBe thrownBy(decoder.uncheckedDecodeTypeForTest(buildPrimType(ANY)))
      }
    }

    "accept Any if version >= 1.7" in {
      forEveryVersionSuchThat(_ >= LV.Features.anyType) { version =>
        val decoder = moduleDecoder(version)
        decoder.uncheckedDecodeTypeForTest(buildPrimType(ANY)) shouldBe TAny
      }
    }

    s"reject BigNumeric and RoundingMode if version < ${LV.Features.bigNumeric}" in {
      forEveryVersionSuchThat(_ < LV.Features.bigNumeric) { version =>
        val decoder = moduleDecoder(version)
        an[Error.Parsing] shouldBe thrownBy(
          decoder.uncheckedDecodeTypeForTest(buildPrimType(BIGNUMERIC))
        )
        an[Error.Parsing] shouldBe thrownBy(
          decoder.uncheckedDecodeTypeForTest(buildPrimType(ROUNDING_MODE))
        )
      }
    }

    s"accept BigNumeric and RoundingMode if version >= ${LV.Features.bigNumeric}" in {
      forEveryVersionSuchThat(_ >= LV.Features.bigNumeric) { version =>
        val decoder = moduleDecoder(version)
        decoder.uncheckedDecodeTypeForTest(buildPrimType(BIGNUMERIC)) shouldBe TBigNumeric
        decoder.uncheckedDecodeTypeForTest(buildPrimType(ROUNDING_MODE)) shouldBe TRoundingMode
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
              builder.addFields(fieldWithUnitWithoutInterning(name))
            )
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
              builder.addFields(fieldWithUnitWithInterning(name))
            )
          )
          .build()

      forEveryVersionSuchThat(_ < LV.Features.internedStrings) { version =>
        val decoder = moduleDecoder(version)
        forEvery(negativeTestCases) { fieldNames =>
          decoder.uncheckedDecodeTypeForTest(buildTStructWithoutInterning(fieldNames))
        }
        forEvery(positiveTestCases) { fieldNames =>
          an[Error.Parsing] shouldBe thrownBy(
            decoder.uncheckedDecodeTypeForTest(buildTStructWithoutInterning(fieldNames))
          )
        }
      }

      forEveryVersionSuchThat(_ >= LV.Features.internedStrings) { version =>
        val decoder = moduleDecoder(version, stringTable)
        forEvery(negativeTestCases) { fieldNames =>
          decoder.uncheckedDecodeTypeForTest(buildTStructWithInterning(fieldNames))
        }
        forEvery(positiveTestCases) { fieldNames =>
          an[Error.Parsing] shouldBe thrownBy(
            decoder.uncheckedDecodeTypeForTest(buildTStructWithInterning(fieldNames))
          )
        }
      }
    }

    s"translate exception types iff version >= ${LV.Features.exceptions}" in {
      val exceptionBuiltinTypes = Table(
        "builtin types",
        DamlLf1.PrimType.ANY_EXCEPTION -> Ast.BTAnyException,
      )

      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(exceptionBuiltinTypes) { case (proto, bType) =>
          val result = Try(decoder.uncheckedDecodeTypeForTest(buildPrimType(proto)))

          if (version >= LV.Features.exceptions)
            result shouldBe Success(Ast.TBuiltin(bType))
          else
            inside(result) { case Failure(error) =>
              error shouldBe an[Error.Parsing]
            }
        }
      }
    }
  }

  "decodeType" should {

    "reject non interned type for LF >= 1.dev" in {

      val stringTable = ImmArraySeq("pkgId", "x")
      val dottedNameTable = ImmArraySeq("Mod", "T", "S").map(Ref.DottedName.assertFromString)

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

      forEveryVersionSuchThat(_ >= LV.Features.internedTypes) { version =>
        val decoder = moduleDecoder(version, stringTable, dottedNameTable)
        forEvery(testCases)(proto =>
          an[Error.Parsing] shouldBe thrownBy(decoder.decodeTypeForTest(proto))
        )
      }
    }

  }

  "decodeExpr" should {

    def toProtoExpr(b: DamlLf1.BuiltinFunction) =
      DamlLf1.Expr.newBuilder().setBuiltin(b).build()

    def toDecimalProto(s: String): DamlLf1.Expr =
      DamlLf1.Expr.newBuilder().setPrimLit(DamlLf1.PrimLit.newBuilder().setDecimalStr(s)).build()

    // def toNumericProto(s: String): DamlLf1.Expr =
    //  DamlLf1.Expr.newBuilder().setPrimLit(DamlLf1.PrimLit.newBuilder().setNumeric(s)).build()

    def toNumericProto(id: Int): DamlLf1.Expr =
      DamlLf1.Expr
        .newBuilder()
        .setPrimLit(DamlLf1.PrimLit.newBuilder().setNumericInternedStr(id))
        .build()

    val decimalBuiltinTestCases = Table[DamlLf1.BuiltinFunction, String, Ast.Expr](
      ("decimal builtins", "minVersion", "expected output"),
      (
        DamlLf1.BuiltinFunction.ADD_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BAddNumeric), TDecimalScale),
      ),
      (
        DamlLf1.BuiltinFunction.SUB_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BSubNumeric), TDecimalScale),
      ),
      (
        DamlLf1.BuiltinFunction.MUL_DECIMAL,
        "6",
        Ast.ETyApp(
          Ast.ETyApp(Ast.ETyApp(Ast.EBuiltin(Ast.BMulNumeric), TDecimalScale), TDecimalScale),
          TDecimalScale,
        ),
      ),
      (
        DamlLf1.BuiltinFunction.DIV_DECIMAL,
        "6",
        Ast.ETyApp(
          Ast.ETyApp(Ast.ETyApp(Ast.EBuiltin(Ast.BDivNumeric), TDecimalScale), TDecimalScale),
          TDecimalScale,
        ),
      ),
      (
        DamlLf1.BuiltinFunction.ROUND_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BRoundNumeric), TDecimalScale),
      ),
      (DamlLf1.BuiltinFunction.LEQ_DECIMAL, "6", Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TDecimal)),
      (DamlLf1.BuiltinFunction.LESS_DECIMAL, "6", Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TDecimal)),
      (
        DamlLf1.BuiltinFunction.GEQ_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TDecimal),
      ),
      (
        DamlLf1.BuiltinFunction.GREATER_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BGreater), TDecimal),
      ),
      (
        DamlLf1.BuiltinFunction.DECIMAL_TO_TEXT,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BNumericToText), TDecimalScale),
      ),
      (
        DamlLf1.BuiltinFunction.TEXT_TO_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BTextToNumeric), TDecimalScale),
      ),
      (
        DamlLf1.BuiltinFunction.INT64_TO_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BInt64ToNumeric), TDecimalScale),
      ),
      (
        DamlLf1.BuiltinFunction.DECIMAL_TO_INT64,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BNumericToInt64), TDecimalScale),
      ),
      (DamlLf1.BuiltinFunction.EQUAL_DECIMAL, "6", Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TDecimal)),
    )

    val numericBuiltinTestCases = Table(
      "numeric builtins" -> "expected output",
      DamlLf1.BuiltinFunction.ADD_NUMERIC -> Ast.EBuiltin(Ast.BAddNumeric),
      DamlLf1.BuiltinFunction.SUB_NUMERIC -> Ast.EBuiltin(Ast.BSubNumeric),
      DamlLf1.BuiltinFunction.MUL_NUMERIC -> Ast.EBuiltin(Ast.BMulNumeric),
      DamlLf1.BuiltinFunction.DIV_NUMERIC -> Ast.EBuiltin(Ast.BDivNumeric),
      DamlLf1.BuiltinFunction.ROUND_NUMERIC -> Ast.EBuiltin(Ast.BRoundNumeric),
      DamlLf1.BuiltinFunction.NUMERIC_TO_TEXT -> Ast.EBuiltin(Ast.BNumericToText),
      DamlLf1.BuiltinFunction.TEXT_TO_NUMERIC -> Ast.EBuiltin(Ast.BTextToNumeric),
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
      DamlLf1.BuiltinFunction.APPEND_TEXT -> Ast.EBuiltin(Ast.BAppendText),
    )

    val contractIdTextConversionCases = Table(
      "builtin" -> "expected output",
      DamlLf1.BuiltinFunction.CONTRACT_ID_TO_TEXT -> Ast.EBuiltin(Ast.BContractIdToText),
    )

    "translate non numeric/decimal builtin as is for any version" in {
      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(negativeBuiltinTestCases) { (proto, scala) =>
          decoder.decodeExprForTest(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    "transparently apply TNat(10) to Decimal builtins if version < 1.7" in {

      forEveryVersionSuchThat(_ < LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)

        forEvery(decimalBuiltinTestCases) { (proto, versionId, scala) =>
          if (LV.Major.V1.minorVersionOrdering.gteq(LV.Minor(versionId), version.minor))
            decoder.decodeExprForTest(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    "reject Numeric builtins if version < 1.7" in {

      forEveryVersionSuchThat(_ < LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)

        forEvery(numericBuiltinTestCases) { (proto, _) =>
          an[Error.Parsing] shouldBe thrownBy(decoder.decodeExprForTest(toProtoExpr(proto), "test"))
        }
      }
    }

    "translate Numeric builtins as is if version >= 1.7" in {

      forEveryVersionSuchThat(_ >= LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)

        forEvery(numericBuiltinTestCases) { (proto, scala) =>
          decoder.decodeExprForTest(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    "translate numeric comparison builtins as is if version >= 1.7" in {

      forEveryVersionSuchThat(version =>
        LV.Features.numeric <= version && version < LV.Features.genComparison
      ) { version =>
        val decoder = moduleDecoder(version)

        forEvery(numericComparisonBuiltinCases) { (proto, scala) =>
          if (proto != DamlLf1.BuiltinFunction.EQUAL_NUMERIC || version == LV.v1_7)
            decoder.decodeExprForTest(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    "reject Decimal builtins if version >= 1.7" in {

      forEveryVersionSuchThat(_ >= LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)

        forEvery(decimalBuiltinTestCases) { (proto, _, _) =>
          an[Error.Parsing] shouldBe thrownBy(decoder.decodeExprForTest(toProtoExpr(proto), "test"))
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
          "-9999999999999999999999999999.9999999999",
        )

      forEveryVersionSuchThat(_ < LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(testCases) { string =>
          inside(decoder.decodeExprForTest(toDecimalProto(string), "test")) {
            case Ast.EPrimLit(Ast.PLNumeric(num)) =>
              num shouldBe new BigDecimal(string).setScale(10)
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

      forEveryVersionSuchThat(_ < LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(testCases) { string =>
          an[Error.Parsing] shouldBe thrownBy(
            decoder.decodeExprForTest(toDecimalProto(string), "test")
          )
        }
      }
    }

    "reject numeric literal if version < 1.7" in {

      val decoder = moduleDecoder(LV(LV.Major.V1, LV.Features.numeric.minor), ImmArraySeq("0.0"))
      decoder.decodeExprForTest(toNumericProto(0), "test")

      forEveryVersionSuchThat(_ < LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version, ImmArraySeq("0.0"))
        an[Error.Parsing] shouldBe thrownBy(decoder.decodeExprForTest(toNumericProto(0), "test"))
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
          7 -> "-99999999999999999999.999999999999999999",
        )

      forEveryVersionSuchThat(_ >= LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version, ImmArraySeq(testCases.map(_._2): _*))
        forEvery(testCases) { (id, string) =>
          inside(decoder.decodeExprForTest(toNumericProto(id), "test")) {
            case Ast.EPrimLit(Ast.PLNumeric(num)) =>
              num shouldBe new BigDecimal(string)
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
          7 -> "0",
        )

      forEveryVersionSuchThat(_ >= LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version, ImmArraySeq("0." +: testCases.map(_._2): _*))
        forEvery(testCases) { (id, _) =>
          decoder.decodeExprForTest(toNumericProto(0), "test")
          an[Error.Parsing] shouldBe thrownBy(decoder.decodeExprForTest(toNumericProto(id), "test"))
        }
      }
    }

    s"reject numeric decimal if version >= ${LV.Features.numeric}" in {

      forEveryVersionSuchThat(_ >= LV.Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        an[Error.Parsing] shouldBe thrownBy(
          decoder.decodeExprForTest(toDecimalProto("0.0"), "test")
        )
      }
    }

    s"translate comparison builtins as is if version < ${LV.Features.genComparison}" in {

      forEveryVersionSuchThat(_ < LV.Features.genComparison) { version =>
        val decoder = moduleDecoder(version)

        forEvery(comparisonBuiltinCases) { (proto, scala) =>
          decoder.decodeExprForTest(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    s"reject comparison builtins as is if version >= ${LV.Features.genComparison}" in {

      forEveryVersionSuchThat(_ >= LV.Features.genComparison) { version =>
        val decoder = moduleDecoder(version)
        forEvery(comparisonBuiltinCases) { (proto, _) =>
          an[Error.Parsing] shouldBe thrownBy(decoder.decodeExprForTest(toProtoExpr(proto), "test"))
        }
      }
    }

    s"translate generic comparison builtins as is if version >= ${LV.Features.genComparison}" in {
      forEveryVersionSuchThat(_ >= LV.Features.genComparison) { version =>
        val decoder = moduleDecoder(version)
        forEvery(genericComparisonBuiltinCases) { (proto, scala) =>
          decoder.decodeExprForTest(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    s"translate generic comparison builtins as is if version < ${LV.Features.genComparison}" in {
      forEveryVersionSuchThat(_ < LV.Features.genComparison) { version =>
        val decoder = moduleDecoder(version)
        forEvery(genericComparisonBuiltinCases) { (proto, _) =>
          an[Error.Parsing] shouldBe thrownBy(decoder.decodeExprForTest(toProtoExpr(proto), "test"))
        }
      }
    }

    s"translate contract id text conversions as is if version >= ${LV.Features.contractIdTextConversions}" in {
      forEveryVersionSuchThat(_ >= LV.Features.contractIdTextConversions) { version =>
        val decoder = moduleDecoder(version)
        forEvery(contractIdTextConversionCases) { (proto, scala) =>
          decoder.decodeExprForTest(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    s"reject contract id text conversions if version < ${LV.Features.contractIdTextConversions}" in {
      forEveryVersionSuchThat(_ < LV.Features.contractIdTextConversions) { version =>
        val decoder = moduleDecoder(version)
        forEvery(contractIdTextConversionCases) { (proto, _) =>
          an[Error.Parsing] shouldBe thrownBy(decoder.decodeExprForTest(toProtoExpr(proto), "test"))
        }
      }
    }

    s"translate BigNumeric builtins iff version >= ${LV.Features.bigNumeric}" in {
      val exceptionBuiltinCases = Table(
        "exception builtins" -> "expected output",
        DamlLf1.BuiltinFunction.SCALE_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BScaleBigNumeric),
        DamlLf1.BuiltinFunction.PRECISION_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BPrecisionBigNumeric),
        DamlLf1.BuiltinFunction.ADD_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BAddBigNumeric),
        DamlLf1.BuiltinFunction.SUB_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BSubBigNumeric),
        DamlLf1.BuiltinFunction.MUL_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BMulBigNumeric),
        DamlLf1.BuiltinFunction.DIV_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BDivBigNumeric),
        DamlLf1.BuiltinFunction.NUMERIC_TO_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BNumericToBigNumeric),
        DamlLf1.BuiltinFunction.BIGNUMERIC_TO_NUMERIC ->
          Ast.EBuiltin(Ast.BBigNumericToNumeric),
        DamlLf1.BuiltinFunction.BIGNUMERIC_TO_TEXT ->
          Ast.EBuiltin(Ast.BBigNumericToText),
      )

      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(exceptionBuiltinCases) { (proto, scala) =>
          val result = Try(decoder.decodeExprForTest(toProtoExpr(proto), "test"))

          if (version >= LV.Features.bigNumeric)
            result shouldBe Success(scala)
          else
            inside(result) { case Failure(error) => error shouldBe an[Error.Parsing] }
        }
      }
    }

    val roundingModeTestCases = Table(
      "proto" -> "expected rounding Mode",
      DamlLf1.PrimLit.RoundingMode.UP -> java.math.RoundingMode.UP,
      DamlLf1.PrimLit.RoundingMode.DOWN -> java.math.RoundingMode.DOWN,
      DamlLf1.PrimLit.RoundingMode.CEILING -> java.math.RoundingMode.CEILING,
      DamlLf1.PrimLit.RoundingMode.FLOOR -> java.math.RoundingMode.FLOOR,
      DamlLf1.PrimLit.RoundingMode.HALF_UP -> java.math.RoundingMode.HALF_UP,
      DamlLf1.PrimLit.RoundingMode.HALF_DOWN -> java.math.RoundingMode.HALF_DOWN,
      DamlLf1.PrimLit.RoundingMode.HALF_EVEN -> java.math.RoundingMode.HALF_EVEN,
      DamlLf1.PrimLit.RoundingMode.UNNECESSARY -> java.math.RoundingMode.UNNECESSARY,
    )

    def roundingToProtoExpr(s: DamlLf1.PrimLit.RoundingMode): DamlLf1.Expr =
      DamlLf1.Expr.newBuilder().setPrimLit(DamlLf1.PrimLit.newBuilder().setRoundingMode(s)).build()

    s"translate RoundingMode iff version  >= ${LV.Features.bigNumeric}" in {
      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(roundingModeTestCases) { (proto, scala) =>
          val result =
            Try(decoder.decodeExprForTest(roundingToProtoExpr(proto), "test"))

          if (version >= LV.Features.bigNumeric)
            result shouldBe Success(Ast.EPrimLit(Ast.PLRoundingMode(scala)))
          else
            inside(result) { case Failure(error) => error shouldBe an[Error.Parsing] }
        }
      }

    }

    s"translate exception primitive as is iff version >= ${LV.Features.exceptions}" in {
      val exceptionBuiltinCases = Table(
        "exception primitive" -> "expected output",
        toProtoExpr(DamlLf1.BuiltinFunction.ANY_EXCEPTION_MESSAGE) ->
          Ast.EBuiltin(Ast.BAnyExceptionMessage),
        DamlLf1.Expr
          .newBuilder()
          .setToAnyException(
            DamlLf1.Expr.ToAnyException.newBuilder().setType(unitTypInterned).setExpr(unitExpr)
          )
          .build() ->
          Ast.EToAnyException(TUnit, EUnit),
        DamlLf1.Expr
          .newBuilder()
          .setFromAnyException(
            DamlLf1.Expr.FromAnyException.newBuilder().setType(unitTypInterned).setExpr(unitExpr)
          )
          .build() ->
          Ast.EFromAnyException(TUnit, EUnit),
      )

      forEveryVersion { version =>
        val decoder = moduleDecoder(version, ImmArraySeq.empty, ImmArraySeq.empty, typeTable)
        forEvery(exceptionBuiltinCases) { (proto, scala) =>
          val result = Try(decoder.decodeExprForTest(proto, "test"))

          if (version >= LV.Features.exceptions)
            result shouldBe Success(scala)
          else
            inside(result) { case Failure(error) =>
              error shouldBe an[Error.Parsing]
            }
        }
      }
    }

    s"translate UpdateTryCatch as is iff version >= ${LV.Features.exceptions}" in {
      val tryCatchProto =
        DamlLf1.Update.TryCatch
          .newBuilder()
          .setReturnType(unitTypInterned)
          .setTryExpr(unitExpr)
          .setVarInternedStr(0)
          .setCatchExpr(unitExpr)
          .build()
      val tryCatchUpdateProto = DamlLf1.Update.newBuilder().setTryCatch(tryCatchProto).build()
      val tryCatchExprProto = DamlLf1.Expr.newBuilder().setUpdate(tryCatchUpdateProto).build()
      val tryCatchExprScala = Ast.EUpdate(
        Ast.UpdateTryCatch(
          typ = TUnit,
          body = EUnit,
          binder = Ref.Name.assertFromString("a"),
          handler = EUnit,
        )
      )
      val stringTable = ImmArraySeq("a")
      forEveryVersion { version =>
        val decoder = moduleDecoder(version, stringTable, ImmArraySeq.empty, typeTable)
        val result = Try(decoder.decodeExprForTest(tryCatchExprProto, "test"))
        if (version >= LV.Features.exceptions)
          result shouldBe Success(tryCatchExprScala)
        else
          inside(result) { case Failure(error) =>
            error shouldBe an[Error.Parsing]
          }
      }
    }

    val interfaceDottedNameTable =
      ImmArraySeq("Mod", "T", "I", "J").map(Ref.DottedName.assertFromString)

    val interfacePrimitivesDecoder =
      (version: LV) => moduleDecoder(version, dottedNameTable = interfaceDottedNameTable)

    s"decode basic interface primitives iff version >= ${LV.Features.basicInterfaces}" in {
      val testCases = {

        val unit = DamlLf1.Unit.newBuilder().build()
        val pkgRef = DamlLf1.PackageRef.newBuilder().setSelf(unit).build
        val modRef =
          DamlLf1.ModuleRef.newBuilder().setPackageRef(pkgRef).setModuleNameInternedDname(0).build()
        val templateTyConName =
          DamlLf1.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(1)
        val ifaceTyConName =
          DamlLf1.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(2)
        val requiredIfaceTyConName =
          DamlLf1.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(3)
        val scalaTemplateTyConName = Ref.TypeConName.assertFromString("noPkgId:Mod:T")
        val scalaIfaceTyConName = Ref.TypeConName.assertFromString("noPkgId:Mod:I")
        val scalaRequiredIfaceTyConName = Ref.TypeConName.assertFromString("noPkgId:Mod:J")

        val signatoryInterface = DamlLf1.Expr
          .newBuilder()
          .setSignatoryInterface(
            DamlLf1.Expr.SignatoryInterface
              .newBuilder()
              .setInterface(ifaceTyConName)
              .setExpr(unitExpr)
              .build()
          )
          .build()

        val observerInterface = DamlLf1.Expr
          .newBuilder()
          .setObserverInterface(
            DamlLf1.Expr.ObserverInterface
              .newBuilder()
              .setInterface(ifaceTyConName)
              .setExpr(unitExpr)
              .build()
          )
          .build()

        val toInterface = DamlLf1.Expr
          .newBuilder()
          .setToInterface(
            DamlLf1.Expr.ToInterface
              .newBuilder()
              .setInterfaceType(ifaceTyConName)
              .setTemplateType(templateTyConName)
              .setTemplateExpr(unitExpr)
              .build()
          )
          .build()

        val fromInterface = DamlLf1.Expr
          .newBuilder()
          .setFromInterface(
            DamlLf1.Expr.FromInterface
              .newBuilder()
              .setInterfaceType(ifaceTyConName)
              .setTemplateType(templateTyConName)
              .setInterfaceExpr(unitExpr)
              .build()
          )
          .build()

        val interfaceTemplateTypeRep = DamlLf1.Expr
          .newBuilder()
          .setInterfaceTemplateTypeRep(
            DamlLf1.Expr.InterfaceTemplateTypeRep
              .newBuilder()
              .setInterface(ifaceTyConName)
              .setExpr(unitExpr)
              .build()
          )
          .build()

        val unsafeFromInterface = DamlLf1.Expr
          .newBuilder()
          .setUnsafeFromInterface(
            DamlLf1.Expr.UnsafeFromInterface
              .newBuilder()
              .setInterfaceType(ifaceTyConName)
              .setTemplateType(templateTyConName)
              .setContractIdExpr(unitExpr)
              .setInterfaceExpr(falseExpr)
              .build()
          )
          .build()

        val toRequiredInterface = DamlLf1.Expr
          .newBuilder()
          .setToRequiredInterface(
            DamlLf1.Expr.ToRequiredInterface
              .newBuilder()
              .setRequiredInterface(requiredIfaceTyConName)
              .setRequiringInterface(ifaceTyConName)
              .setExpr(unitExpr)
              .build()
          )
          .build()
        val fromRequiredInterface = DamlLf1.Expr
          .newBuilder()
          .setFromRequiredInterface(
            DamlLf1.Expr.FromRequiredInterface
              .newBuilder()
              .setRequiredInterface(requiredIfaceTyConName)
              .setRequiringInterface(ifaceTyConName)
              .setExpr(unitExpr)
              .build()
          )
          .build()
        val unsafeFromRequiredInterface = DamlLf1.Expr
          .newBuilder()
          .setUnsafeFromRequiredInterface(
            DamlLf1.Expr.UnsafeFromRequiredInterface
              .newBuilder()
              .setRequiredInterface(requiredIfaceTyConName)
              .setRequiringInterface(ifaceTyConName)
              .setContractIdExpr(unitExpr)
              .setInterfaceExpr(falseExpr)
              .build()
          )
          .build()

        Table(
          "input" -> "expected output",
          signatoryInterface -> Ast
            .ESignatoryInterface(ifaceId = scalaIfaceTyConName, body = EUnit),
          observerInterface -> Ast.EObserverInterface(ifaceId = scalaIfaceTyConName, body = EUnit),
          toInterface -> Ast.EToInterface(
            interfaceId = scalaIfaceTyConName,
            templateId = scalaTemplateTyConName,
            value = EUnit,
          ),
          fromInterface -> Ast.EFromInterface(
            interfaceId = scalaIfaceTyConName,
            templateId = scalaTemplateTyConName,
            value = EUnit,
          ),
          interfaceTemplateTypeRep -> Ast.EInterfaceTemplateTypeRep(
            ifaceId = scalaIfaceTyConName,
            body = EUnit,
          ),
          unsafeFromInterface -> Ast.EUnsafeFromInterface(
            interfaceId = scalaIfaceTyConName,
            templateId = scalaTemplateTyConName,
            contractIdExpr = EUnit,
            ifaceExpr = EFalse,
          ),
          toRequiredInterface -> Ast.EToRequiredInterface(
            requiredIfaceId = scalaRequiredIfaceTyConName,
            requiringIfaceId = scalaIfaceTyConName,
            body = EUnit,
          ),
          fromRequiredInterface -> Ast.EFromRequiredInterface(
            requiredIfaceId = scalaRequiredIfaceTyConName,
            requiringIfaceId = scalaIfaceTyConName,
            body = EUnit,
          ),
          unsafeFromRequiredInterface -> Ast.EUnsafeFromRequiredInterface(
            requiredIfaceId = scalaRequiredIfaceTyConName,
            requiringIfaceId = scalaIfaceTyConName,
            contractIdExpr = EUnit,
            ifaceExpr = EFalse,
          ),
        )
      }

      forEveryVersion { version =>
        forEvery(testCases) { (proto, scala) =>
          val result = Try(interfacePrimitivesDecoder(version).decodeExprForTest(proto, "test"))
          if (version < LV.Features.basicInterfaces)
            inside(result) { case Failure(error) => error shouldBe a[Error.Parsing] }
          else
            result shouldBe Success(scala)
        }
      }
    }

    s"decode extended TypeRep iff version < ${LV.v1_dev}" in {
      val testCases = {
        val typeRepTyConName = DamlLf1.Expr
          .newBuilder()
          .setBuiltin(
            DamlLf1.BuiltinFunction.TYPEREP_TYCON_NAME
          )
          .build()

        Table(
          "input" -> "expected output",
          typeRepTyConName -> Ast.EBuiltin(Ast.BTypeRepTyConName),
        )
      }

      forEveryVersion { version =>
        forEvery(testCases) { (proto, scala) =>
          val result = Try(interfacePrimitivesDecoder(version).decodeExprForTest(proto, "test"))
          if (version < LV.v1_dev)
            inside(result) { case Failure(error) => error shouldBe a[Error.Parsing] }
          else
            result shouldBe Success(scala)
        }
      }
    }

    s"decode interface update iff version >= ${LV.Features.basicInterfaces} " in {
      val testCases = {

        val unit = DamlLf1.Unit.newBuilder().build()
        val pkgRef = DamlLf1.PackageRef.newBuilder().setSelf(unit).build
        val modRef =
          DamlLf1.ModuleRef.newBuilder().setPackageRef(pkgRef).setModuleNameInternedDname(0).build()
        val ifaceTyConName =
          DamlLf1.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(2)

        val exerciseInterfaceProto = {
          val exe = DamlLf1.Update.ExerciseInterface
            .newBuilder()
            .setInterface(ifaceTyConName)
            .setChoiceInternedStr(0)
            .setCid(unitExpr)
            .setArg(unitExpr)
            .build()
          DamlLf1.Update.newBuilder().setExerciseInterface(exe).build()
        }

        val exerciseInterfaceScala = Ast.UpdateExerciseInterface(
          Ref.Identifier.assertFromString("noPkgId:Mod:I"),
          Ref.ChoiceName.assertFromString("Choice"),
          EUnit,
          EUnit,
          None,
        )

        val fetchInterfaceProto = {
          val fetch = DamlLf1.Update.FetchInterface
            .newBuilder()
            .setInterface(ifaceTyConName)
            .setCid(unitExpr)
            .build()
          DamlLf1.Update.newBuilder().setFetchInterface(fetch).build()
        }

        val fetchInterfaceScala = Ast.UpdateFetchInterface(
          Ref.Identifier.assertFromString("noPkgId:Mod:I"),
          EUnit,
        )

        Table(
          "input" -> "expected output",
          exerciseInterfaceProto -> exerciseInterfaceScala,
          fetchInterfaceProto -> fetchInterfaceScala,
        )
      }

      forEveryVersionSuchThat(_ >= LV.Features.basicInterfaces) { version =>
        forEvery(testCases) { (protoUpdate, scala) =>
          val decoder =
            moduleDecoder(version, ImmArraySeq("Choice"), interfaceDottedNameTable, typeTable)
          val proto = DamlLf1.Expr.newBuilder().setUpdate(protoUpdate).build()
          decoder.decodeExprForTest(proto, "test") shouldBe Ast.EUpdate(scala)
        }
      }
    }

    s"translate interface exercise guard iff version >= ${LV.v1_dev}" in {

      val unit = DamlLf1.Unit.newBuilder().build()
      val pkgRef = DamlLf1.PackageRef.newBuilder().setSelf(unit).build
      val modRef =
        DamlLf1.ModuleRef.newBuilder().setPackageRef(pkgRef).setModuleNameInternedDname(0).build()
      val ifaceTyConName =
        DamlLf1.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(2)

      val exerciseInterfaceProto = {
        val exe = DamlLf1.Update.ExerciseInterface
          .newBuilder()
          .setInterface(ifaceTyConName)
          .setChoiceInternedStr(0)
          .setCid(unitExpr)
          .setArg(unitExpr)
          .setGuard(unitExpr)
          .build()
        DamlLf1.Update.newBuilder().setExerciseInterface(exe).build()
      }

      val exerciseInterfaceScala = Ast.UpdateExerciseInterface(
        Ref.Identifier.assertFromString("noPkgId:Mod:I"),
        Ref.ChoiceName.assertFromString("Choice"),
        EUnit,
        EUnit,
        Some(EUnit),
      )

      forEveryVersionSuchThat(_ >= LV.v1_dev) { version =>
        val decoder =
          moduleDecoder(version, ImmArraySeq("Choice"), interfaceDottedNameTable, typeTable)
        val proto = DamlLf1.Expr.newBuilder().setUpdate(exerciseInterfaceProto).build()
        decoder.decodeExprForTest(proto, "test") shouldBe Ast.EUpdate(exerciseInterfaceScala)
      }
    }
  }

  "decodeModule" should {

    val interfaceDefDecoder = {
      val interfaceDefStringTable = ImmArraySeq("this", "method1", "method2")

      val interfaceDefDottedNameTable =
        ImmArraySeq("Mod", "T", "I", "J", "K").map(Ref.DottedName.assertFromString)

      (version: LV) =>
        moduleDecoder(version, interfaceDefStringTable, interfaceDefDottedNameTable, typeTable)
    }

    s"decode interface definitions correctly iff version >= ${LV.Features.basicInterfaces}" in {

      val fearureFlags = DamlLf1.FeatureFlags
        .newBuilder()
        .setForbidPartyLiterals(true)
        .setDontDivulgeContractIdsInCreateArguments(true)
        .setDontDiscloseNonConsumingChoicesToObservers(true)
        .build()

      val emptyDefInterface = DamlLf1.DefInterface
        .newBuilder()
        .setTyconInternedDname(2)
        .setParamInternedStr(0)
        .setView(unitTypInterned)
        .build()

      val emptyDefInterfaceScala =
        Ast.DefInterface(
          Set.empty,
          Ref.Name.assertFromString("this"),
          Map.empty,
          Map.empty,
          Map.empty,
          TUnit,
        )

      val methodsDefInterface = {
        val interfaceMethod1 =
          DamlLf1.InterfaceMethod.newBuilder().setMethodInternedName(1).setType(textTypInterned)
        val interfaceMethod2 =
          DamlLf1.InterfaceMethod.newBuilder().setMethodInternedName(2).setType(boolTypInterned)

        DamlLf1.DefInterface
          .newBuilder()
          .setTyconInternedDname(2)
          .setParamInternedStr(0)
          .addMethods(interfaceMethod1)
          .addMethods(interfaceMethod2)
          .setView(unitTypInterned)
          .build()
      }

      val methodsDefInterfaceScala = {
        val methodName1 = Ref.MethodName.assertFromString("method1")
        val methodName2 = Ref.MethodName.assertFromString("method2")

        Ast.DefInterface(
          Set.empty,
          Ref.Name.assertFromString("this"),
          Map.empty,
          Map(
            methodName1 -> Ast.InterfaceMethod(methodName1, TText),
            methodName2 -> Ast.InterfaceMethod(methodName2, TBool),
          ),
          Map.empty,
          TUnit,
        )
      }

      val coImplementsDefInterface = DamlLf1.DefInterface
        .newBuilder()
        .setTyconInternedDname(2)
        .setParamInternedStr(0)
        .setView(unitTypInterned)
        .build()

      val coImplementsDefInterfaceScala =
        Ast.DefInterface(
          Set.empty,
          Ref.IdString.Name.assertFromString("this"),
          Map.empty,
          Map.empty,
          Map.empty,
          TUnit,
        )

      val testCases = {
        Table(
          "input" -> "expected output",
          emptyDefInterface -> emptyDefInterfaceScala,
          methodsDefInterface -> methodsDefInterfaceScala,
          coImplementsDefInterface -> coImplementsDefInterfaceScala,
        )
      }

      val interfaceName = Ref.DottedName.assertFromString("I")
      val modName = Ref.DottedName.assertFromString("Mod")

      forEveryVersion { version =>
        forEvery(testCases) { (proto, scala) =>
          val module =
            DamlLf1.Module
              .newBuilder()
              .setFlags(fearureFlags)
              .setNameInternedDname(0)
              .addInterfaces(proto)
              .build()
          val result = Try(interfaceDefDecoder(version).decodeModule(module))
          if (version < LV.Features.basicInterfaces)
            inside(result) { case Failure(error) =>
              if (error.isInstanceOf[Error.Parsing])
                error shouldBe an[Error.Parsing]
            }
          else {
            result shouldBe Success(
              Ast.Module(
                name = modName,
                definitions = Map.empty,
                templates = Map.empty,
                exceptions = Map.empty,
                interfaces = Map(interfaceName -> scala),
                featureFlags = Ast.FeatureFlags(),
              )
            )
          }
        }
      }
    }

    s"accept interface requires iff version >= ${LV.Features.basicInterfaces}" in {

      val interfaceName = Ref.DottedName.assertFromString("I")

      val unit = DamlLf1.Unit.newBuilder()
      val pkgRef = DamlLf1.PackageRef.newBuilder().setSelf(unit)
      val modRef =
        DamlLf1.ModuleRef.newBuilder().setPackageRef(pkgRef).setModuleNameInternedDname(0)

      val requiresDefInterface = {
        val typeConNameJ =
          DamlLf1.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(3)
        val typeConNameK =
          DamlLf1.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(4)

        DamlLf1.DefInterface
          .newBuilder()
          .setTyconInternedDname(1)
          .setParamInternedStr(0)
          .addRequires(typeConNameJ)
          .addRequires(typeConNameK)
          .setView(unitTypInterned)
          .build()
      }

      val requiresDefInterfaceScala =
        Ast.DefInterface(
          Set(
            Ref.TypeConName.assertFromString("noPkgId:Mod:J"),
            Ref.TypeConName.assertFromString("noPkgId:Mod:K"),
            Ref.TypeConName.assertFromString("noPkgId:Mod:K"),
          ),
          Ref.IdString.Name.assertFromString("this"),
          Map.empty,
          Map.empty,
          Map.empty,
          TUnit,
        )

      forEveryVersion { version =>
        val decoder = interfaceDefDecoder(version)
        val result = Try(decoder.decodeDefInterfaceForTest(interfaceName, requiresDefInterface))
        if (version >= LV.Features.basicInterfaces)
          result shouldBe Success(requiresDefInterfaceScala)
        else
          inside(result) { case Failure(error) => error shouldBe an[Error.Parsing] }
      }
    }

  }

  "decodeModuleRef" should {

    lazy val Right(ArchivePayload(pkgId, dalfProto, version)) =
      ArchiveReader.fromFile(Paths.get(rlocation("daml-lf/archive/DarReaderTest.dalf")))

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
      version.major should ===(LV.Major.V1)

      version.minor should !==("dev")

      extId should not be empty
      (extId: String) should !==(pkgId: String)
    }

    "decode resolving the interned package ID" in {
      val decoder = new DecodeV1(version.minor)
      inside(decoder.decodePackage(pkgId, dalfProto.getDamlLf1, false)) { case Right(pkg) =>
        inside(
          pkg
            .modules(Ref.DottedName.assertFromString("DarReaderTest"))
            .definitions(Ref.DottedName.assertFromString("reverseCopy"))
        ) { case Ast.DValue(_, Ast.ELocation(_, Ast.EVal(Ref.Identifier(resolvedExtId, _))), _) =>
          (resolvedExtId: String) should ===(extId: String)
        }
      }
    }
  }

  "decodePackageMetadata" should {
    "accept a valid package name and version" in {
      forEveryVersionSuchThat(_ >= LV.Features.packageMetadata) { version =>
        new DecodeV1(version.minor).decodePackageMetadata(
          DamlLf1.PackageMetadata
            .newBuilder()
            .setNameInternedStr(0)
            .setVersionInternedStr(1)
            .build(),
          ImmArraySeq("foobar", "0.0.0"),
        ) shouldBe Ast.PackageMetadata(
          Ref.PackageName.assertFromString("foobar"),
          Ref.PackageVersion.assertFromString("0.0.0"),
        )
      }
    }

    "reject a package namewith space" in {
      forEveryVersionSuchThat(_ >= LV.Features.packageMetadata) { version =>
        an[Error.Parsing] shouldBe thrownBy(
          new DecodeV1(version.minor).decodePackageMetadata(
            DamlLf1.PackageMetadata
              .newBuilder()
              .setNameInternedStr(0)
              .setVersionInternedStr(1)
              .build(),
            ImmArraySeq("foo bar", "0.0.0"),
          )
        )
      }
    }

    "reject a package version with leading zero" in {
      forEveryVersionSuchThat(_ >= LV.Features.packageMetadata) { version =>
        an[Error.Parsing] shouldBe thrownBy(
          new DecodeV1(version.minor).decodePackageMetadata(
            DamlLf1.PackageMetadata
              .newBuilder()
              .setNameInternedStr(0)
              .setVersionInternedStr(1)
              .build(),
            ImmArraySeq("foobar", "01.0.0"),
          )
        )
      }
    }

    "reject a package version with a dash" in {
      forEveryVersionSuchThat(_ >= LV.Features.packageMetadata) { version =>
        an[Error.Parsing] shouldBe thrownBy(
          new DecodeV1(version.minor).decodePackageMetadata(
            DamlLf1.PackageMetadata
              .newBuilder()
              .setNameInternedStr(0)
              .setVersionInternedStr(1)
              .build(),
            ImmArraySeq("foobar", "0.0.0-"),
          )
        )
      }
    }
  }

  "decodePackage" should {
    "reject PackageMetadata if lf version < 1.8" in {
      forEveryVersionSuchThat(_ < LV.Features.packageMetadata) { version =>
        val decoder = new DecodeV1(version.minor)
        val pkgId = Ref.PackageId.assertFromString(
          "0000000000000000000000000000000000000000000000000000000000000000"
        )
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
        inside(decoder.decodePackage(pkgId, pkg, false)) { case Left(err) =>
          err shouldBe an[Error.Parsing]
        }
      }
    }

    "require PackageMetadata to be present if lf version >= 1.8" in {
      forEveryVersionSuchThat(_ >= LV.Features.packageMetadata) { version =>
        val decoder = new DecodeV1(version.minor)
        val pkgId = Ref.PackageId.assertFromString(
          "0000000000000000000000000000000000000000000000000000000000000000"
        )
        inside(decoder.decodePackage(pkgId, DamlLf1.Package.newBuilder().build(), false)) {
          case Left(err) => err shouldBe an[Error.Parsing]
        }
      }
    }

    "decode PackageMetadata if lf version >= 1.8" in {
      forEveryVersionSuchThat(_ >= LV.Features.packageMetadata) { version =>
        val decoder = new DecodeV1(version.minor)
        val pkgId = Ref.PackageId.assertFromString(
          "0000000000000000000000000000000000000000000000000000000000000000"
        )
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
        inside(decoder.decodePackage(pkgId, pkg, false)) { case Right(pkg) =>
          pkg.metadata shouldBe Some(
            Ast.PackageMetadata(
              Ref.PackageName.assertFromString("foobar"),
              Ref.PackageVersion.assertFromString("0.0.0"),
            )
          )
        }
      }
    }
  }

  "decodeChoice" should {
    val stringTable = ImmArraySeq("SomeChoice", "controllers", "observers", "self", "arg", "body")
    val templateName = Ref.DottedName.assertFromString("Template")
    val controllersExpr = DamlLf1.Expr.newBuilder().setVarInternedStr(1).build()
    val observersExpr = DamlLf1.Expr.newBuilder().setVarInternedStr(2).build()
    val bodyExp = DamlLf1.Expr.newBuilder().setVarInternedStr(5).build()

    "reject choice with observers if lf version = 1.6" in {
      // special case for LF 1.6 that does not support string interning
      val protoChoiceWithoutObservers = DamlLf1.TemplateChoice
        .newBuilder()
        .setNameStr("ChoiceName")
        .setConsuming(true)
        .setControllers(DamlLf1.Expr.newBuilder().setVarStr("controllers"))
        .setSelfBinderStr("self")
        .setArgBinder(DamlLf1.VarWithType.newBuilder().setVarStr("arg").setType(unitTyp))
        .setRetType(unitTyp)
        .setUpdate(DamlLf1.Expr.newBuilder().setVarStr("body").build())
        .build()

      val protoChoiceWithObservers =
        protoChoiceWithoutObservers.toBuilder.setObservers(observersExpr).build

      forEveryVersionSuchThat(_ < LV.Features.internedStrings) { version =>
        assert(version < LV.Features.internedStrings)
        assert(version < LV.Features.internedTypes)

        val decoder = moduleDecoder(version)

        decoder.decodeChoiceForTest(templateName, protoChoiceWithoutObservers)
        an[Error.Parsing] should be thrownBy (decoder
          .decodeChoiceForTest(templateName, protoChoiceWithObservers))

      }
    }

    "reject choice with observers if 1.7 < lf version < 1.dev" in {
      val protoChoiceWithoutObservers = DamlLf1.TemplateChoice
        .newBuilder()
        .setNameInternedStr(0)
        .setConsuming(true)
        .setControllers(controllersExpr)
        .setSelfBinderInternedStr(3)
        .setArgBinder(DamlLf1.VarWithType.newBuilder().setVarInternedStr(4).setType(unitTyp))
        .setRetType(unitTyp)
        .setUpdate(bodyExp)
        .build()

      val protoChoiceWithObservers =
        protoChoiceWithoutObservers.toBuilder.setObservers(observersExpr).build

      forEveryVersionSuchThat(v =>
        LV.Features.internedStrings < v && v < LV.Features.choiceObservers
      ) { version =>
        assert(LV.Features.internedStrings <= version)
        assert(version < LV.Features.internedTypes)

        val decoder = moduleDecoder(version, stringTable)

        decoder.decodeChoiceForTest(templateName, protoChoiceWithoutObservers)
        an[Error.Parsing] should be thrownBy (
          decoder
            .decodeChoiceForTest(templateName, protoChoiceWithObservers),
        )

      }
    }

    "reject choice without observers if lv version >= 1.dev" in {

      val protoChoiceWithoutObservers = DamlLf1.TemplateChoice
        .newBuilder()
        .setNameInternedStr(0)
        .setConsuming(true)
        .setControllers(controllersExpr)
        .setSelfBinderInternedStr(3)
        .setArgBinder(
          DamlLf1.VarWithType.newBuilder().setVarInternedStr(4).setType(unitTypInterned)
        )
        .setRetType(unitTypInterned)
        .setUpdate(bodyExp)
        .build()

      val protoChoiceWithObservers =
        protoChoiceWithoutObservers.toBuilder.setObservers(observersExpr).build

      forEveryVersionSuchThat(LV.Features.choiceObservers <= _) { version =>
        assert(LV.Features.internedStrings <= version)
        assert(LV.Features.internedTypes <= version)

        val decoder = moduleDecoder(version, stringTable, ImmArraySeq.empty, typeTable)

        an[Error.Parsing] should be thrownBy (
          decoder.decodeChoiceForTest(templateName, protoChoiceWithoutObservers),
        )
        decoder.decodeChoiceForTest(templateName, protoChoiceWithObservers)
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
      forEveryVersionSuchThat(_ < LV.Features.internedTypes) { version =>
        val decoder = new DecodeV1(version.minor)
        val env = decoder.Env(
          Ref.PackageId.assertFromString("noPkgId"),
          ImmArraySeq.empty,
          ImmArraySeq.empty,
          IndexedSeq.empty,
          None,
          None,
          onlySerializableDataDefs = false,
        )
        val parseError =
          the[Error.Parsing] thrownBy (decoder
            .decodeInternedTypesForTest(env, pkgWithInternedTypes))
        parseError.toString should include("interned types table is not supported")
      }
    }
  }

  s"reject experiment expression if LF version < ${LV.v1_dev}" in {

    val expr = DamlLf1.Expr
      .newBuilder()
      .setExperimental(
        DamlLf1.Expr.Experimental
          .newBuilder()
          .setName("ANSWER")
          .setType(
            DamlLf1.Type
              .newBuilder()
              .setPrim(
                DamlLf1.Type.Prim.newBuilder().setPrim(DamlLf1.PrimType.INT64)
              )
          )
      )
      .build()

    forEveryVersionSuchThat(_ < LV.v1_dev) { version =>
      val decoder = moduleDecoder(version)
      an[Error.Parsing] shouldBe thrownBy(decoder.decodeExprForTest(expr, "test"))
    }
  }

  s"reject DefValue with no_party_literals = false" in {
    val defValue =
      DamlLf1.DefValue
        .newBuilder()
        .setNoPartyLiterals(false)
        .build()
    forEveryVersion { version =>
      val decoder = moduleDecoder(version)
      val ex = the[Error.Parsing] thrownBy decoder.decodeDefValueForTest(defValue)
      ex.msg shouldBe "DefValue must have no_party_literals set to true"
    }
  }

  s"reject Feature flags set to false" in {
    def featureFlags(
        forbidPartyLits: Boolean,
        dontDivulgeCids: Boolean,
        dontDiscloseNonConsuming: Boolean,
    ) = DamlLf1.FeatureFlags
      .newBuilder()
      .setForbidPartyLiterals(forbidPartyLits)
      .setDontDivulgeContractIdsInCreateArguments(dontDivulgeCids)
      .setDontDiscloseNonConsumingChoicesToObservers(dontDiscloseNonConsuming)
      .build()
    forEveryVersion { version =>
      val decoder = moduleDecoder(version)
      decoder.decodeFeatureFlags(featureFlags(true, true, true)) shouldBe Ast.FeatureFlags.default
      Seq(
        featureFlags(false, true, true),
        featureFlags(true, false, true),
        featureFlags(true, true, false),
      ).foreach { flags =>
        an[Error.Parsing] shouldBe thrownBy(decoder.decodeFeatureFlags(flags))
      }
    }
  }
}
