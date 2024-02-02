// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.math.BigDecimal
import java.nio.file.Paths
import com.daml.bazeltools.BazelRunfiles._
import com.daml.lf.data.{Decimal, Numeric, Ref}
import com.daml.lf.language.Util._
import com.daml.lf.language.{Ast, LanguageVersion => LV}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.daml_lf_dev.DamlLf2
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{Inside, OptionValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.Ordering.Implicits.infixOrderingOps
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class DecodeV2Spec
    extends AnyWordSpec
    with Matchers
    with Inside
    with OptionValues
    with ScalaCheckPropertyChecks {

  val unitTyp: DamlLf2.Type = DamlLf2.Type
    .newBuilder()
    .setPrim(DamlLf2.Type.Prim.newBuilder().setPrim(DamlLf2.PrimType.UNIT))
    .build()

  val typeTable = ImmArraySeq(TUnit, TBool, TText)
  val unitTypInterned = DamlLf2.Type.newBuilder().setInterned(0).build()
  val boolTypInterned = DamlLf2.Type.newBuilder().setInterned(1).build()
  val textTypInterned = DamlLf2.Type.newBuilder().setInterned(2).build()

  val unitExpr: DamlLf2.Expr = DamlLf2.Expr
    .newBuilder()
    .setPrimCon(DamlLf2.PrimCon.CON_UNIT)
    .build()

  val falseExpr: DamlLf2.Expr = DamlLf2.Expr
    .newBuilder()
    .setPrimCon(DamlLf2.PrimCon.CON_FALSE)
    .build()

  "The entries of primTypeInfos correspond to Protobuf DamlLf2.PrimType" in {

    (Set(
      DamlLf2.PrimType.UNRECOGNIZED,
      DamlLf2.PrimType.DECIMAL,
    ) ++
      DecodeV2.builtinTypeInfos.map(_.proto)) shouldBe
      DamlLf2.PrimType.values().toSet

  }

  "The entries of builtinFunctionInfos correspond to Protobuf DamlLf2.BuiltinFunction" in {

    val s1 =
      Set(DamlLf2.BuiltinFunction.UNRECOGNIZED) ++ DecodeV2.builtinFunctionInfos.map(_.proto)
    val s2 = DamlLf2.BuiltinFunction.values().toSet
    (s1 -- s2) shouldBe Set.empty
    (s2 -- s1) shouldBe Set.empty

  }

  private[this] val dummyModuleStr = "dummyModule"
  private[this] val dummyModuleName = Ref.DottedName.assertFromString(dummyModuleStr)

  // TODO(https://github.com/digital-asset/daml/issues/18240): revert to [[All]] once V1 is gone
  private[this] val lfVersions = LV.All.filter(_.major == LV.Major.V2)

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
    new DecodeV2(version.minor).Env(
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

      val input = DamlLf2.Kind.newBuilder().setNat(DamlLf2.Unit.newBuilder()).build()

      forEveryVersionSuchThat(_ < LV.Features.numeric) { version =>
        an[Error.Parsing] shouldBe thrownBy(moduleDecoder(version).decodeKindForTest(input))
      }
    }

    "accept nat kind if lf version >= 1.7" in {
      val input = DamlLf2.Kind.newBuilder().setNat(DamlLf2.Unit.newBuilder()).build()
      forEveryVersionSuchThat(_ >= LV.Features.numeric) { version =>
        moduleDecoder(version).decodeKindForTest(input) shouldBe Ast.KNat
      }
    }
  }

  "uncheckedDecodeType" should {

    import DamlLf2.PrimType._

    def buildNat(i: Long) = DamlLf2.Type.newBuilder().setNat(i).build()

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

    def buildPrimType(primType: DamlLf2.PrimType, args: DamlLf2.Type*) =
      DamlLf2.Type
        .newBuilder()
        .setPrim(DamlLf2.Type.Prim.newBuilder().setPrim(primType).addAllArgs(args.asJava))
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
      buildPrimType(NUMERIC, DamlLf2.Type.newBuilder().setNat(Decimal.scale.toLong).build()) ->
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

      val unit = DamlLf2.Type
        .newBuilder()
        .setPrim(DamlLf2.Type.Prim.newBuilder().setPrim(DamlLf2.PrimType.UNIT))
        .build

      val stringTable = ImmArraySeq("a", "b", "c")
      val stringIdx = stringTable.zipWithIndex.toMap

      def fieldWithUnitWithInterning(s: String) =
        DamlLf2.FieldWithType.newBuilder().setFieldInternedStr(stringIdx(s)).setType(unit)

      def buildTStructWithInterning(fields: Seq[String]) =
        DamlLf2.Type
          .newBuilder()
          .setStruct(
            fields.foldLeft(DamlLf2.Type.Struct.newBuilder())((builder, name) =>
              builder.addFields(fieldWithUnitWithInterning(name))
            )
          )
          .build()

      forEveryVersion { version =>
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
        DamlLf2.PrimType.ANY_EXCEPTION -> Ast.BTAnyException,
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

    "reject non interned type for LF >= 1.11" in {

      val stringTable = ImmArraySeq("pkgId", "x")
      val dottedNameTable = ImmArraySeq("Mod", "T", "S").map(Ref.DottedName.assertFromString)

      val unit = DamlLf2.Unit.newBuilder().build()
      val pkgRef = DamlLf2.PackageRef.newBuilder().setSelf(unit).build
      val modRef =
        DamlLf2.ModuleRef.newBuilder().setPackageRef(pkgRef).setModuleNameInternedDname(0).build()
      val tyConName = DamlLf2.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(1)
      val tySynName = DamlLf2.TypeSynName.newBuilder().setModule(modRef).setNameInternedDname(2)

      def newBuilder = DamlLf2.Type.newBuilder()

      val star = DamlLf2.Kind.newBuilder().setStar(unit).build
      val xWithStar =
        DamlLf2.TypeVarWithKind.newBuilder().setVarInternedStr(1).setKind(star).build()
      val typeVar = newBuilder.setVar(DamlLf2.Type.Var.newBuilder().setVarInternedStr(0)).build()
      val typeBool =
        newBuilder.setPrim(DamlLf2.Type.Prim.newBuilder().setPrim(DamlLf2.PrimType.BOOL)).build()
      val xWithBool =
        DamlLf2.FieldWithType.newBuilder.setFieldInternedStr(1).setType(typeBool).build()

      val testCases = Table[DamlLf2.Type](
        "type",
        typeVar,
        newBuilder.setNat(10).build(),
        newBuilder.setSyn(DamlLf2.Type.Syn.newBuilder().setTysyn(tySynName)).build(),
        newBuilder.setCon(DamlLf2.Type.Con.newBuilder().setTycon(tyConName)).build(),
        typeBool,
        newBuilder
          .setForall(DamlLf2.Type.Forall.newBuilder().addVars(xWithStar).setBody(typeVar))
          .build(),
        newBuilder.setStruct(DamlLf2.Type.Struct.newBuilder().addFields(xWithBool)).build(),
      )

      forEveryVersion { version =>
        val decoder = moduleDecoder(version, stringTable, dottedNameTable)
        forEvery(testCases)(proto =>
          an[Error.Parsing] shouldBe thrownBy(decoder.decodeTypeForTest(proto))
        )
      }
    }

  }

  "decodeExpr" should {

    def toProtoExpr(b: DamlLf2.BuiltinFunction) =
      DamlLf2.Expr.newBuilder().setBuiltin(b).build()

    def toDecimalProto(s: String): DamlLf2.Expr =
      DamlLf2.Expr.newBuilder().setPrimLit(DamlLf2.PrimLit.newBuilder().setDecimalStr(s)).build()

    // def toNumericProto(s: String): DamlLf2.Expr =
    //  DamlLf2.Expr.newBuilder().setPrimLit(DamlLf2.PrimLit.newBuilder().setNumeric(s)).build()

    def toNumericProto(id: Int): DamlLf2.Expr =
      DamlLf2.Expr
        .newBuilder()
        .setPrimLit(DamlLf2.PrimLit.newBuilder().setNumericInternedStr(id))
        .build()

    val decimalBuiltinTestCases = Table[DamlLf2.BuiltinFunction, String, Ast.Expr](
      ("decimal builtins", "minVersion", "expected output"),
      (
        DamlLf2.BuiltinFunction.ADD_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BAddNumeric), TDecimalScale),
      ),
      (
        DamlLf2.BuiltinFunction.SUB_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BSubNumeric), TDecimalScale),
      ),
      (
        DamlLf2.BuiltinFunction.MUL_DECIMAL,
        "6",
        Ast.ETyApp(
          Ast.ETyApp(Ast.ETyApp(Ast.EBuiltin(Ast.BMulNumericLegacy), TDecimalScale), TDecimalScale),
          TDecimalScale,
        ),
      ),
      (
        DamlLf2.BuiltinFunction.DIV_DECIMAL,
        "6",
        Ast.ETyApp(
          Ast.ETyApp(Ast.ETyApp(Ast.EBuiltin(Ast.BDivNumericLegacy), TDecimalScale), TDecimalScale),
          TDecimalScale,
        ),
      ),
      (
        DamlLf2.BuiltinFunction.ROUND_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BRoundNumeric), TDecimalScale),
      ),
      (DamlLf2.BuiltinFunction.LEQ_DECIMAL, "6", Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TDecimal)),
      (DamlLf2.BuiltinFunction.LESS_DECIMAL, "6", Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TDecimal)),
      (
        DamlLf2.BuiltinFunction.GEQ_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TDecimal),
      ),
      (
        DamlLf2.BuiltinFunction.GREATER_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BGreater), TDecimal),
      ),
      (
        DamlLf2.BuiltinFunction.DECIMAL_TO_TEXT,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BNumericToText), TDecimalScale),
      ),
      (
        DamlLf2.BuiltinFunction.TEXT_TO_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BTextToNumericLegacy), TDecimalScale),
      ),
      (
        DamlLf2.BuiltinFunction.INT64_TO_DECIMAL,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BInt64ToNumericLegacy), TDecimalScale),
      ),
      (
        DamlLf2.BuiltinFunction.DECIMAL_TO_INT64,
        "6",
        Ast.ETyApp(Ast.EBuiltin(Ast.BNumericToInt64), TDecimalScale),
      ),
      (DamlLf2.BuiltinFunction.EQUAL_DECIMAL, "6", Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TDecimal)),
    )

    val numericBuiltinTestCases = Table(
      "numeric builtins" -> "expected output",
      DamlLf2.BuiltinFunction.ADD_NUMERIC -> Ast.EBuiltin(Ast.BAddNumeric),
      DamlLf2.BuiltinFunction.SUB_NUMERIC -> Ast.EBuiltin(Ast.BSubNumeric),
      DamlLf2.BuiltinFunction.MUL_NUMERIC_LEGACY -> Ast.EBuiltin(Ast.BMulNumericLegacy),
      DamlLf2.BuiltinFunction.DIV_NUMERIC_LEGACY -> Ast.EBuiltin(Ast.BDivNumericLegacy),
      DamlLf2.BuiltinFunction.ROUND_NUMERIC -> Ast.EBuiltin(Ast.BRoundNumeric),
      DamlLf2.BuiltinFunction.NUMERIC_TO_TEXT -> Ast.EBuiltin(Ast.BNumericToText),
      DamlLf2.BuiltinFunction.TEXT_TO_NUMERIC_LEGACY -> Ast.EBuiltin(Ast.BTextToNumericLegacy),
      DamlLf2.BuiltinFunction.INT64_TO_NUMERIC_LEGACY -> Ast.EBuiltin(Ast.BInt64ToNumericLegacy),
      DamlLf2.BuiltinFunction.NUMERIC_TO_INT64 -> Ast.EBuiltin(Ast.BNumericToInt64),
    )

    val comparisonBuiltinCases = Table(
      "compare builtins" -> "expected output",
      DamlLf2.BuiltinFunction.EQUAL_INT64 -> Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TInt64),
      DamlLf2.BuiltinFunction.LEQ_INT64 -> Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TInt64),
      DamlLf2.BuiltinFunction.LESS_INT64 -> Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TInt64),
      DamlLf2.BuiltinFunction.GEQ_INT64 -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TInt64),
      DamlLf2.BuiltinFunction.GREATER_INT64 -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreater), TInt64),
      DamlLf2.BuiltinFunction.EQUAL_DATE -> Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TDate),
      DamlLf2.BuiltinFunction.LEQ_DATE -> Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TDate),
      DamlLf2.BuiltinFunction.LESS_DATE -> Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TDate),
      DamlLf2.BuiltinFunction.GEQ_DATE -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TDate),
      DamlLf2.BuiltinFunction.GREATER_DATE -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreater), TDate),
      DamlLf2.BuiltinFunction.EQUAL_TIMESTAMP -> Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TTimestamp),
      DamlLf2.BuiltinFunction.LEQ_TIMESTAMP -> Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TTimestamp),
      DamlLf2.BuiltinFunction.LESS_TIMESTAMP -> Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TTimestamp),
      DamlLf2.BuiltinFunction.GEQ_TIMESTAMP -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TTimestamp),
      DamlLf2.BuiltinFunction.GREATER_TIMESTAMP -> Ast
        .ETyApp(Ast.EBuiltin(Ast.BGreater), TTimestamp),
      DamlLf2.BuiltinFunction.EQUAL_TEXT -> Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TText),
      DamlLf2.BuiltinFunction.LEQ_TEXT -> Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TText),
      DamlLf2.BuiltinFunction.LESS_TEXT -> Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TText),
      DamlLf2.BuiltinFunction.GEQ_TEXT -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TText),
      DamlLf2.BuiltinFunction.GREATER_TEXT -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreater), TText),
      DamlLf2.BuiltinFunction.EQUAL_PARTY -> Ast.ETyApp(Ast.EBuiltin(Ast.BEqual), TParty),
      DamlLf2.BuiltinFunction.LEQ_PARTY -> Ast.ETyApp(Ast.EBuiltin(Ast.BLessEq), TParty),
      DamlLf2.BuiltinFunction.LESS_PARTY -> Ast.ETyApp(Ast.EBuiltin(Ast.BLess), TParty),
      DamlLf2.BuiltinFunction.GEQ_PARTY -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreaterEq), TParty),
      DamlLf2.BuiltinFunction.GREATER_PARTY -> Ast.ETyApp(Ast.EBuiltin(Ast.BGreater), TParty),
    )

    val numericComparisonBuiltinCases = Table(
      "numeric comparison builtins" -> "expected output",
      DamlLf2.BuiltinFunction.EQUAL_NUMERIC -> Ast.EBuiltin(Ast.BEqualNumeric),
      DamlLf2.BuiltinFunction.LEQ_NUMERIC -> Ast.EBuiltin(Ast.BLessEqNumeric),
      DamlLf2.BuiltinFunction.LESS_NUMERIC -> Ast.EBuiltin(Ast.BLessNumeric),
      DamlLf2.BuiltinFunction.GEQ_NUMERIC -> Ast.EBuiltin(Ast.BGreaterEqNumeric),
      DamlLf2.BuiltinFunction.GREATER_NUMERIC -> Ast.EBuiltin(Ast.BGreaterNumeric),
    )

    val genericComparisonBuiltinCases = Table(
      "generic comparison builtins" -> "expected output",
      DamlLf2.BuiltinFunction.EQUAL -> Ast.EBuiltin(Ast.BEqual),
      DamlLf2.BuiltinFunction.LESS_EQ -> Ast.EBuiltin(Ast.BLessEq),
      DamlLf2.BuiltinFunction.LESS -> Ast.EBuiltin(Ast.BLess),
      DamlLf2.BuiltinFunction.GREATER_EQ -> Ast.EBuiltin(Ast.BGreaterEq),
      DamlLf2.BuiltinFunction.GREATER -> Ast.EBuiltin(Ast.BGreater),
    )

    val negativeBuiltinTestCases = Table(
      "other builtins" -> "expected output",
      // We do not need to test all other builtin
      DamlLf2.BuiltinFunction.ADD_INT64 -> Ast.EBuiltin(Ast.BAddInt64),
      DamlLf2.BuiltinFunction.APPEND_TEXT -> Ast.EBuiltin(Ast.BAppendText),
    )

    val contractIdTextConversionCases = Table(
      "builtin" -> "expected output",
      DamlLf2.BuiltinFunction.CONTRACT_ID_TO_TEXT -> Ast.EBuiltin(Ast.BContractIdToText),
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
          if (proto != DamlLf2.BuiltinFunction.EQUAL_NUMERIC || version == LV.v1_7)
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
        DamlLf2.BuiltinFunction.SCALE_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BScaleBigNumeric),
        DamlLf2.BuiltinFunction.PRECISION_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BPrecisionBigNumeric),
        DamlLf2.BuiltinFunction.ADD_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BAddBigNumeric),
        DamlLf2.BuiltinFunction.SUB_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BSubBigNumeric),
        DamlLf2.BuiltinFunction.MUL_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BMulBigNumeric),
        DamlLf2.BuiltinFunction.DIV_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BDivBigNumeric),
        DamlLf2.BuiltinFunction.NUMERIC_TO_BIGNUMERIC ->
          Ast.EBuiltin(Ast.BNumericToBigNumeric),
        DamlLf2.BuiltinFunction.BIGNUMERIC_TO_NUMERIC_LEGACY ->
          Ast.EBuiltin(Ast.BBigNumericToNumericLegacy),
        DamlLf2.BuiltinFunction.BIGNUMERIC_TO_TEXT ->
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
      DamlLf2.PrimLit.RoundingMode.UP -> java.math.RoundingMode.UP,
      DamlLf2.PrimLit.RoundingMode.DOWN -> java.math.RoundingMode.DOWN,
      DamlLf2.PrimLit.RoundingMode.CEILING -> java.math.RoundingMode.CEILING,
      DamlLf2.PrimLit.RoundingMode.FLOOR -> java.math.RoundingMode.FLOOR,
      DamlLf2.PrimLit.RoundingMode.HALF_UP -> java.math.RoundingMode.HALF_UP,
      DamlLf2.PrimLit.RoundingMode.HALF_DOWN -> java.math.RoundingMode.HALF_DOWN,
      DamlLf2.PrimLit.RoundingMode.HALF_EVEN -> java.math.RoundingMode.HALF_EVEN,
      DamlLf2.PrimLit.RoundingMode.UNNECESSARY -> java.math.RoundingMode.UNNECESSARY,
    )

    def roundingToProtoExpr(s: DamlLf2.PrimLit.RoundingMode): DamlLf2.Expr =
      DamlLf2.Expr.newBuilder().setPrimLit(DamlLf2.PrimLit.newBuilder().setRoundingMode(s)).build()

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
        toProtoExpr(DamlLf2.BuiltinFunction.ANY_EXCEPTION_MESSAGE) ->
          Ast.EBuiltin(Ast.BAnyExceptionMessage),
        DamlLf2.Expr
          .newBuilder()
          .setToAnyException(
            DamlLf2.Expr.ToAnyException.newBuilder().setType(unitTypInterned).setExpr(unitExpr)
          )
          .build() ->
          Ast.EToAnyException(TUnit, EUnit),
        DamlLf2.Expr
          .newBuilder()
          .setFromAnyException(
            DamlLf2.Expr.FromAnyException.newBuilder().setType(unitTypInterned).setExpr(unitExpr)
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
        DamlLf2.Update.TryCatch
          .newBuilder()
          .setReturnType(unitTypInterned)
          .setTryExpr(unitExpr)
          .setVarInternedStr(0)
          .setCatchExpr(unitExpr)
          .build()
      val tryCatchUpdateProto = DamlLf2.Update.newBuilder().setTryCatch(tryCatchProto).build()
      val tryCatchExprProto = DamlLf2.Expr.newBuilder().setUpdate(tryCatchUpdateProto).build()
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

        val unit = DamlLf2.Unit.newBuilder().build()
        val pkgRef = DamlLf2.PackageRef.newBuilder().setSelf(unit).build
        val modRef =
          DamlLf2.ModuleRef.newBuilder().setPackageRef(pkgRef).setModuleNameInternedDname(0).build()
        val templateTyConName =
          DamlLf2.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(1)
        val ifaceTyConName =
          DamlLf2.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(2)
        val requiredIfaceTyConName =
          DamlLf2.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(3)
        val scalaTemplateTyConName = Ref.TypeConName.assertFromString("noPkgId:Mod:T")
        val scalaIfaceTyConName = Ref.TypeConName.assertFromString("noPkgId:Mod:I")
        val scalaRequiredIfaceTyConName = Ref.TypeConName.assertFromString("noPkgId:Mod:J")

        val signatoryInterface = DamlLf2.Expr
          .newBuilder()
          .setSignatoryInterface(
            DamlLf2.Expr.SignatoryInterface
              .newBuilder()
              .setInterface(ifaceTyConName)
              .setExpr(unitExpr)
              .build()
          )
          .build()

        val observerInterface = DamlLf2.Expr
          .newBuilder()
          .setObserverInterface(
            DamlLf2.Expr.ObserverInterface
              .newBuilder()
              .setInterface(ifaceTyConName)
              .setExpr(unitExpr)
              .build()
          )
          .build()

        val toInterface = DamlLf2.Expr
          .newBuilder()
          .setToInterface(
            DamlLf2.Expr.ToInterface
              .newBuilder()
              .setInterfaceType(ifaceTyConName)
              .setTemplateType(templateTyConName)
              .setTemplateExpr(unitExpr)
              .build()
          )
          .build()

        val fromInterface = DamlLf2.Expr
          .newBuilder()
          .setFromInterface(
            DamlLf2.Expr.FromInterface
              .newBuilder()
              .setInterfaceType(ifaceTyConName)
              .setTemplateType(templateTyConName)
              .setInterfaceExpr(unitExpr)
              .build()
          )
          .build()

        val interfaceTemplateTypeRep = DamlLf2.Expr
          .newBuilder()
          .setInterfaceTemplateTypeRep(
            DamlLf2.Expr.InterfaceTemplateTypeRep
              .newBuilder()
              .setInterface(ifaceTyConName)
              .setExpr(unitExpr)
              .build()
          )
          .build()

        val unsafeFromInterface = DamlLf2.Expr
          .newBuilder()
          .setUnsafeFromInterface(
            DamlLf2.Expr.UnsafeFromInterface
              .newBuilder()
              .setInterfaceType(ifaceTyConName)
              .setTemplateType(templateTyConName)
              .setContractIdExpr(unitExpr)
              .setInterfaceExpr(falseExpr)
              .build()
          )
          .build()

        val toRequiredInterface = DamlLf2.Expr
          .newBuilder()
          .setToRequiredInterface(
            DamlLf2.Expr.ToRequiredInterface
              .newBuilder()
              .setRequiredInterface(requiredIfaceTyConName)
              .setRequiringInterface(ifaceTyConName)
              .setExpr(unitExpr)
              .build()
          )
          .build()
        val fromRequiredInterface = DamlLf2.Expr
          .newBuilder()
          .setFromRequiredInterface(
            DamlLf2.Expr.FromRequiredInterface
              .newBuilder()
              .setRequiredInterface(requiredIfaceTyConName)
              .setRequiringInterface(ifaceTyConName)
              .setExpr(unitExpr)
              .build()
          )
          .build()
        val unsafeFromRequiredInterface = DamlLf2.Expr
          .newBuilder()
          .setUnsafeFromRequiredInterface(
            DamlLf2.Expr.UnsafeFromRequiredInterface
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

    s"decode extended TypeRep iff version < ${LV.Features.templateTypeRepToText}" in {
      val testCases = {
        val typeRepTyConName = DamlLf2.Expr
          .newBuilder()
          .setBuiltin(
            DamlLf2.BuiltinFunction.TYPE_REP_TYCON_NAME
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
          if (version < LV.Features.templateTypeRepToText)
            inside(result) { case Failure(error) => error shouldBe a[Error.Parsing] }
          else
            result shouldBe Success(scala)
        }
      }
    }

    s"decode interface update iff version >= ${LV.Features.basicInterfaces} " in {
      val testCases = {

        val unit = DamlLf2.Unit.newBuilder().build()
        val pkgRef = DamlLf2.PackageRef.newBuilder().setSelf(unit).build
        val modRef =
          DamlLf2.ModuleRef.newBuilder().setPackageRef(pkgRef).setModuleNameInternedDname(0).build()
        val ifaceTyConName =
          DamlLf2.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(2)

        val exerciseInterfaceProto = {
          val exe = DamlLf2.Update.ExerciseInterface
            .newBuilder()
            .setInterface(ifaceTyConName)
            .setChoiceInternedStr(0)
            .setCid(unitExpr)
            .setArg(unitExpr)
            .build()
          DamlLf2.Update.newBuilder().setExerciseInterface(exe).build()
        }

        val exerciseInterfaceScala = Ast.UpdateExerciseInterface(
          Ref.Identifier.assertFromString("noPkgId:Mod:I"),
          Ref.ChoiceName.assertFromString("Choice"),
          EUnit,
          EUnit,
          None,
        )

        val fetchInterfaceProto = {
          val fetch = DamlLf2.Update.FetchInterface
            .newBuilder()
            .setInterface(ifaceTyConName)
            .setCid(unitExpr)
            .build()
          DamlLf2.Update.newBuilder().setFetchInterface(fetch).build()
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
          val proto = DamlLf2.Expr.newBuilder().setUpdate(protoUpdate).build()
          decoder.decodeExprForTest(proto, "test") shouldBe Ast.EUpdate(scala)
        }
      }
    }

    s"translate interface exercise guard iff version >= ${LV.Features.extendedInterfaces}" in {

      val unit = DamlLf2.Unit.newBuilder().build()
      val pkgRef = DamlLf2.PackageRef.newBuilder().setSelf(unit).build
      val modRef =
        DamlLf2.ModuleRef.newBuilder().setPackageRef(pkgRef).setModuleNameInternedDname(0).build()
      val ifaceTyConName =
        DamlLf2.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(2)

      val exerciseInterfaceProto = {
        val exe = DamlLf2.Update.ExerciseInterface
          .newBuilder()
          .setInterface(ifaceTyConName)
          .setChoiceInternedStr(0)
          .setCid(unitExpr)
          .setArg(unitExpr)
          .setGuard(unitExpr)
          .build()
        DamlLf2.Update.newBuilder().setExerciseInterface(exe).build()
      }

      val exerciseInterfaceScala = Ast.UpdateExerciseInterface(
        Ref.Identifier.assertFromString("noPkgId:Mod:I"),
        Ref.ChoiceName.assertFromString("Choice"),
        EUnit,
        EUnit,
        Some(EUnit),
      )

      forEveryVersionSuchThat(_ >= LV.Features.extendedInterfaces) { version =>
        val decoder =
          moduleDecoder(version, ImmArraySeq("Choice"), interfaceDottedNameTable, typeTable)
        val proto = DamlLf2.Expr.newBuilder().setUpdate(exerciseInterfaceProto).build()
        decoder.decodeExprForTest(proto, "test") shouldBe Ast.EUpdate(exerciseInterfaceScala)
      }
    }

    s"decode softFetch iff version >= ${LV.Features.packageUpgrades} " in {
      val dottedNameTable = ImmArraySeq("Mod", "T").map(Ref.DottedName.assertFromString)
      val unit = DamlLf2.Unit.newBuilder().build()
      val pkgRef = DamlLf2.PackageRef.newBuilder().setSelf(unit).build
      val modRef =
        DamlLf2.ModuleRef.newBuilder().setPackageRef(pkgRef).setModuleNameInternedDname(0).build()
      val templateTyConName =
        DamlLf2.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(1)

      val softFetchProto = {
        val exe = DamlLf2.Update.SoftFetch
          .newBuilder()
          .setTemplate(templateTyConName)
          .setCid(unitExpr)
          .build()
        DamlLf2.Update.newBuilder().setSoftFetch(exe).build()
      }

      val softFetchScala = Ast.UpdateSoftFetchTemplate(
        Ref.Identifier.assertFromString("noPkgId:Mod:T"),
        EUnit,
      )

      forEveryVersion { version =>
        val decoder = moduleDecoder(version, ImmArraySeq.empty, dottedNameTable, typeTable)
        val proto = DamlLf2.Expr.newBuilder().setUpdate(softFetchProto).build()
        val result = Try(decoder.decodeExprForTest(proto, "test"))
        if (version >= LV.Features.packageUpgrades)
          result shouldBe Success(Ast.EUpdate(softFetchScala))
        else
          inside(result) { case Failure(error) => error shouldBe an[Error.Parsing] }
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

      val fearureFlags = DamlLf2.FeatureFlags
        .newBuilder()
        .setForbidPartyLiterals(true)
        .setDontDivulgeContractIdsInCreateArguments(true)
        .setDontDiscloseNonConsumingChoicesToObservers(true)
        .build()

      val emptyDefInterface = DamlLf2.DefInterface
        .newBuilder()
        .setTyconInternedDname(2)
        .setParamInternedStr(0)
        .setView(unitTypInterned)
        .build()

      val emptyDefInterfaceScala =
        Ast.DefInterface(
          requires = Set.empty,
          param = Ref.Name.assertFromString("this"),
          choices = Map.empty,
          methods = Map.empty,
          view = TUnit,
        )

      val methodsDefInterface = {
        val interfaceMethod1 =
          DamlLf2.InterfaceMethod.newBuilder().setMethodInternedName(1).setType(textTypInterned)
        val interfaceMethod2 =
          DamlLf2.InterfaceMethod.newBuilder().setMethodInternedName(2).setType(boolTypInterned)

        DamlLf2.DefInterface
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
          requires = Set.empty,
          param = Ref.Name.assertFromString("this"),
          choices = Map.empty,
          methods = Map(
            methodName1 -> Ast.InterfaceMethod(methodName1, TText),
            methodName2 -> Ast.InterfaceMethod(methodName2, TBool),
          ),
          view = TUnit,
        )
      }

      val coImplementsDefInterface = DamlLf2.DefInterface
        .newBuilder()
        .setTyconInternedDname(2)
        .setParamInternedStr(0)
        .setView(unitTypInterned)
        .build()

      val coImplementsDefInterfaceScala =
        Ast.DefInterface(
          requires = Set.empty,
          param = Ref.IdString.Name.assertFromString("this"),
          choices = Map.empty,
          methods = Map.empty,
          view = TUnit,
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
            DamlLf2.Module
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

      val unit = DamlLf2.Unit.newBuilder()
      val pkgRef = DamlLf2.PackageRef.newBuilder().setSelf(unit)
      val modRef =
        DamlLf2.ModuleRef.newBuilder().setPackageRef(pkgRef).setModuleNameInternedDname(0)

      val requiresDefInterface = {
        val typeConNameJ =
          DamlLf2.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(3)
        val typeConNameK =
          DamlLf2.TypeConName.newBuilder().setModule(modRef).setNameInternedDname(4)

        DamlLf2.DefInterface
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
          requires = Set(
            Ref.TypeConName.assertFromString("noPkgId:Mod:J"),
            Ref.TypeConName.assertFromString("noPkgId:Mod:K"),
            Ref.TypeConName.assertFromString("noPkgId:Mod:K"),
          ),
          param = Ref.IdString.Name.assertFromString("this"),
          choices = Map.empty,
          methods = Map.empty,
          view = TUnit,
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
      val dalf1 = dalfProto.getDamlLf2
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
            pr.getSumCase shouldBe DamlLf2.PackageRef.SumCase.PACKAGE_ID_INTERNED_STR
            pr.getPackageIdInternedStr
        }
        .value
      dalf1.getInternedStringsList.asScala.lift(iix.toInt).value
    }

    "take a dalf with interned IDs" in {
      version.major should ===(LV.Major.V2)

      version.minor should !==("dev")

      extId should not be empty
      (extId: String) should !==(pkgId: String)
    }

    "decode resolving the interned package ID" in {
      val decoder = new DecodeV2(version.minor)
      inside(decoder.decodePackage(pkgId, dalfProto.getDamlLf2, false)) { case Right(pkg) =>
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
      forEveryVersion { version =>
        new DecodeV2(version.minor).decodePackageMetadata(
          DamlLf2.PackageMetadata
            .newBuilder()
            .setNameInternedStr(0)
            .setVersionInternedStr(1)
            .build(),
          ImmArraySeq("foobar", "0.0.0"),
        ) shouldBe Ast.PackageMetadata(
          Ref.PackageName.assertFromString("foobar"),
          Ref.PackageVersion.assertFromString("0.0.0"),
          None,
        )
      }
    }

    "reject a package namewith space" in {
      forEveryVersion { version =>
        an[Error.Parsing] shouldBe thrownBy(
          new DecodeV2(version.minor).decodePackageMetadata(
            DamlLf2.PackageMetadata
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
      forEveryVersion { version =>
        an[Error.Parsing] shouldBe thrownBy(
          new DecodeV2(version.minor).decodePackageMetadata(
            DamlLf2.PackageMetadata
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
      forEveryVersion { version =>
        an[Error.Parsing] shouldBe thrownBy(
          new DecodeV2(version.minor).decodePackageMetadata(
            DamlLf2.PackageMetadata
              .newBuilder()
              .setNameInternedStr(0)
              .setVersionInternedStr(1)
              .build(),
            ImmArraySeq("foobar", "0.0.0-"),
          )
        )
      }
    }

    s"decode upgradedPackageId iff version >= ${LV.Features.packageUpgrades} " in {
      forEveryVersion { version =>
        val result = Try(
          new DecodeV2(version.minor).decodePackageMetadata(
            DamlLf2.PackageMetadata
              .newBuilder()
              .setNameInternedStr(0)
              .setVersionInternedStr(1)
              .setUpgradedPackageId(
                DamlLf2.UpgradedPackageId
                  .newBuilder()
                  .setUpgradedPackageIdInternedStr(2)
                  .build()
              )
              .build(),
            ImmArraySeq(
              "foobar",
              "0.0.0",
              "0000000000000000000000000000000000000000000000000000000000000000",
            ),
          )
        )

        if (version >= LV.Features.packageUpgrades)
          result shouldBe Success(
            Ast.PackageMetadata(
              Ref.PackageName.assertFromString("foobar"),
              Ref.PackageVersion.assertFromString("0.0.0"),
              Some(
                Ref.PackageId.assertFromString(
                  "0000000000000000000000000000000000000000000000000000000000000000"
                )
              ),
            )
          )
        else
          inside(result) { case Failure(error) => error shouldBe an[Error.Parsing] }
      }
    }
  }

  "decodePackage" should {

    "require PackageMetadata" in {
      forEveryVersion { version =>
        val decoder = new DecodeV2(version.minor)
        val pkgId = Ref.PackageId.assertFromString(
          "0000000000000000000000000000000000000000000000000000000000000000"
        )
        inside(decoder.decodePackage(pkgId, DamlLf2.Package.newBuilder().build(), false)) {
          case Left(err) => err shouldBe an[Error.Parsing]
        }
      }
    }

    "decode PackageMetadata" in {
      forEveryVersion { version =>
        val decoder = new DecodeV2(version.minor)
        val pkgId = Ref.PackageId.assertFromString(
          "0000000000000000000000000000000000000000000000000000000000000000"
        )
        val metadata =
          DamlLf2.PackageMetadata.newBuilder
            .setNameInternedStr(0)
            .setVersionInternedStr(1)
            .build()
        val pkg = DamlLf2.Package
          .newBuilder()
          .addInternedStrings("foobar")
          .addInternedStrings("0.0.0")
          .setMetadata(metadata)
          .build()
        inside(decoder.decodePackage(pkgId, pkg, false)) { case Right(pkg) =>
          pkg.metadata shouldBe
            Ast.PackageMetadata(
              Ref.PackageName.assertFromString("foobar"),
              Ref.PackageVersion.assertFromString("0.0.0"),
              None,
            )
        }
      }
    }
  }

  "decodeChoice" should {
    val stringTable = ImmArraySeq("SomeChoice", "controllers", "observers", "self", "arg", "body")
    val templateName = Ref.DottedName.assertFromString("Template")
    val controllersExpr = DamlLf2.Expr.newBuilder().setVarInternedStr(1).build()
    val observersExpr = DamlLf2.Expr.newBuilder().setVarInternedStr(2).build()
    val bodyExp = DamlLf2.Expr.newBuilder().setVarInternedStr(5).build()

    // TODO: https://github.com/digital-asset/daml/issues/15882
    // -- When choice authority encode/decode has been implemented,
    // -- test that we reject explicit choice authorizers prior to the feature version.

    "reject choice with observers if 1.7 < lf version < 1.11" in {
      val protoChoiceWithoutObservers = DamlLf2.TemplateChoice
        .newBuilder()
        .setNameInternedStr(0)
        .setConsuming(true)
        .setControllers(controllersExpr)
        .setSelfBinderInternedStr(3)
        .setArgBinder(DamlLf2.VarWithType.newBuilder().setVarInternedStr(4).setType(unitTyp))
        .setRetType(unitTyp)
        .setUpdate(bodyExp)
        .build()

      val protoChoiceWithObservers =
        protoChoiceWithoutObservers.toBuilder.setObservers(observersExpr).build

      forEveryVersionSuchThat(v => v < LV.Features.choiceObservers) { version =>
        val decoder = moduleDecoder(version, stringTable)

        decoder.decodeChoiceForTest(templateName, protoChoiceWithoutObservers)
        an[Error.Parsing] should be thrownBy (
          decoder
            .decodeChoiceForTest(templateName, protoChoiceWithObservers),
        )

      }
    }

    "reject choice without observers if lv version >= 1.11" in {

      val protoChoiceWithoutObservers = DamlLf2.TemplateChoice
        .newBuilder()
        .setNameInternedStr(0)
        .setConsuming(true)
        .setControllers(controllersExpr)
        .setSelfBinderInternedStr(3)
        .setArgBinder(
          DamlLf2.VarWithType.newBuilder().setVarInternedStr(4).setType(unitTypInterned)
        )
        .setRetType(unitTypInterned)
        .setUpdate(bodyExp)
        .build()

      val protoChoiceWithObservers =
        protoChoiceWithoutObservers.toBuilder.setObservers(observersExpr).build

      forEveryVersionSuchThat(LV.Features.choiceObservers <= _) { version =>
        val decoder = moduleDecoder(version, stringTable, ImmArraySeq.empty, typeTable)

        an[Error.Parsing] should be thrownBy (
          decoder.decodeChoiceForTest(templateName, protoChoiceWithoutObservers),
        )
        decoder.decodeChoiceForTest(templateName, protoChoiceWithObservers)
      }
    }
  }

  s"reject experiment expression if LF version < ${LV.Features.unstable}" in {

    val expr = DamlLf2.Expr
      .newBuilder()
      .setExperimental(
        DamlLf2.Expr.Experimental
          .newBuilder()
          .setName("ANSWER")
          .setType(
            DamlLf2.Type
              .newBuilder()
              .setPrim(
                DamlLf2.Type.Prim.newBuilder().setPrim(DamlLf2.PrimType.INT64)
              )
          )
      )
      .build()

    forEveryVersionSuchThat(_ < LV.Features.unstable) { version =>
      val decoder = moduleDecoder(version)
      an[Error.Parsing] shouldBe thrownBy(decoder.decodeExprForTest(expr, "test"))
    }
  }

  s"reject DefValue with no_party_literals = false" in {
    val defValue =
      DamlLf2.DefValue
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
    ) = DamlLf2.FeatureFlags
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
