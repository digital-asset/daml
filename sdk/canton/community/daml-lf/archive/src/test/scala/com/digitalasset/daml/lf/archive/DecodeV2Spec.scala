// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import com.digitalasset.daml.SafeProto

import java.math.BigDecimal
import java.nio.file.Paths
import com.daml.crypto.MessageDigestPrototype
import com.digitalasset.daml.lf.archive.{DamlLf2 => PLF}
import com.digitalasset.daml.lf.data.{Numeric, Ref}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion => LV}
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.archive.DamlLf2
import com.digitalasset.daml.lf.archive.DamlLf
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{Inside, OptionValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class DecodeV2Spec
    extends AnyWordSpec
    with Matchers
    with Inside
    with OptionValues
    with ScalaCheckPropertyChecks {

  val typeTable = ImmArraySeq(TUnit, TBool, TText)
  val unitTypInterned = DamlLf2.Type.newBuilder().setInternedType(0).build()
  val boolTypInterned = DamlLf2.Type.newBuilder().setInternedType(1).build()
  val textTypInterned = DamlLf2.Type.newBuilder().setInternedType(2).build()

  val unit = DamlLf2.Unit.newBuilder().build()
  val kstar = DamlLf2.Kind.newBuilder().setStar(unit).build()

  val unitExpr: DamlLf2.Expr = DamlLf2.Expr
    .newBuilder()
    .setBuiltinCon(DamlLf2.BuiltinCon.CON_UNIT)
    .build()

  val falseExpr: DamlLf2.Expr = DamlLf2.Expr
    .newBuilder()
    .setBuiltinCon(DamlLf2.BuiltinCon.CON_FALSE)
    .build()

  val unitType = DamlLf2.Type
    .newBuilder()
    .setBuiltin(DamlLf2.Type.Builtin.newBuilder().setBuiltin(DamlLf2.BuiltinType.UNIT))
    .build

  "The entries of primTypeInfos correspond to Protobuf DamlLf2.BuiltinType" in {

    (Set(DamlLf2.BuiltinType.UNRECOGNIZED) ++
      DecodeV2.builtinTypeInfos.map(_.proto)) shouldBe
      DamlLf2.BuiltinType.values().toSet

  }

  "The entries of builtinFunctionInfos correspond to Protobuf DamlLf2.BuiltinFunction" in {

    // TODO(https://github.com/digital-asset/daml/issues/18240): remove these builtins from the
    //  proto and revert s1 to the UNRECOGNIZED singleton.
    val s1 =
      Set(
        DamlLf2.BuiltinFunction.UNRECOGNIZED,
        DamlLf2.BuiltinFunction.BIGNUMERIC_TO_TEXT,
      ) ++ DecodeV2.builtinFunctionInfos.map(_.proto)
    val s2 = DamlLf2.BuiltinFunction.values().toSet
    (s1 -- s2) shouldBe Set.empty
    (s2 -- s1) shouldBe Set.empty

  }

  private[this] val dummyModuleStr = "dummyModule"
  private[this] val dummyModuleName = Ref.DottedName.assertFromString(dummyModuleStr)

  private[this] val lfVersions = LV.allLfVersions

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
      kindTable: ImmArraySeq[Ast.Kind] = ImmArraySeq.empty,
      typeTable: ImmArraySeq[Ast.Type] = ImmArraySeq.empty,
      exprTable: ImmArraySeq[PLF.Expr] = ImmArraySeq.empty,
  ) = {
    new DecodeV2(version.minor).Env(
      Ref.PackageId.assertFromString("noPkgId"),
      stringTable,
      dottedNameTable,
      kindTable,
      typeTable,
      exprTable,
      None,
      Some(dummyModuleName),
      schemaMode = false,
      None,
      Left("package made in com.digitalasset.daml.lf.archive.DecodeV2Spec"),
    )
  }

  "decodeKind" should {

    "accept nat kind" in {
      val input = DamlLf2.Kind.newBuilder().setNat(DamlLf2.Unit.newBuilder()).build()
      forEveryVersion { version =>
        moduleDecoder(version).decodeKindForTest(input) shouldBe Ast.KNat
      }
    }

    "reject if result_interned_kind is set" in {
      val input = DamlLf2.Kind
        .newBuilder()
        .setInternedKind(32)
        .build()

      forEveryVersionSuchThat(!LV.featureFlatArchive.enabledIn(_)) { version =>
        an[Error.Parsing] shouldBe thrownBy(moduleDecoder(version).decodeKindForTest(input))
      }
    }

    "reject arrow with empty arg list" in {
      val input = DamlLf2.Kind
        .newBuilder()
        .setArrow(
          DamlLf2.Kind.Arrow
            .newBuilder()
            .setResult(kstar)
        )
        .build()

      forEveryVersion { version =>
        an[Error.Parsing] shouldBe thrownBy(moduleDecoder(version).decodeKindForTest(input))
      }
    }

    s"Reject local flattening if lf version in ${LV.featureFlatArchive}" in {
      val input = DamlLf2.Kind
        .newBuilder()
        .setArrow(
          DamlLf2.Kind.Arrow
            .newBuilder()
            .addParams(kstar)
            .addParams(kstar)
            .setResult(kstar)
        )
        .build()

      forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
        inside(Try(moduleDecoder(version).decodeKindForTest(input))) {
          case Failure(Error.Parsing(message)) =>
            message should include("Illegal local flattening")
        }
      }
    }

  }

  "decodeKindsTable" should {

    "reject nonempty lfkinds on unsupported versions" in {
      val pkg = DamlLf2.Package
        .newBuilder()
        .addInternedKinds(kstar)
        .build()

      forEveryVersionSuchThat(!LV.featureFlatArchive.enabledIn(_)) { version =>
        val decoder = new DecodeV2(version.minor)
        // TODO: https://github.com/digital-asset/daml/issues/21155
        // convert to proper error catching
        an[Error.Parsing] shouldBe thrownBy(decoder.decodeKindsTable(null, pkg))
      }
    }

    "reject interned in interned kinds table" in {
      val kind = DamlLf2.Kind
        .newBuilder()
        .setInternedKind(42)
        .build()
      val pkg = DamlLf2.Package
        .newBuilder()
        .addInternedKinds(kind)
        .build()

      forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
        val decoder = new DecodeV2(version.minor)
        val env = decoder.Env(
          packageId = Ref.PackageId.assertFromString("noPkgId"),
          optModuleName = Some(dummyModuleName),
        )
        an[Error.IllegalInterning] shouldBe thrownBy(decoder.decodeKindsTable(env, pkg))
      }
    }
  }

  "uncheckedDecodeType" should {

    import DamlLf2.BuiltinType._

    def buildNat(i: Long) = DamlLf2.Type.newBuilder().setNat(i).build()

    val validNatTypes = List(0, 1, 2, 5, 11, 35, 36, 37)
    val invalidNatTypes = List(Long.MinValue, -100, -2, -1, 38, 39, 200, Long.MaxValue)

    "accept only valid nat types" in {
      val positiveTestCases =
        Table("proto nat type" -> "nat", validNatTypes.map(v => buildNat(v.toLong) -> v): _*)
      val negativeTestCases = Table("proto nat type", invalidNatTypes.map(buildNat): _*)

      forEveryVersion { version =>
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

    def buildPrimType(primType: DamlLf2.BuiltinType, args: DamlLf2.Type*) =
      DamlLf2.Type
        .newBuilder()
        .setBuiltin(DamlLf2.Type.Builtin.newBuilder().setBuiltin(primType).addAllArgs(args.asJava))
        .build()

    val numericTestCases = Table(
      "input" -> "expected output",
      buildPrimType(NUMERIC) ->
        TNumeric.cons,
      buildPrimType(NUMERIC, DamlLf2.Type.newBuilder().setNat(10.toLong).build()) ->
        TNumeric(Ast.TNat(Numeric.Scale.assertFromInt(10))),
      buildPrimType(NUMERIC, buildPrimType(TEXT)) ->
        Ast.TApp(TNumeric.cons, TText),
      buildPrimType(ARROW, buildPrimType(TEXT), buildPrimType(NUMERIC)) ->
        TFun(TText, TNumeric.cons),
    )

    "translate TNumeric as is" in {
      forEveryVersionSuchThat(!LV.featureFlatArchive.enabledIn(_)) { version =>
        val decoder = moduleDecoder(version)
        forEvery(numericTestCases) { (input, expectedOutput) =>
          decoder.uncheckedDecodeTypeForTest(input) shouldBe expectedOutput
        }
      }
    }

    "accept Any" in {
      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        decoder.uncheckedDecodeTypeForTest(buildPrimType(ANY)) shouldBe TAny
      }
    }

    s"reject BigNumeric and RoundingMode if version < ${LV.featureBigNumeric}" in {
      forEveryVersionSuchThat(!LV.featureBigNumeric.enabledIn(_)) { version =>
        val decoder = moduleDecoder(version)
        an[Error.Parsing] shouldBe thrownBy(
          decoder.uncheckedDecodeTypeForTest(buildPrimType(BIGNUMERIC))
        )
        an[Error.Parsing] shouldBe thrownBy(
          decoder.uncheckedDecodeTypeForTest(buildPrimType(ROUNDING_MODE))
        )
      }
    }

    s"accept BigNumeric and RoundingMode if version >= ${LV.featureBigNumeric}" in {
      forEveryVersionSuchThat(LV.featureBigNumeric.enabledIn) { version =>
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

      val stringTable = ImmArraySeq("a", "b", "c")
      val stringIdx = stringTable.zipWithIndex.toMap

      def fieldWithUnitWithInterning(s: String) =
        DamlLf2.FieldWithType.newBuilder().setFieldInternedStr(stringIdx(s)).setType(unitType)

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

    s"translate exception types" in {
      val exceptionBuiltinTypes = Table(
        "builtin types",
        DamlLf2.BuiltinType.ANY_EXCEPTION -> Ast.BTAnyException,
      )

      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(exceptionBuiltinTypes) { case (proto, bType) =>
          val result = Try(decoder.uncheckedDecodeTypeForTest(buildPrimType(proto)))
          result shouldBe Success(Ast.TBuiltin(bType))
        }
      }
    }

    s"Reject local flattening if lf version in ${LV.featureFlatArchive}" should {

      "for case VAR (enforced: null)" in {
        val stringTable = ImmArraySeq("a")
        val input = DamlLf2.Type
          .newBuilder()
          .setVar(
            DamlLf2.Type.Var
              .newBuilder()
              .setVarInternedStr(0)
              .addArgs(unitType)
          )
          .build()

        forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
          inside(Try(moduleDecoder(version, stringTable).uncheckedDecodeTypeForTest(input))) {
            case Failure(Error.Parsing(message)) =>
              message should include("Illegal local flattening")
          }
        }
      }

      "for case CON (enforced: null)" in {
        val stringTable = ImmArraySeq("pkgId", "x")
        val dottedNameTable = ImmArraySeq("Mod", "T", "S").map(Ref.DottedName.assertFromString)

        val pkgId = DamlLf2.SelfOrImportedPackageId.newBuilder().setSelfPackageId(unit).build
        val modId =
          DamlLf2.ModuleId.newBuilder().setPackageId(pkgId).setModuleNameInternedDname(0).build()
        val tyConId = DamlLf2.TypeConId.newBuilder().setModule(modId).setNameInternedDname(1)

        val input = DamlLf2.Type
          .newBuilder()
          .setCon(
            DamlLf2.Type.Con
              .newBuilder()
              .setTycon(tyConId)
              .addArgs(unitType)
          )
          .build()

        forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
          inside(
            Try(
              moduleDecoder(version, stringTable, dottedNameTable).uncheckedDecodeTypeForTest(input)
            )
          ) { case Failure(Error.Parsing(message)) =>
            message should include("Illegal local flattening")
          }
        }
      }

      "for case BUILTIN (enforced: null)" in {
        val input = DamlLf2.Type.newBuilder
          .setBuiltin(
            DamlLf2.Type.Builtin
              .newBuilder()
              .setBuiltin(DamlLf2.BuiltinType.BOOL)
              .addArgs(unitType)
          )
          .build()

        forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
          inside(Try(moduleDecoder(version).uncheckedDecodeTypeForTest(input))) {
            case Failure(Error.Parsing(message)) =>
              message should include("Illegal local flattening")
          }
        }
      }

      "for case FORALL (enforced: singleton)" in {
        val stringTable = ImmArraySeq("pkgId", "x")
        val typeVar = DamlLf2.Type
          .newBuilder()
          .setVar(DamlLf2.Type.Var.newBuilder().setVarInternedStr(0))
          .build()
        val xWithStar =
          DamlLf2.TypeVarWithKind.newBuilder().setVarInternedStr(1).setKind(kstar).build()

        val input = DamlLf2.Type.newBuilder
          .setForall(
            DamlLf2.Type.Forall
              .newBuilder()
              .setBody(typeVar)
              .addVars(xWithStar)
              .addVars(xWithStar)
          )
          .build()

        forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
          inside(Try(moduleDecoder(version, stringTable).uncheckedDecodeTypeForTest(input))) {
            case Failure(Error.Parsing(message)) =>
              message should include("Illegal local flattening")
          }
        }
      }
    }
  }

  "decodeType" should {

    "reject non interned type for LF >= 1.11" in {

      val stringTable = ImmArraySeq("pkgId", "x")
      val dottedNameTable = ImmArraySeq("Mod", "T", "S").map(Ref.DottedName.assertFromString)

      val pkgId = DamlLf2.SelfOrImportedPackageId.newBuilder().setSelfPackageId(unit).build
      val modId =
        DamlLf2.ModuleId.newBuilder().setPackageId(pkgId).setModuleNameInternedDname(0).build()
      val tyConId = DamlLf2.TypeConId.newBuilder().setModule(modId).setNameInternedDname(1)
      val tySynId = DamlLf2.TypeSynId.newBuilder().setModule(modId).setNameInternedDname(2)

      def newBuilder = DamlLf2.Type.newBuilder()

      val star = DamlLf2.Kind.newBuilder().setStar(unit).build
      val xWithStar =
        DamlLf2.TypeVarWithKind.newBuilder().setVarInternedStr(1).setKind(star).build()
      val typeVar = newBuilder.setVar(DamlLf2.Type.Var.newBuilder().setVarInternedStr(0)).build()
      val typeBool =
        newBuilder
          .setBuiltin(DamlLf2.Type.Builtin.newBuilder().setBuiltin(DamlLf2.BuiltinType.BOOL))
          .build()
      val xWithBool =
        DamlLf2.FieldWithType.newBuilder.setFieldInternedStr(1).setType(typeBool).build()

      val testCases = Table[DamlLf2.Type](
        "type",
        typeVar,
        newBuilder.setNat(10).build(),
        newBuilder.setSyn(DamlLf2.Type.Syn.newBuilder().setTysyn(tySynId)).build(),
        newBuilder.setCon(DamlLf2.Type.Con.newBuilder().setTycon(tyConId)).build(),
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

  "decodeTypesTable" should {
    "reject interned in interned Types table" in {
      val Type = DamlLf2.Type
        .newBuilder()
        .setInternedType(42)
        .build()
      val pkg = DamlLf2.Package
        .newBuilder()
        .addInternedTypes(Type)
        .build()

      forEveryVersion { version =>
        val decoder = new DecodeV2(version.minor)
        val env = decoder.Env(
          packageId = Ref.PackageId.assertFromString("noPkgId"),
          optModuleName = Some(dummyModuleName),
        )
        an[Error.IllegalInterning] shouldBe thrownBy(decoder.decodeTypesTable(env, pkg))
      }
    }
  }

  "decodeExpr" should {

    def toProtoExpr(b: DamlLf2.BuiltinFunction) =
      DamlLf2.Expr.newBuilder().setBuiltin(b).build()

    // def toNumericProto(s: String): DamlLf2.Expr =
    //  DamlLf2.Expr.newBuilder().setBuiltinLit(DamlLf2.BuiltinLit.newBuilder().setNumeric(s)).build()

    def toNumericProto(id: Int): DamlLf2.Expr =
      DamlLf2.Expr
        .newBuilder()
        .setBuiltinLit(DamlLf2.BuiltinLit.newBuilder().setNumericInternedStr(id))
        .build()

    val numericBuiltinTestCases = Table(
      "numeric builtins" -> "expected output",
      DamlLf2.BuiltinFunction.ADD_NUMERIC -> Ast.EBuiltinFun(Ast.BAddNumeric),
      DamlLf2.BuiltinFunction.SUB_NUMERIC -> Ast.EBuiltinFun(Ast.BSubNumeric),
      DamlLf2.BuiltinFunction.ROUND_NUMERIC -> Ast.EBuiltinFun(Ast.BRoundNumeric),
      DamlLf2.BuiltinFunction.NUMERIC_TO_TEXT -> Ast.EBuiltinFun(Ast.BNumericToText),
      DamlLf2.BuiltinFunction.NUMERIC_TO_INT64 -> Ast.EBuiltinFun(Ast.BNumericToInt64),
    )

    val genericComparisonBuiltinCases = Table(
      "generic comparison builtins" -> "expected output",
      DamlLf2.BuiltinFunction.EQUAL -> Ast.EBuiltinFun(Ast.BEqual),
      DamlLf2.BuiltinFunction.LESS_EQ -> Ast.EBuiltinFun(Ast.BLessEq),
      DamlLf2.BuiltinFunction.LESS -> Ast.EBuiltinFun(Ast.BLess),
      DamlLf2.BuiltinFunction.GREATER_EQ -> Ast.EBuiltinFun(Ast.BGreaterEq),
      DamlLf2.BuiltinFunction.GREATER -> Ast.EBuiltinFun(Ast.BGreater),
    )

    val negativeBuiltinTestCases = Table(
      "other builtins" -> "expected output",
      // We do not need to test all other builtin
      DamlLf2.BuiltinFunction.ADD_INT64 -> Ast.EBuiltinFun(Ast.BAddInt64),
      DamlLf2.BuiltinFunction.APPEND_TEXT -> Ast.EBuiltinFun(Ast.BAppendText),
    )

    val contractIdTextConversionCases = Table(
      "builtin" -> "expected output",
      DamlLf2.BuiltinFunction.CONTRACT_ID_TO_TEXT -> Ast.EBuiltinFun(Ast.BContractIdToText),
    )

    "translate non numeric/decimal builtin as is for any version" in {
      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(negativeBuiltinTestCases) { (proto, scala) =>
          decoder.decodeExprForTest(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    "translate Numeric builtins as is" in {

      forEveryVersion { version =>
        val decoder = moduleDecoder(version)

        forEvery(numericBuiltinTestCases) { (proto, scala) =>
          decoder.decodeExprForTest(toProtoExpr(proto), "test") shouldBe scala
        }
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

      forEveryVersion { version =>
        val decoder = moduleDecoder(version, ImmArraySeq(testCases.map(_._2): _*))
        forEvery(testCases) { (id, string) =>
          inside(decoder.decodeExprForTest(toNumericProto(id), "test")) {
            case Ast.EBuiltinLit(Ast.BLNumeric(num)) =>
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

      forEveryVersion { version =>
        val decoder = moduleDecoder(version, ImmArraySeq("0." +: testCases.map(_._2): _*))
        forEvery(testCases) { (id, _) =>
          decoder.decodeExprForTest(toNumericProto(0), "test")
          an[Error.Parsing] shouldBe thrownBy(decoder.decodeExprForTest(toNumericProto(id), "test"))
        }
      }
    }

    s"translate generic comparison builtins as is" in {
      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(genericComparisonBuiltinCases) { (proto, scala) =>
          decoder.decodeExprForTest(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    s"translate contract id text conversions as is" in {
      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(contractIdTextConversionCases) { (proto, scala) =>
          decoder.decodeExprForTest(toProtoExpr(proto), "test") shouldBe scala
        }
      }
    }

    s"translate BigNumeric builtins iff version in ${LV.featureBigNumeric}" in {
      val exceptionBuiltinCases = Table(
        "exception builtins" -> "expected output",
        DamlLf2.BuiltinFunction.SCALE_BIGNUMERIC ->
          Ast.EBuiltinFun(Ast.BScaleBigNumeric),
        DamlLf2.BuiltinFunction.PRECISION_BIGNUMERIC ->
          Ast.EBuiltinFun(Ast.BPrecisionBigNumeric),
        DamlLf2.BuiltinFunction.ADD_BIGNUMERIC ->
          Ast.EBuiltinFun(Ast.BAddBigNumeric),
        DamlLf2.BuiltinFunction.SUB_BIGNUMERIC ->
          Ast.EBuiltinFun(Ast.BSubBigNumeric),
        DamlLf2.BuiltinFunction.MUL_BIGNUMERIC ->
          Ast.EBuiltinFun(Ast.BMulBigNumeric),
        DamlLf2.BuiltinFunction.DIV_BIGNUMERIC ->
          Ast.EBuiltinFun(Ast.BDivBigNumeric),
        DamlLf2.BuiltinFunction.NUMERIC_TO_BIGNUMERIC ->
          Ast.EBuiltinFun(Ast.BNumericToBigNumeric),
        DamlLf2.BuiltinFunction.BIGNUMERIC_TO_TEXT ->
          Ast.EBuiltinFun(Ast.BBigNumericToText),
      )

      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(exceptionBuiltinCases) { (proto, scala) =>
          val result = Try(decoder.decodeExprForTest(toProtoExpr(proto), "test"))

          if (LV.featureBigNumeric.enabledIn(version))
            result shouldBe Success(scala)
          else
            inside(result) { case Failure(error) => error shouldBe an[Error.Parsing] }
        }
      }
    }

    val roundingModeTestCases = Table(
      "proto" -> "expected rounding Mode",
      DamlLf2.BuiltinLit.RoundingMode.UP -> java.math.RoundingMode.UP,
      DamlLf2.BuiltinLit.RoundingMode.DOWN -> java.math.RoundingMode.DOWN,
      DamlLf2.BuiltinLit.RoundingMode.CEILING -> java.math.RoundingMode.CEILING,
      DamlLf2.BuiltinLit.RoundingMode.FLOOR -> java.math.RoundingMode.FLOOR,
      DamlLf2.BuiltinLit.RoundingMode.HALF_UP -> java.math.RoundingMode.HALF_UP,
      DamlLf2.BuiltinLit.RoundingMode.HALF_DOWN -> java.math.RoundingMode.HALF_DOWN,
      DamlLf2.BuiltinLit.RoundingMode.HALF_EVEN -> java.math.RoundingMode.HALF_EVEN,
      DamlLf2.BuiltinLit.RoundingMode.UNNECESSARY -> java.math.RoundingMode.UNNECESSARY,
    )

    def roundingToProtoExpr(s: DamlLf2.BuiltinLit.RoundingMode): DamlLf2.Expr =
      DamlLf2.Expr
        .newBuilder()
        .setBuiltinLit(DamlLf2.BuiltinLit.newBuilder().setRoundingMode(s))
        .build()

    s"translate RoundingMode iff version  >= ${LV.featureBigNumeric}" in {
      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(roundingModeTestCases) { (proto, scala) =>
          val result =
            Try(decoder.decodeExprForTest(roundingToProtoExpr(proto), "test"))

          if (LV.featureBigNumeric.enabledIn(version))
            result shouldBe Success(Ast.EBuiltinLit(Ast.BLRoundingMode(scala)))
          else
            inside(result) { case Failure(error) => error shouldBe an[Error.Parsing] }
        }
      }

    }

    s"translate exception primitive" in {
      val exceptionBuiltinCases = Table(
        "exception primitive" -> "expected output",
        toProtoExpr(DamlLf2.BuiltinFunction.ANY_EXCEPTION_MESSAGE) ->
          Ast.EBuiltinFun(Ast.BAnyExceptionMessage),
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
        val decoder =
          moduleDecoder(version, ImmArraySeq.empty, ImmArraySeq.empty, ImmArraySeq.empty, typeTable)
        forEvery(exceptionBuiltinCases) { (proto, scala) =>
          val result = Try(decoder.decodeExprForTest(proto, "test"))

          result shouldBe Success(scala)
        }
      }
    }

    s"translate UpdateTryCatch" in {
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
        val decoder =
          moduleDecoder(version, stringTable, ImmArraySeq.empty, ImmArraySeq.empty, typeTable)
        val result = Try(decoder.decodeExprForTest(tryCatchExprProto, "test"))
        result shouldBe Success(tryCatchExprScala)
      }
    }

    val interfaceDottedNameTable =
      ImmArraySeq("Mod", "T", "I", "J").map(Ref.DottedName.assertFromString)

    val interfacePrimitivesDecoder =
      (version: LV) => moduleDecoder(version, dottedNameTable = interfaceDottedNameTable)

    s"decode basic interface primitives" in {
      val testCases = {

        val pkgId = DamlLf2.SelfOrImportedPackageId.newBuilder().setSelfPackageId(unit).build
        val modId =
          DamlLf2.ModuleId.newBuilder().setPackageId(pkgId).setModuleNameInternedDname(0).build()
        val templateTyConId =
          DamlLf2.TypeConId.newBuilder().setModule(modId).setNameInternedDname(1)
        val ifaceTyConId =
          DamlLf2.TypeConId.newBuilder().setModule(modId).setNameInternedDname(2)
        val requiredIfaceTyConName =
          DamlLf2.TypeConId.newBuilder().setModule(modId).setNameInternedDname(3)
        val scalaTemplateTyConName = Ref.TypeConId.assertFromString("noPkgId:Mod:T")
        val scalaIfaceTyConName = Ref.TypeConId.assertFromString("noPkgId:Mod:I")
        val scalaRequiredIfaceTyConName = Ref.TypeConId.assertFromString("noPkgId:Mod:J")

        val signatoryInterface = DamlLf2.Expr
          .newBuilder()
          .setSignatoryInterface(
            DamlLf2.Expr.SignatoryInterface
              .newBuilder()
              .setInterface(ifaceTyConId)
              .setExpr(unitExpr)
              .build()
          )
          .build()

        val observerInterface = DamlLf2.Expr
          .newBuilder()
          .setObserverInterface(
            DamlLf2.Expr.ObserverInterface
              .newBuilder()
              .setInterface(ifaceTyConId)
              .setExpr(unitExpr)
              .build()
          )
          .build()

        val toInterface = DamlLf2.Expr
          .newBuilder()
          .setToInterface(
            DamlLf2.Expr.ToInterface
              .newBuilder()
              .setInterfaceType(ifaceTyConId)
              .setTemplateType(templateTyConId)
              .setTemplateExpr(unitExpr)
              .build()
          )
          .build()

        val fromInterface = DamlLf2.Expr
          .newBuilder()
          .setFromInterface(
            DamlLf2.Expr.FromInterface
              .newBuilder()
              .setInterfaceType(ifaceTyConId)
              .setTemplateType(templateTyConId)
              .setInterfaceExpr(unitExpr)
              .build()
          )
          .build()

        val interfaceTemplateTypeRep = DamlLf2.Expr
          .newBuilder()
          .setInterfaceTemplateTypeRep(
            DamlLf2.Expr.InterfaceTemplateTypeRep
              .newBuilder()
              .setInterface(ifaceTyConId)
              .setExpr(unitExpr)
              .build()
          )
          .build()

        val toRequiredInterface = DamlLf2.Expr
          .newBuilder()
          .setToRequiredInterface(
            DamlLf2.Expr.ToRequiredInterface
              .newBuilder()
              .setRequiredInterface(requiredIfaceTyConName)
              .setRequiringInterface(ifaceTyConId)
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
              .setRequiringInterface(ifaceTyConId)
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
              .setRequiringInterface(ifaceTyConId)
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
          result shouldBe Success(scala)
        }
      }
    }

    s"decode unsafe_from_interface primitive, iff version < LF 2.2" in {
      val pkgId = DamlLf2.SelfOrImportedPackageId.newBuilder().setSelfPackageId(unit).build
      val modId =
        DamlLf2.ModuleId.newBuilder().setPackageId(pkgId).setModuleNameInternedDname(0).build()
      val templateTyConId =
        DamlLf2.TypeConId.newBuilder().setModule(modId).setNameInternedDname(1)
      val ifaceTyConId =
        DamlLf2.TypeConId.newBuilder().setModule(modId).setNameInternedDname(2)
      val scalaTemplateTyConName = Ref.TypeConId.assertFromString("noPkgId:Mod:T")
      val scalaIfaceTyConName = Ref.TypeConId.assertFromString("noPkgId:Mod:I")

      val unsafeFromInterface = DamlLf2.Expr
        .newBuilder()
        .setUnsafeFromInterface(
          DamlLf2.Expr.UnsafeFromInterface
            .newBuilder()
            .setInterfaceType(ifaceTyConId)
            .setTemplateType(templateTyConId)
            .setContractIdExpr(unitExpr)
            .setInterfaceExpr(falseExpr)
            .build()
        )
        .build()
      val expected = Ast.EUnsafeFromInterface(
        interfaceId = scalaIfaceTyConName,
        templateId = scalaTemplateTyConName,
        contractIdExpr = EUnit,
        ifaceExpr = EFalse,
      )

      forEveryVersion { version =>
        val result =
          Try(interfacePrimitivesDecoder(version).decodeExprForTest(unsafeFromInterface, "test"))
        if (LV.featureUnsafeFromInterface.enabledIn(version))
          // featureUnsafeFromInterface is a removed feature
          result shouldBe Success(expected)
        else inside(result) { case Failure(error) => error shouldBe an[Error.Parsing] }
      }
    }

    s"decode extended TypeRep iff version not in ${LV.featureTemplateTypeRepToText}" in {
      val testCases = {
        val typeRepTyConName = DamlLf2.Expr
          .newBuilder()
          .setBuiltin(
            DamlLf2.BuiltinFunction.TYPE_REP_TYCON_NAME
          )
          .build()

        Table(
          "input" -> "expected output",
          typeRepTyConName -> Ast.EBuiltinFun(Ast.BTypeRepTyConName),
        )
      }

      forEveryVersion { version =>
        forEvery(testCases) { (proto, scala) =>
          val result = Try(interfacePrimitivesDecoder(version).decodeExprForTest(proto, "test"))
          if (!LV.featureTemplateTypeRepToText.enabledIn(version))
            inside(result) { case Failure(error) => error shouldBe a[Error.Parsing] }
          else
            result shouldBe Success(scala)
        }
      }
    }

    s"decode interface update" in {
      val testCases = {

        val pkgRef = DamlLf2.SelfOrImportedPackageId.newBuilder().setSelfPackageId(unit).build
        val modRef =
          DamlLf2.ModuleId.newBuilder().setPackageId(pkgRef).setModuleNameInternedDname(0).build()
        val ifaceTyConName =
          DamlLf2.TypeConId.newBuilder().setModule(modRef).setNameInternedDname(2)

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

      forEveryVersion { version =>
        forEvery(testCases) { (protoUpdate, scala) =>
          val decoder =
            moduleDecoder(
              version,
              ImmArraySeq("Choice"),
              interfaceDottedNameTable,
              ImmArraySeq.empty,
              typeTable,
            )
          val proto = DamlLf2.Expr.newBuilder().setUpdate(protoUpdate).build()
          decoder.decodeExprForTest(proto, "test") shouldBe Ast.EUpdate(scala)
        }
      }
    }

    s"translate interface exercise guard iff version in ${LV.featureExtendedInterfaces}" in {

      val pkgRef = DamlLf2.SelfOrImportedPackageId.newBuilder().setSelfPackageId(unit).build
      val modRef =
        DamlLf2.ModuleId.newBuilder().setPackageId(pkgRef).setModuleNameInternedDname(0).build()
      val ifaceTyConName =
        DamlLf2.TypeConId.newBuilder().setModule(modRef).setNameInternedDname(2)

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

      forEveryVersionSuchThat(LV.featureExtendedInterfaces.enabledIn) { version =>
        val decoder =
          moduleDecoder(
            version,
            ImmArraySeq("Choice"),
            interfaceDottedNameTable,
            ImmArraySeq.empty,
            typeTable,
          )
        val proto = DamlLf2.Expr.newBuilder().setUpdate(exerciseInterfaceProto).build()
        decoder.decodeExprForTest(proto, "test") shouldBe Ast.EUpdate(exerciseInterfaceScala)
      }
    }

    "reject Expr if INTERNED is set in unsupported lf version" in {
      val input = DamlLf2.Expr
        .newBuilder()
        .setInternedExpr(0)
        .build()

      forEveryVersionSuchThat(!LV.featureFlatArchive.enabledIn(_)) { version =>
        val decoder = new DecodeV2(version.minor)
        val env = decoder.Env(
          packageId = Ref.PackageId.assertFromString("noPkgId"),
          internedExprs = ImmArraySeq(unitExpr),
          optModuleName = Some(dummyModuleName),
        )
        an[Error.Parsing] shouldBe thrownBy(env.decodeExprForTest(input, ""))
      }
    }

    "reject negative interned expressions" in {
      val input = DamlLf2.Expr
        .newBuilder()
        .setInternedExpr(-1)
        .build()
      forEveryVersion { version =>
        val decoder = new DecodeV2(version.minor)
        val env = decoder.Env(
          packageId = Ref.PackageId.assertFromString("noPkgId"),
          optModuleName = Some(dummyModuleName),
        )
        an[Error.Parsing] shouldBe thrownBy(env.decodeExprForTest(input, ""))
      }
    }

    "accept Expr if INTERNED is set in supported lf version" in {
      val input = DamlLf2.Expr
        .newBuilder()
        .setInternedExpr(0)
        .build()

      forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
        val decoder = new DecodeV2(version.minor)
        val env = decoder.Env(
          packageId = Ref.PackageId.assertFromString("noPkgId"),
          internedExprs = ImmArraySeq(unitExpr),
          optModuleName = Some(dummyModuleName),
        )
        env.decodeExprForTest(input, "test") shouldBe EUnit
      }
    }

    "reject interned in interned expressions table" in {
      val internedZero = DamlLf2.Expr
        .newBuilder()
        .setInternedExpr(0)
        .build()
      val internedOne = DamlLf2.Expr
        .newBuilder()
        .setInternedExpr(1)
        .build()

      forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
        val decoder = new DecodeV2(version.minor)
        val env = decoder.Env(
          packageId = Ref.PackageId.assertFromString("noPkgId"),
          internedExprs = ImmArraySeq(unitExpr, internedZero),
          optModuleName = Some(dummyModuleName),
        )
        an[Error.IllegalInterning] shouldBe thrownBy(env.decodeExprForTest(internedOne, ""))
      }
    }

    "reject non-topologically sorted interning table" in {
      val internedZero = DamlLf2.Expr
        .newBuilder()
        .setInternedExpr(0)
        .build()
      val x = DamlLf2.VarWithType
        .newBuilder()
        .setVarInternedStr(0)
        .setType(
          DamlLf2.Type
            .newBuilder()
            .setBuiltin(DamlLf2.Type.Builtin.newBuilder().setBuiltin(DamlLf2.BuiltinType.UNIT))
        )
        .build()
      val ab = DamlLf2.Expr.Abs
        .newBuilder()
        .addParam(x)
        .setBody(internedZero)
        .build()
      val ab2 = DamlLf2.Expr
        .newBuilder()
        .setAbs(ab)
        .build()

      forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
        val decoder = new DecodeV2(version.minor)
        val env = decoder.Env(
          packageId = Ref.PackageId.assertFromString("noPkgId"),
          internedStrings = ImmArraySeq("arg"),
          internedExprs = ImmArraySeq(ab2),
          optModuleName = Some(dummyModuleName),
        )
        an[Error.IllegalInterning] shouldBe thrownBy(env.decodeExprForTest(internedZero, ""))
      }
    }

    s"Reject local flattening if lf version in ${LV.featureFlatArchive}" should {
      "for case ABS (enforced: singleton)" in {
        val internedZero = DamlLf2.Expr
          .newBuilder()
          .setInternedExpr(0)
          .build()
        val internedOne = DamlLf2.Expr
          .newBuilder()
          .setInternedExpr(1)
          .build()
        val x = DamlLf2.VarWithType
          .newBuilder()
          .setVarInternedStr(0)
          .setType(
            DamlLf2.Type
              .newBuilder()
              .setBuiltin(DamlLf2.Type.Builtin.newBuilder().setBuiltin(DamlLf2.BuiltinType.UNIT))
          )
          .build()
        val ab = DamlLf2.Expr.Abs
          .newBuilder()
          .addParam(x)
          .addParam(x)
          .setBody(internedZero)
          .build()
        val ab2 = DamlLf2.Expr
          .newBuilder()
          .setAbs(ab)
          .build()

        forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
          val decoder = new DecodeV2(version.minor)
          val env = decoder.Env(
            packageId = Ref.PackageId.assertFromString("noPkgId"),
            internedStrings = ImmArraySeq("arg"),
            internedExprs = ImmArraySeq(unitExpr, ab2),
            optModuleName = Some(dummyModuleName),
          )
          inside(Try(env.decodeExprForTest(internedOne, ""))) {
            case Failure(Error.Parsing(message)) =>
              message should include("Illegal local flattening")
          }
        }
      }

      "for case TY_APP (enforced: singleton)" in {
        val internedZero = DamlLf2.Expr
          .newBuilder()
          .setInternedExpr(0)
          .build()
        val internedOne = DamlLf2.Expr
          .newBuilder()
          .setInternedExpr(1)
          .build()

        val tyapp = DamlLf2.Expr
          .newBuilder()
          .setTyApp(
            DamlLf2.Expr.TyApp
              .newBuilder()
              .setExpr(internedZero)
              .addTypes(unitType)
              .addTypes(unitType)
          )
          .build()

        forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
          val decoder = new DecodeV2(version.minor)
          val env = decoder.Env(
            packageId = Ref.PackageId.assertFromString("noPkgId"),
            internedStrings = ImmArraySeq("arg"),
            internedExprs = ImmArraySeq(unitExpr, tyapp),
            optModuleName = Some(dummyModuleName),
          )
          inside(Try(env.decodeExprForTest(internedOne, ""))) {
            case Failure(Error.Parsing(message)) =>
              message should include("Illegal local flattening")
          }
        }
      }

      "for case TY_ABS (enforced: singleton)" in {
        val internedZero = DamlLf2.Expr
          .newBuilder()
          .setInternedExpr(0)
          .build()
        val internedOne = DamlLf2.Expr
          .newBuilder()
          .setInternedExpr(1)
          .build()

        val stringTable = ImmArraySeq("pkgId", "x")
        val xWithStar =
          DamlLf2.TypeVarWithKind.newBuilder().setVarInternedStr(1).setKind(kstar).build()

        val tyabs = DamlLf2.Expr
          .newBuilder()
          .setTyAbs(
            DamlLf2.Expr.TyAbs
              .newBuilder()
              .setBody(internedZero)
              .addParam(xWithStar)
              .addParam(xWithStar)
          )
          .build()

        forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { version =>
          val decoder = new DecodeV2(version.minor)
          val env = decoder.Env(
            packageId = Ref.PackageId.assertFromString("noPkgId"),
            internedStrings = stringTable,
            internedExprs = ImmArraySeq(unitExpr, tyabs),
            optModuleName = Some(dummyModuleName),
          )
          inside(Try(env.decodeExprForTest(internedOne, ""))) {
            case Failure(Error.Parsing(message)) =>
              message should include("Illegal local flattening")
          }
        }
      }
    }

    "accept well-formed QueryNByKey for lf features that support it)" in {
     val protoPkgId = DamlLf2.SelfOrImportedPackageId.newBuilder().setSelfPackageId(unit).build
     val protoModId =
       DamlLf2.ModuleId.newBuilder().setPackageId(protoPkgId).setModuleNameInternedDname(0).build()
      val protoTemplateTyConId =
        DamlLf2.TypeConId.newBuilder().setModule(protoModId).setNameInternedDname(1)

      val query = DamlLf2.Update.QueryNByKey
        .newBuilder()
        .setTemplate(protoTemplateTyConId)
      val update = DamlLf2.Update
        .newBuilder()
        .setQueryNByKey(query)
      val expr = DamlLf2.Expr
        .newBuilder()
        .setUpdate(update)
        .build()

      forEveryVersionSuchThat(LV.featureNUCK.enabledIn(_)) { version =>
        val decoder = new DecodeV2(version.minor)
        val env = decoder.Env(
         packageId = Ref.PackageId.assertFromString("noPkgId"),
          internedDottedNames = ImmArraySeq("Mod", "T").map(Ref.DottedName.assertFromString),
        )
        inside(env.decodeExprForTest(expr, "test")) {
          case Ast.EUpdate(u) =>
            u shouldBe Ast.UpdateQueryNByKey(Ref.TypeConId.assertFromString("noPkgId:Mod:T"))
        }
      }
    }

    "reject well-formed QueryNByKey for lf features that do not support it" in {
     val protoPkgId = DamlLf2.SelfOrImportedPackageId.newBuilder().setSelfPackageId(unit).build
     val protoModId =
       DamlLf2.ModuleId.newBuilder().setPackageId(protoPkgId).setModuleNameInternedDname(0).build()
      val protoTemplateTyConId =
        DamlLf2.TypeConId.newBuilder().setModule(protoModId).setNameInternedDname(1)

      val query = DamlLf2.Update.QueryNByKey
        .newBuilder()
        .setTemplate(protoTemplateTyConId)
      val update = DamlLf2.Update
        .newBuilder()
        .setQueryNByKey(query)
      val expr = DamlLf2.Expr
        .newBuilder()
        .setUpdate(update)
        .build()

      forEveryVersionSuchThat(!LV.featureNUCK.enabledIn(_)) { version =>
        val decoder = new DecodeV2(version.minor)
        val env = decoder.Env(
         packageId = Ref.PackageId.assertFromString("noPkgId"),
          internedDottedNames = ImmArraySeq("Mod", "T").map(Ref.DottedName.assertFromString),
        )
        inside(Try(env.decodeExprForTest(expr, "test"))) {
          case Failure(Error.Parsing(message)) =>
            message should include("Non-unique contract keys is not supported by Daml-LF")
        }
      }
    }
  }

  "decodeModule" should {

    val interfaceDefDecoder = {
      val interfaceDefStringTable = ImmArraySeq("this", "method1", "method2")

      val interfaceDefDottedNameTable =
        ImmArraySeq("Mod", "T", "I", "J", "K").map(Ref.DottedName.assertFromString)

      (version: LV) =>
        moduleDecoder(
          version,
          interfaceDefStringTable,
          interfaceDefDottedNameTable,
          ImmArraySeq.empty,
          typeTable,
        )
    }

    s"decode interface definitions correctly" in {

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

    s"accept interface requires" in {

      val interfaceName = Ref.DottedName.assertFromString("I")

      val unit = DamlLf2.Unit.newBuilder()
      val pkgRef = DamlLf2.SelfOrImportedPackageId.newBuilder().setSelfPackageId(unit)
      val modRef =
        DamlLf2.ModuleId.newBuilder().setPackageId(pkgRef).setModuleNameInternedDname(0)

      val requiresDefInterface = {
        val TypeConIdJ =
          DamlLf2.TypeConId.newBuilder().setModule(modRef).setNameInternedDname(3)
        val TypeConIdK =
          DamlLf2.TypeConId.newBuilder().setModule(modRef).setNameInternedDname(4)

        DamlLf2.DefInterface
          .newBuilder()
          .setTyconInternedDname(1)
          .setParamInternedStr(0)
          .addRequires(TypeConIdJ)
          .addRequires(TypeConIdK)
          .setView(unitTypInterned)
          .build()
      }

      val requiresDefInterfaceScala =
        Ast.DefInterface(
          requires = Set(
            Ref.TypeConId.assertFromString("noPkgId:Mod:J"),
            Ref.TypeConId.assertFromString("noPkgId:Mod:K"),
            Ref.TypeConId.assertFromString("noPkgId:Mod:K"),
          ),
          param = Ref.IdString.Name.assertFromString("this"),
          choices = Map.empty,
          methods = Map.empty,
          view = TUnit,
        )

      forEveryVersion { version =>
        val decoder = interfaceDefDecoder(version)
        val result = Try(decoder.decodeDefInterfaceForTest(interfaceName, requiresDefInterface))
        result shouldBe Success(requiresDefInterfaceScala)
      }
    }

  }

  "decodeModuleRef" should {

    lazy val Right(ArchivePayload.Lf2(pkgId, pkgProto, minorVersion, _)) =
      ArchiveReader.fromFile(Paths.get(getClass.getClassLoader.getResource("DarReaderTest.dalf").toURI))

    lazy val extId = {
      val dalf1 = pkgProto
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
            val pr = dv.getExpr.getVal.getModule.getPackageId
            pr.getSumCase shouldBe DamlLf2.SelfOrImportedPackageId.SumCase.IMPORTED_PACKAGE_ID_INTERNED_STR
            pr.getImportedPackageIdInternedStr
        }
        .value
      dalf1.getInternedStringsList.asScala.lift(iix.toInt).value
    }

    "take a dalf with interned IDs" in {

      minorVersion.isDevVersion should be(false)

      extId should not be empty
      (extId: String) should !==(pkgId: String)
    }

    "decode resolving the interned package ID" in {
      val decoder = new DecodeV2(minorVersion)
      inside(decoder.decodePackage(pkgId, pkgProto, false, 0)) { case Right(pkg) =>
        inside(
          pkg
            .modules(Ref.DottedName.assertFromString("DarReaderTest"))
            .definitions(Ref.DottedName.assertFromString("reverseCopy"))
        ) { case Ast.DValue(_, Ast.ELocation(_, Ast.EVal(Ref.Identifier(resolvedExtId, _)))) =>
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

    s"decode upgradedPackageId" in {
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
        inside(decoder.decodePackage(pkgId, DamlLf2.Package.newBuilder().build(), false, 0)) {
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
        inside(decoder.decodePackage(pkgId, pkg, false, 0)) { case Right(pkg) =>
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

  "decodeArchivePayload" should {

    def exprToArch(expr: DamlLf2.Expr, minor: String) = {
      val internedTZero = DamlLf2.Type
        .newBuilder()
        .setInternedType(0)
        .build()

      val theVal = DamlLf2.DefValue
        .newBuilder()
        .setNameWithType(
          DamlLf2.DefValue.NameWithType
            .newBuilder()
            .setNameInternedDname(0)
            .setType(internedTZero)
        )
        .setExpr(expr)
        .build()

      val metadata =
        DamlLf2.PackageMetadata.newBuilder
          .setNameInternedStr(0)
          .setVersionInternedStr(1)
          .build()

      val ffs = DamlLf2.FeatureFlags
        .newBuilder()
        .setForbidPartyLiterals(true)
        .setDontDivulgeContractIdsInCreateArguments(true)
        .setDontDiscloseNonConsumingChoicesToObservers(true)
        .build()

      val pkg = DamlLf2.Package
        .newBuilder()
        .addInternedTypes(unitType)
        .addInternedStrings("foobar")
        .addInternedStrings("0.0.0")
        .addInternedDottedNames(DamlLf2.InternedDottedName.newBuilder().addSegmentsInternedStr(0))
        .setMetadata(metadata)
        .addModules(DamlLf2.Module.newBuilder().addValues(theVal).setFlags(ffs))
        .build()

      val lf2 = SafeProto.toByteString(pkg) match {
        case Right(v) =>
          v
        case Left(_) =>
          throw new RuntimeException("Failed to toByteString")
      }

      val payload = DamlLf.ArchivePayload
        .newBuilder()
        .setMinor(minor)
        .setDamlLf2(lf2)
        .build()

      val payloadBytes = SafeProto.toByteString(payload) match {
        case Right(v) =>
          v
        case Left(_) =>
          throw new RuntimeException("Failed to toByteString")
      }

      val hash = PackageId.assertFromString(
        MessageDigestPrototype.Sha256.newDigest
          .digest(payload.toByteArray)
          .map("%02x" format _)
          .mkString
      )

      DamlLf.Archive
        .newBuilder()
        .setHashFunction(DamlLf.HashFunction.SHA256)
        .setPayload(payloadBytes)
        .setHash(hash)
        .build()
    }

    def buildLet(n: Int): DamlLf2.Expr = {
      if (n == 0)
        unitExpr
      else
        DamlLf2.Expr
          .newBuilder()
          .setApp(
            DamlLf2.Expr.App
              .newBuilder()
              .setFun(buildLet(n - 1))
              .addArgs(unitExpr)
              .build()
          )
          .build()
    }

    "gracefully fail when expression too deep when version supports expression interning" in {
      forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { _ =>
        inside(Decode.decodeArchive(exprToArch(buildLet(500), "dev"))) { case Left(err) =>
          err shouldBe an[Error.IO]
        }
      }
    }

    "not fail when expression deep but not too deep when version supports expression interning" in {
      forEveryVersionSuchThat(LV.featureFlatArchive.enabledIn) { _ =>
        // explanation for "magic" number:
        //
        // The amount of nested lets is not equal to the proto limit since there
        // are several message layers between each let constructor
        //
        // Subject to change when proto message structure changes (safe to
        // adjust, if with explanation and still reasonably deep)
        Decode.decodeArchive(exprToArch(buildLet(48), "dev")) shouldBe a[Right[_, _]]
      }
    }

    "still accept reasonably deep expressions when version does not support" in {
      forEveryVersionSuchThat(!LV.featureFlatArchive.enabledIn(_)) { _ =>
        // explanation for "magic" number: see above
        Decode.decodeArchive(exprToArch(buildLet(498), "1")) shouldBe a[Right[_, _]]
      }
    }

    val pkgId = Ref.PackageId.assertFromString("-pkgId-")

    val pkg = DamlLf2.Package
      .newBuilder()
      .addInternedStrings("foobar")
      .addInternedStrings("0.0.0")
      .setMetadata(
        DamlLf2.PackageMetadata.newBuilder
          .setNameInternedStr(0)
          .setVersionInternedStr(1)
          .build()
      )
      .build()

    "fail for unknown patch versions" in {
      val pkgId = Ref.PackageId.assertFromString("-pkgId-")

      forEveryVersion { version =>
        Decode.decodeArchivePayload(
          ArchivePayload.Lf2(
            pkgId = pkgId,
            proto = pkg,
            minor = version.minor,
            patch = 0,
          )
        ) shouldBe a[Right[_, _]]

        inside(
          Decode.decodeArchivePayload(
            ArchivePayload.Lf2(
              pkgId = pkgId,
              proto = pkg,
              minor = version.minor,
              patch = 1,
            )
          )
        ) { case Left(err) =>
          err.msg should include(s"Unknown patch version 1 for LF $version")
        }
      }
    }

    "ignore patch versions when decoding only Schema" in {
      forEveryVersion { version =>
        Decode.decodeArchivePayloadSchema(
          ArchivePayload.Lf2(
            pkgId = pkgId,
            proto = pkg,
            minor = version.minor,
            patch = 1,
          )
        ) shouldBe a[Right[_, _]]
      }
    }
  }

  "decodeChoice" should {
    val stringTable = ImmArraySeq("SomeChoice", "controllers", "observers", "self", "arg", "body")
    val templateName = Ref.DottedName.assertFromString("Template")
    val controllersExpr = DamlLf2.Expr.newBuilder().setVarInternedStr(1).build()
    val observersExpr = DamlLf2.Expr.newBuilder().setVarInternedStr(2).build()
    val bodyExp = DamlLf2.Expr.newBuilder().setVarInternedStr(5).build()

    "reject choice without observers" in {

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

      forEveryVersion { version =>
        val decoder =
          moduleDecoder(version, stringTable, ImmArraySeq.empty, ImmArraySeq.empty, typeTable)

        an[Error.Parsing] should be thrownBy (
          decoder.decodeChoiceForTest(templateName, protoChoiceWithoutObservers),
        )
        decoder.decodeChoiceForTest(templateName, protoChoiceWithObservers)
      }
    }
  }

  s"reject experiment expression if LF version not in ${LV.featureUnstable}" in {

    val expr = DamlLf2.Expr
      .newBuilder()
      .setExperimental(
        DamlLf2.Expr.Experimental
          .newBuilder()
          .setName("ANSWER")
          .setType(
            DamlLf2.Type
              .newBuilder()
              .setBuiltin(
                DamlLf2.Type.Builtin.newBuilder().setBuiltin(DamlLf2.BuiltinType.INT64)
              )
          )
      )
      .build()

    forEveryVersionSuchThat(!LV.featureUnstable.enabledIn(_)) { version =>
      val decoder = moduleDecoder(version)
      an[Error.Parsing] shouldBe thrownBy(decoder.decodeExprForTest(expr, "test"))
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
