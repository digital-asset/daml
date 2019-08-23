// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.math.BigDecimal
import java.nio.file.{Files, Paths}

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.archive.DecodeV1.BuiltinFunctionInfo
import com.digitalasset.daml.lf.archive.Reader.ParseError
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion => LV}
import com.digitalasset.daml_lf.DamlLf1
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Inside, Matchers, OptionValues, WordSpec}

import scala.collection.JavaConverters._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class DecodeV1Spec
    extends WordSpec
    with Matchers
    with Inside
    with OptionValues
    with TableDrivenPropertyChecks {

  "The entries of primTypeInfos correspond to Protobuf DamlLf1.PrimType" in {

    (Set(DamlLf1.PrimType.UNRECOGNIZED, DamlLf1.PrimType.DECIMAL) ++
      DecodeV1.builtinTypeInfos.map(_.proto)) shouldBe
      DamlLf1.PrimType.values().toSet

  }

  "The entries of builtinFunctionInfos correspond to Protobuf DamlLf1.BuiltinFunction" in {

    (Set(DamlLf1.BuiltinFunction.UNRECOGNIZED) ++ DecodeV1.builtinFunctionInfos.map(_.proto)) shouldBe
      DamlLf1.BuiltinFunction.values().toSet
  }

  private val dummyModule = DamlLf1.Module
    .newBuilder()
    .setName(DamlLf1.DottedName.newBuilder().addSegments("dummyModule")) build ()

  private def moduleDecoder(minVersion: LV.Minor) =
    new DecodeV1(minVersion)
      .ModuleDecoder(
        Ref.PackageId.assertFromString("noPkgId"),
        ImmArray.empty.toSeq,
        dummyModule,
        onlySerializableDataDefs = false)

  private val preNumericMinVersions = Table(
    "minVersion",
    List(1, 4, 6).map(i => LV.Minor.Stable(i.toString)): _*
  )

  // FixMe: https://github.com/digital-asset/daml/issues/2289
  //        add stable version when numerics are released
  private val postNumericMinVersions = Table(
    "minVersion",
    LV.Minor.Dev
  )

  "decodeKind" should {

    "reject nat kind if lf version < 1.dev" in {

      val input = DamlLf1.Kind.newBuilder().setNat(DamlLf1.Unit.newBuilder()).build()

      forEvery(preNumericMinVersions) { minVersion =>
        an[ParseError] shouldBe thrownBy(moduleDecoder(minVersion).decodeKind(input))
      }
    }

    "accept nat kind if lf version >= 1.dev" in {
      val input = DamlLf1.Kind.newBuilder().setNat(DamlLf1.Unit.newBuilder()).build()

      forEvery(postNumericMinVersions) { minVersion =>
        moduleDecoder(minVersion).decodeKind(input) shouldBe Ast.KNat
      }
    }
  }

  "decodeType" should {

    import DamlLf1.PrimType._

    def buildNat(i: Long) = DamlLf1.Type.newBuilder().setNat(i).build()

    val validNatTypes = List(0, 1, 2, 5, 11, 35, 37, 38)
    val invalidNatTypes = List(Long.MinValue, -100, -2, -1, 39, 40, 200, Long.MaxValue)

    "reject nat type if lf version < 1.dev" in {

      val testCases =
        Table("proto nat type", (validNatTypes.map(_.toLong) ++ invalidNatTypes).map(buildNat): _*)

      forEvery(preNumericMinVersions) { minVersion =>
        val decoder = moduleDecoder(minVersion)
        forEvery(testCases) { natType =>
          an[ParseError] shouldBe thrownBy(decoder.decodeType(natType))
        }
      }
    }

    "accept only valid nat types if lf version >= 1.dev" in {
      val positiveTestCases =
        Table("proto nat type" -> "nat", validNatTypes.map(v => buildNat(v.toLong) -> v): _*)
      val negativeTestCases = Table("proto nat type", invalidNatTypes.map(buildNat): _*)

      forEvery(postNumericMinVersions) { minVersion =>
        val decoder = moduleDecoder(minVersion)
        forEvery(positiveTestCases) { (natType, nat) =>
          decoder.decodeType(natType) shouldBe Ast.TNat(nat)
        }
        forEvery(negativeTestCases) { natType =>
          an[ParseError] shouldBe thrownBy(decoder.decodeType(natType))
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
        TNumeric(Ast.TNat(10)),
      buildPrimType(DECIMAL, buildPrimType(TEXT)) ->
        Ast.TApp(TNumeric(Ast.TNat(10)), TText),
      buildPrimType(ARROW, buildPrimType(TEXT), buildPrimType(DECIMAL)) ->
        TFun(TText, TNumeric(Ast.TNat(10))),
    )

    val numericTestCases = Table(
      "input" -> "expected output",
      buildPrimType(NUMERIC) ->
        TNumeric.cons,
      buildPrimType(NUMERIC, DamlLf1.Type.newBuilder().setNat(10).build()) ->
        TNumeric(Ast.TNat(10)),
      buildPrimType(NUMERIC, buildPrimType(TEXT)) ->
        Ast.TApp(TNumeric.cons, TText),
      buildPrimType(ARROW, buildPrimType(TEXT), buildPrimType(NUMERIC)) ->
        TFun(TText, TNumeric.cons),
    )

    "translate TDecimal to TApp(TNumeric, TNat(10))" in {
      forEvery(preNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)
        forEvery(decimalTestCases) { (input, expectedOutput) =>
          decoder.decodeType(input) shouldBe expectedOutput
        }
      }
    }

    "reject Numeric types if version < 1.dev" in {
      forEvery(preNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)
        forEvery(numericTestCases) { (input, _) =>
          a[ParseError] shouldBe thrownBy(decoder.decodeType(input))
        }
      }
    }

    "translate TNumeric as is if version >= 1.dev" in {
      forEvery(postNumericMinVersions) { minVersion =>
        val decoder = moduleDecoder(minVersion)
        forEvery(numericTestCases) { (input, expectedOutput) =>
          decoder.decodeType(input) shouldBe expectedOutput
        }
      }
    }

    // FixMe: https://github.com/digital-asset/daml/issues/2289
    //  reactive the test once the decoder is not so lenient
    "reject Decimal types if version >= 1.dev" ignore {
      forEvery(postNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)
        forEvery(decimalTestCases) { (input, _) =>
          a[ParseError] shouldBe thrownBy(decoder.decodeType(input))
        }
      }
    }
  }

  "decodeExpr" should {

    def toProto(b: BuiltinFunctionInfo) =
      DamlLf1.Expr.newBuilder().setBuiltin(b.proto).build()

    def toDecimalProto(s: String) =
      DamlLf1.Expr.newBuilder().setPrimLit(DamlLf1.PrimLit.newBuilder().setDecimal(s)).build()

    def toNumericProto(s: String) =
      DamlLf1.Expr.newBuilder().setPrimLit(DamlLf1.PrimLit.newBuilder().setNumeric(s)).build()

    def toScala(b: BuiltinFunctionInfo) =
      Ast.EBuiltin(b.builtin)

    // All the legacy decimal bultins.
    val decimalBuilttins =
      DecodeV1.builtinFunctionInfos.filter(_.handleLegacyDecimal)

    // All the numeric versions of the former.
    val numericBuilttins = {
      val isNumeric = decimalBuilttins.map(_.builtin).toSet
      DecodeV1.builtinFunctionInfos.filter(info =>
        !info.handleLegacyDecimal && isNumeric(info.builtin))
    }

    // Two other unrelated builtins, no need to test more.
    val otherBuiltins =
      DecodeV1.builtinFunctionInfos.filter(
        info =>
          info.proto == DamlLf1.BuiltinFunction.ADD_INT64 ||
            info.proto == DamlLf1.BuiltinFunction.APPEND_TEXT)
    assert(otherBuiltins.length == 2)

    val decimalBuiltinTestCases = Table("decimal builtinInfo", numericBuilttins: _*)
    val numericBuiltinTestCases = Table("numeric builtinInfo", numericBuilttins: _*)
    val negativeBuiltinTestCases = Table("other builtinInfo", otherBuiltins: _*)

    "translate non numeric/decimal builtin as is for any version" in {
      val allVersions = Table("all versions", preNumericMinVersions ++ postNumericMinVersions: _*)

      forEvery(allVersions) { version =>
        val decoder = moduleDecoder(version)
        forEvery(negativeBuiltinTestCases) { info =>
          decoder.decodeExpr(toProto(info), "test") shouldBe toScala(info)
        }
      }
    }

    "transparently apply TNat(10) to Decimal builtins if version < 1.dev" in {

      forEvery(preNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)

        forEvery(decimalBuiltinTestCases) { info =>
          if (LV.ordering.gteq(LV(LV.Major.V1, version), info.minVersion))
            decoder.decodeExpr(toProto(info), "test") shouldBe Ast
              .ETyApp(toScala(info), Ast.TNat(10))
        }
      }
    }

    "reject Numeric builtins if version < 1.dev" in {

      forEvery(preNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)

        forEvery(numericBuiltinTestCases) { info =>
          an[ParseError] shouldBe thrownBy(decoder.decodeExpr(toProto(info), "test"))
        }
      }
    }

    "translate Numeric builtins as is if version >= 1.dev" in {

      forEvery(preNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)

        forEvery(numericBuiltinTestCases) { info =>
          if (LV.ordering.gteq(LV(LV.Major.V1, version), info.minVersion))
            decoder.decodeExpr(toProto(info), "test") shouldBe toScala(info)
        }
      }
    }

    // FixMe: https://github.com/digital-asset/daml/issues/2289
    //  reactive the test once the decoder is not so lenient
    "reject Decimal builtins if version >= 1.dev" ignore {

      forEvery(preNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)

        forEvery(decimalBuiltinTestCases) { info =>
          an[ParseError] shouldBe thrownBy(decoder.decodeExpr(toProto(info), "test"))
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

      forEvery(preNumericMinVersions) { version =>
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

      forEvery(preNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)
        forEvery(testCases) { string =>
          a[ParseError] shouldBe thrownBy(decoder.decodeExpr(toDecimalProto(string), "test"))
        }
      }
    }

    "reject numeric literal if version < 1.dev" in {

      forEvery(preNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)
        a[ParseError] shouldBe thrownBy(decoder.decodeExpr(toNumericProto("0.0"), "test"))
      }
    }

    "parse properly numeric literals" in {

      val testCases =
        Table(
          "string",
          "9999999999999999999999999999.9999999999",
          "0.0000000000",
          "1000000000000000000000000000000.",
          "99999999999999999999999999999999999999.",
          "-0.0",
          "0.",
          "3.1415",
          "-99999999999999999999.999999999999999999"
        )

      forEvery(postNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)
        forEvery(testCases) { string =>
          decoder.decodeExpr(toNumericProto(string), "test") match {
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
          "string",
          "10000000000000000000000000000.0000000000",
          "-1000000000000000000000000000000000000000.",
          "0.000000000000000000000000000000000000001",
          "0000000000000000000000000000.0000000000",
          "0.0.0",
          "+0.0",
          "0"
        )

      forEvery(postNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)
        forEvery(testCases) { string =>
          a[ParseError] shouldBe thrownBy(decoder.decodeExpr(toNumericProto(string), "test"))
        }
      }
    }

    // FixMe: https://github.com/digital-asset/daml/issues/2289
    //  enable the test once the dev decoder is not so lenien
    "reject numeric decimal if version >= 1.dev" ignore {

      forEvery(postNumericMinVersions) { version =>
        val decoder = moduleDecoder(version)
        a[ParseError] shouldBe thrownBy(decoder.decodeExpr(toDecimalProto("0.0"), "test"))
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
          case dv if dv.getNameWithType.getNameList.asScala.lastOption contains "reverseCopy" =>
            val pr = dv.getExpr.getVal.getModule.getPackageRef
            pr.getSumCase shouldBe DamlLf1.PackageRef.SumCase.INTERNED_ID
            pr.getInternedId
        }
        .value
      dalf1.getInternedPackageIdsList.asScala.lift(iix.toInt).value
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
          .lookupIdentifier(Ref.QualifiedName assertFromString "DarReaderTest:reverseCopy")) {
        case Right(
            Ast.DValue(_, _, Ast.ELocation(_, Ast.EVal(Ref.Identifier(resolvedExtId, _))), _)) =>
          (resolvedExtId: String) should ===(extId: String)
      }
    }
  }

}
