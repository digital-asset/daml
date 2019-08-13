// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion => LV}
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml_lf.DamlLf1
import com.digitalasset.daml_lf.DamlLf1.PackageRef
import org.scalatest.{Inside, Matchers, OptionValues, WordSpec}
import java.nio.file.{Files, Paths}

import com.digitalasset.daml.lf.archive.DecodeV1.BuiltinInfo
import com.digitalasset.daml.lf.archive.Reader.ParseError
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.JavaConverters._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class DecodeV1Spec
    extends WordSpec
    with Matchers
    with Inside
    with OptionValues
    with TableDrivenPropertyChecks {

  "The entries of primTypeInfos correspond to Protobuf DamlLf1.PrimType" in {

    (DecodeV1.builtinTypeInfos
      .map(_.protoName)
      .toSeq
      .toSet + DamlLf1.PrimType.UNRECOGNIZED) shouldBe
      DamlLf1.PrimType.values().toSet

  }

  "The entries of builtinFunctionInfos correspond to Protobuf DamlLf1.BuiltinFunction" in {
    (DecodeV1.builtinInfos
      .map(_.protoName)
      .toSeq
      .toSet + DamlLf1.BuiltinFunction.UNRECOGNIZED) shouldBe
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

    "reject nat kind if lf version <= 1.6" in {

      val input = DamlLf1.Kind.newBuilder().setNat(DamlLf1.Unit.newBuilder()).build()

      forEvery(preNumericMinVersions) { minVersion =>
        val decoder = moduleDecoder(minVersion)
        an[ParseError] shouldBe thrownBy(decoder.decodeKind(input))
      }
    }

    "accept nat kind if lf version > 1.6" in {
      val input = DamlLf1.Kind.newBuilder().setNat(DamlLf1.Unit.newBuilder()).build()

      forEvery(postNumericMinVersions) { minVersion =>
        val decoder = moduleDecoder(minVersion)
        decoder.decodeKind(input) shouldBe Ast.KNat
      }
    }
  }

  "decodeType" should {

    def buildNat(i: Long) = DamlLf1.Type.newBuilder().setNat(i).build()

    val validNatTypes = List(0, 1, 2, 5, 11, 35, 37, 38)
    val invlidNatTypes = List(Long.MinValue, -100, -2, -1, 39, 40, 200, Long.MaxValue)

    "reject nat type if lf version <= 1.6" in {

      val testCases =
        Table("proto nat type", (validNatTypes.map(_.toLong) ++ invlidNatTypes).map(buildNat): _*)

      forEvery(preNumericMinVersions) { minVersion =>
        val decoder = moduleDecoder(minVersion)
        forEvery(testCases) { natType =>
          an[ParseError] shouldBe thrownBy(decoder.decodeType(natType))
        }
      }
    }

    "accept only valid nat types if lf version > 1.6" in {
      val positiveTestCases =
        Table("proto nat type" -> "nat", validNatTypes.map(v => buildNat(v.toLong) -> v): _*)
      val negativeTestCases = Table("proto nat type", invlidNatTypes.map(buildNat): _*)

      forEvery(postNumericMinVersions) { minVersion =>
        val decoder = moduleDecoder(minVersion)
        forEvery(positiveTestCases) {
          case (natType, nat) =>
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

    val TDecimal = TNumeric(Ast.TNat(10))

    "transparently apply TNat(10) to TNumeric if version =< 1.dev" in {
      import DamlLf1.PrimType._

      val testCases = Table(
        "input" -> "expected output",
        buildPrimType(NUMERIC) ->
          TDecimal,
        buildPrimType(NUMERIC, buildPrimType(TEXT)) ->
          Ast.TApp(TDecimal, TText),
        buildPrimType(ARROW, buildPrimType(TEXT), buildPrimType(NUMERIC)) ->
          TFun(TText, TDecimal),
      )

      forEvery(preNumericMinVersions) { minVersion =>
        val decoder = moduleDecoder(minVersion)
        forEvery(testCases) {
          case (input, expectedOutput) =>
            decoder.decodeType(input) shouldBe expectedOutput
        }
      }
    }

    "translate TNumeric such as if version > 1.6" in {
      import DamlLf1.PrimType._

      val testCases = Table(
        "input" -> "expected output",
        buildPrimType(NUMERIC) ->
          TNumeric.cons,
        buildPrimType(NUMERIC, buildPrimType(TEXT)) ->
          Ast.TApp(TNumeric.cons, TText),
        buildPrimType(ARROW, buildPrimType(TEXT), buildPrimType(NUMERIC)) ->
          TFun(TText, TNumeric.cons),
        buildPrimType(NUMERIC, DamlLf1.Type.newBuilder().setNat(10).build()) ->
          TDecimal,
        buildPrimType(NUMERIC, DamlLf1.Type.newBuilder().setNat(0).build()) ->
          TNumeric(Ast.TNat(0)),
      )

      forEvery(postNumericMinVersions) { minVersion =>
        val decoder = moduleDecoder(minVersion)
        forEvery(testCases) {
          case (input, expectedOutput) =>
            decoder.decodeType(input) shouldBe expectedOutput
        }
      }
    }

  }

  "decodeExpr" should {

    def toProto(b: BuiltinInfo) =
      DamlLf1.Expr.newBuilder().setBuiltin(b.protoName).build()

    def toScala(b: BuiltinInfo) =
      Ast.EBuiltin(b.lfName)

    val numericBuilttins = DecodeV1.builtinInfos.filter(_.decimalLegacy)
    val nonNumericBuiltins =
      DecodeV1.builtinInfos.filter(b =>
        b.protoName == DamlLf1.BuiltinFunction.ADD_INT64 || b.protoName == DamlLf1.BuiltinFunction.APPEND_TEXT)
    assert(nonNumericBuiltins.length == 2)

    "transparently apply TNat(10) to Numeric builtins if version =< 1.dev" in {

      val positiveTestCases = Table("builtinInfo", numericBuilttins.toSeq: _*)
      val negativeTetsCases = Table("builtinInfo", nonNumericBuiltins.toSeq: _*)

      forEvery(preNumericMinVersions) { minVersion =>
        val decoder = moduleDecoder(minVersion)

        forEvery(positiveTestCases) { builtinInfo =>
          if (LV.ordering.gteq(LV(LV.Major.V1, minVersion), builtinInfo.minVersion))
            decoder.decodeExpr(toProto(builtinInfo)) shouldBe Ast
              .ETyApp(toScala(builtinInfo), Ast.TNat(10))
          else
            an[ParseError] shouldBe thrownBy(decoder.decodeExpr(toProto(builtinInfo)))
        }

        forEvery(negativeTetsCases) { builtinInfo =>
          decoder.decodeExpr(toProto(builtinInfo)) shouldBe toScala(builtinInfo)
        }
      }

    }

    "translate TNumeric such as if version > 1.6" in {

      val testCases = Table("builtinInfo", (numericBuilttins.toSeq ++ nonNumericBuiltins.toSeq): _*)

      forEvery(postNumericMinVersions) { minVersion =>
        val decoder = moduleDecoder(minVersion)
        forEvery(testCases) { builtinInfo =>
          decoder.decodeExpr(toProto(builtinInfo)) shouldBe toScala(builtinInfo)
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
          case dv if dv.getNameWithType.getNameList.asScala.lastOption contains "reverseCopy" =>
            val pr = dv.getExpr.getVal.getModule.getPackageRef
            pr.getSumCase shouldBe PackageRef.SumCase.INTERNED_ID
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
