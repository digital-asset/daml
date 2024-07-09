// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.nio.file.Paths
import com.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.data.{Numeric, Ref}
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion => LV}
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.archive.DamlLf1
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

  import DecodeV1.Features

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

  private[this] val dummyModuleStr = "dummyModule"
  private[this] val dummyModuleName = Ref.DottedName.assertFromString(dummyModuleStr)

  private[this] val lfVersions = LV.AllV1

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

      forEveryVersionSuchThat(_ < Features.numeric) { version =>
        an[Error.Parsing] shouldBe thrownBy(moduleDecoder(version).decodeKindForTest(input))
      }
    }

    "accept nat kind if lf version >= 1.7" in {
      val input = DamlLf1.Kind.newBuilder().setNat(DamlLf1.Unit.newBuilder()).build()
      forEveryVersionSuchThat(_ >= Features.numeric) { version =>
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

      forEveryVersionSuchThat(_ < Features.numeric) { version =>
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

      forEveryVersionSuchThat(_ >= Features.numeric) { version =>
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
        TDecimal,
      buildPrimType(DECIMAL, buildPrimType(TEXT)) ->
        Ast.TApp(TDecimal, TText),
      buildPrimType(ARROW, buildPrimType(TEXT), buildPrimType(DECIMAL)) ->
        TFun(TText, TDecimal),
    )

    val numericTestCases = Table(
      "input" -> "expected output",
      buildPrimType(NUMERIC) ->
        TNumeric.cons,
      buildPrimType(NUMERIC, DamlLf1.Type.newBuilder().setNat(10).build()) ->
        TDecimal,
      buildPrimType(NUMERIC, buildPrimType(TEXT)) ->
        Ast.TApp(TNumeric.cons, TText),
      buildPrimType(ARROW, buildPrimType(TEXT), buildPrimType(NUMERIC)) ->
        TFun(TText, TNumeric.cons),
    )

    "translate TDecimal to TApp(TNumeric, TNat(10))" in {
      forEveryVersionSuchThat(_ < Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(decimalTestCases) { (input, expectedOutput) =>
          decoder.uncheckedDecodeTypeForTest(input) shouldBe expectedOutput
        }
      }
    }

    "reject Numeric types if version < 1.7" in {
      forEveryVersionSuchThat(_ < Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(numericTestCases) { (input, _) =>
          an[Error.Parsing] shouldBe thrownBy(decoder.uncheckedDecodeTypeForTest(input))
        }
      }
    }

    "translate TNumeric as is if version >= 1.7" in {
      forEveryVersionSuchThat(_ >= Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(numericTestCases) { (input, expectedOutput) =>
          decoder.uncheckedDecodeTypeForTest(input) shouldBe expectedOutput
        }
      }
    }

    "reject Decimal types if version >= 1.7" in {
      forEveryVersionSuchThat(_ >= Features.numeric) { version =>
        val decoder = moduleDecoder(version)
        forEvery(decimalTestCases) { (input, _) =>
          an[Error.Parsing] shouldBe thrownBy(decoder.uncheckedDecodeTypeForTest(input))
        }
      }
    }

    "reject Any if version < 1.7" in {
      forEveryVersionSuchThat(_ < Features.anyType) { version =>
        val decoder = moduleDecoder(version)
        an[Error.Parsing] shouldBe thrownBy(decoder.uncheckedDecodeTypeForTest(buildPrimType(ANY)))
      }
    }

    "accept Any if version >= 1.7" in {
      forEveryVersionSuchThat(_ >= Features.anyType) { version =>
        val decoder = moduleDecoder(version)
        decoder.uncheckedDecodeTypeForTest(buildPrimType(ANY)) shouldBe TAny
      }
    }

    s"reject BigNumeric and RoundingMode if version < ${Features.bigNumeric}" in {
      forEveryVersionSuchThat(_ < Features.bigNumeric) { version =>
        val decoder = moduleDecoder(version)
        an[Error.Parsing] shouldBe thrownBy(
          decoder.uncheckedDecodeTypeForTest(buildPrimType(BIGNUMERIC))
        )
        an[Error.Parsing] shouldBe thrownBy(
          decoder.uncheckedDecodeTypeForTest(buildPrimType(ROUNDING_MODE))
        )
      }
    }

    s"accept BigNumeric and RoundingMode if version >= ${Features.bigNumeric}" in {
      forEveryVersionSuchThat(_ >= Features.bigNumeric) { version =>
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

      forEveryVersionSuchThat(_ < Features.internedStrings) { version =>
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

      forEveryVersionSuchThat(_ >= Features.internedStrings) { version =>
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

    s"translate exception types iff version >= ${Features.exceptions}" in {
      val exceptionBuiltinTypes = Table(
        "builtin types",
        DamlLf1.PrimType.ANY_EXCEPTION -> Ast.BTAnyException,
      )

      forEveryVersion { version =>
        val decoder = moduleDecoder(version)
        forEvery(exceptionBuiltinTypes) { case (proto, bType) =>
          val result = Try(decoder.uncheckedDecodeTypeForTest(buildPrimType(proto)))

          if (version >= Features.exceptions)
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

      forEveryVersionSuchThat(_ >= Features.internedTypes) { version =>
        val decoder = moduleDecoder(version, stringTable, dottedNameTable)
        forEvery(testCases)(proto =>
          an[Error.Parsing] shouldBe thrownBy(decoder.decodeTypeForTest(proto))
        )
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

    s"decode interface definitions correctly iff version >= ${Features.basicInterfaces}" in {

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
          requires = Set.empty,
          param = Ref.Name.assertFromString("this"),
          choices = Map.empty,
          methods = Map.empty,
          view = TUnit,
          coImplements = Map.empty,
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
          requires = Set.empty,
          param = Ref.Name.assertFromString("this"),
          choices = Map.empty,
          methods = Map(
            methodName1 -> Ast.InterfaceMethod(methodName1, TText),
            methodName2 -> Ast.InterfaceMethod(methodName2, TBool),
          ),
          view = TUnit,
          coImplements = Map.empty,
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
          requires = Set.empty,
          param = Ref.IdString.Name.assertFromString("this"),
          choices = Map.empty,
          methods = Map.empty,
          view = TUnit,
          coImplements = Map.empty,
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
          if (version < Features.basicInterfaces)
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

    s"accept interface requires iff version >= ${Features.basicInterfaces}" in {

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
          requires = Set(
            Ref.TypeConName.assertFromString("noPkgId:Mod:J"),
            Ref.TypeConName.assertFromString("noPkgId:Mod:K"),
            Ref.TypeConName.assertFromString("noPkgId:Mod:K"),
          ),
          param = Ref.IdString.Name.assertFromString("this"),
          choices = Map.empty,
          methods = Map.empty,
          view = TUnit,
          coImplements = Map.empty,
        )

      forEveryVersion { version =>
        val decoder = interfaceDefDecoder(version)
        val result = Try(decoder.decodeDefInterfaceForTest(interfaceName, requiresDefInterface))
        if (version >= Features.basicInterfaces)
          result shouldBe Success(requiresDefInterfaceScala)
        else
          inside(result) { case Failure(error) => error shouldBe an[Error.Parsing] }
      }
    }

  }

  "decodeModuleRef" should {
    "take a dar with interned IDs" in {
      assume(System.getProperty("hasLegacyDamlc").toLowerCase() != "false")

      lazy val ArchivePayload.Lf1(pkgId, dalfProto, minorVersion) =
        DarReader
          .assertReadArchiveFromFile(
            Paths.get(rlocation("daml-lf/archive/DarReaderTest-v115.dar")).toFile
          )
          .main

      lazy val extId = {
        val iix = dalfProto
          .getModules(0)
          .getValuesList
          .asScala
          .collectFirst {
            case dv
                if dalfProto.getInternedDottedNamesList
                  .asScala(dv.getNameWithType.getNameInternedDname)
                  .getSegmentsInternedStrList
                  .asScala
                  .lastOption
                  .map(x => dalfProto.getInternedStringsList.asScala(x)) contains "reverseCopy" =>
              val pr = dv.getExpr.getVal.getModule.getPackageRef
              pr.getSumCase shouldBe DamlLf1.PackageRef.SumCase.PACKAGE_ID_INTERNED_STR
              pr.getPackageIdInternedStr
          }
          .value
        dalfProto.getInternedStringsList.asScala.lift(iix.toInt).value
      }

      minorVersion should !==("dev")

      extId should not be empty
      (extId: String) should !==(pkgId: String)
    }

  }

  "decodePackageMetadata" should {
    "accept a valid package name and version" in {
      forEveryVersionSuchThat(_ >= Features.packageMetadata) { version =>
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
          None,
        )
      }
    }

    "reject a package namewith space" in {
      forEveryVersionSuchThat(_ >= Features.packageMetadata) { version =>
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
      forEveryVersionSuchThat(_ >= Features.packageMetadata) { version =>
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
      forEveryVersionSuchThat(_ >= Features.packageMetadata) { version =>
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

    s"decode upgradedPackageId iff version >= ${Features.packageUpgrades} " in {
      forEveryVersionSuchThat(_ >= Features.packageMetadata) { version =>
        val result = Try(
          new DecodeV1(version.minor).decodePackageMetadata(
            DamlLf1.PackageMetadata
              .newBuilder()
              .setNameInternedStr(0)
              .setVersionInternedStr(1)
              .setUpgradedPackageId(
                DamlLf1.UpgradedPackageId
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

        if (version >= Features.packageUpgrades)
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
    "reject PackageMetadata if lf version < 1.8" in {
      forEveryVersionSuchThat(_ < Features.packageMetadata) { version =>
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
      forEveryVersionSuchThat(_ >= Features.packageMetadata) { version =>
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
      forEveryVersionSuchThat(_ >= Features.packageMetadata) { version =>
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

      forEveryVersionSuchThat(_ < Features.internedStrings) { version =>
        assert(version < Features.internedStrings)
        assert(version < Features.internedTypes)

        val decoder = moduleDecoder(version)

        decoder.decodeChoiceForTest(templateName, protoChoiceWithoutObservers)
        an[Error.Parsing] should be thrownBy (decoder
          .decodeChoiceForTest(templateName, protoChoiceWithObservers))

      }
    }

    // TODO: https://github.com/digital-asset/daml/issues/15882
    // -- When choice authority encode/decode has been implemented,
    // -- test that we reject explicit choice authorizers prior to the feature version.

    "reject choice with observers if 1.7 < lf version < 1.11" in {
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

      forEveryVersionSuchThat(v => Features.internedStrings < v && v < Features.choiceObservers) {
        version =>
          assert(Features.internedStrings <= version)
          assert(version < Features.internedTypes)

          val decoder = moduleDecoder(version, stringTable)

          decoder.decodeChoiceForTest(templateName, protoChoiceWithoutObservers)
          an[Error.Parsing] should be thrownBy (
            decoder
              .decodeChoiceForTest(templateName, protoChoiceWithObservers),
          )

      }
    }

    "reject choice without observers if lv version >= 1.11" in {

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

      forEveryVersionSuchThat(Features.choiceObservers <= _) { version =>
        assert(Features.internedStrings <= version)
        assert(Features.internedTypes <= version)

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

    "reject interned types if lf version < 1.11" in {
      forEveryVersionSuchThat(_ < Features.internedTypes) { version =>
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
