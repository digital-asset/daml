// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.archive

import java.io.File
import com.daml.bazeltools.BazelRunfiles
import com.daml.daml_lf_dev.{DamlLf1, DamlLf2}
import com.daml.lf.archive.{
  ArchivePayload,
  Dar,
  DecodeCommon,
  UniversalArchiveDecoder,
  UniversalArchiveReader,
}
import com.daml.lf.data.Ref.DottedName
import com.daml.lf.data.Ref.ModuleName
import com.daml.lf.language.Ast
import com.daml.lf.language.LanguageVersion
import com.daml.lf.language.LanguageVersion.Major
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.Ordering.Implicits.infixOrderingOps

class DamlLfEncoderTest
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with BazelRunfiles {

  "dar generated by encoder" should {

    "be readable" in {

      val modules_1_8 = Set[DottedName](
        "UnitMod",
        "BoolMod",
        "Int64Mod",
        "TextMod",
        "DecimalMod",
        "DateMod",
        "TimestampMod",
        "ListMod",
        "PartyMod",
        "RecordMod",
        "VariantMod",
        "BuiltinMod",
        "TemplateMod",
        "OptionMod",
        "TextMapMod",
        "EnumMod",
        "NumericMod",
        "AnyMod",
        "SynonymMod",
      )
      val modules_1_11 = modules_1_8 + "GenMapMod"
      val modules_1_13 = modules_1_11 + "BigNumericMod"
      val modules_1_14 = modules_1_13 + "ExceptionMod"
      val modules_1_15 = modules_1_14 + "InterfaceMod" + "InterfaceMod0"
      val modules_1_dev = modules_1_15 + "InterfaceExtMod"
      val modules_2_dev = modules_1_dev

      val versions = Table(
        "versions" -> "modules",
        "1.8" -> modules_1_8,
        "1.11" -> modules_1_11,
        "1.13" -> modules_1_13,
        "1.14" -> modules_1_14,
        "1.15" -> modules_1_15,
        "1.dev" -> modules_1_dev,
        "2.dev" -> modules_2_dev,
      )

      forEvery(versions) { (version, expectedModules) =>
        val dar =
          UniversalArchiveReader
            .readFile(new File(rlocation(s"daml-lf/encoder/test-$version.dar")))

        dar shouldBe a[Right[_, _]]

        val findModules = dar.toOption.toList.flatMap(getNonEmptyModules).toSet

        findModules diff expectedModules shouldBe Set()
        expectedModules diff findModules shouldBe Set()

      }
    }

  }

  private def getNonEmptyModules(dar: Dar[ArchivePayload]): Seq[ModuleName] = {
    for {
      payload <- dar.all
      ArchivePayload(_, pkg, version) = payload
      name <- version match {
        case LanguageVersion(Major.V1, _) => getNonEmptyModules(version, pkg.getDamlLf1)
        case LanguageVersion(Major.V2, _) => getNonEmptyModules(pkg.getDamlLf2)
      }
    } yield name
  }

  private def getNonEmptyModules(
      version: LanguageVersion,
      pkg: DamlLf1.Package,
  ): Seq[ModuleName] = {
    val internedStrings = pkg.getInternedStringsList.asScala.toArray
    val dottedNames = pkg.getInternedDottedNamesList.asScala.map(
      _.getSegmentsInternedStrList.asScala.map(internedStrings(_))
    )
    for {
      segments <- pkg.getModulesList.asScala.toSeq.map {
        case mod
            if mod.getSynonymsCount != 0 ||
              mod.getDataTypesCount != 0 ||
              mod.getValuesCount != 0 ||
              mod.getTemplatesCount != 0 =>
          if (version < LanguageVersion.Features.internedStrings)
            mod.getNameDname.getSegmentsList.asScala
          else
            dottedNames(mod.getNameInternedDname)
      }
    } yield DottedName.assertFromSegments(segments)
  }

  private def getNonEmptyModules(pkg: DamlLf2.Package): Seq[DottedName] = {
    val internedStrings = pkg.getInternedStringsList.asScala.toArray
    val dottedNames = pkg.getInternedDottedNamesList.asScala.map(
      _.getSegmentsInternedStrList.asScala.map(internedStrings(_))
    )
    for {
      segments <- pkg.getModulesList.asScala.toSeq.map {
        case mod
            if mod.getSynonymsCount != 0 ||
              mod.getDataTypesCount != 0 ||
              mod.getValuesCount != 0 ||
              mod.getTemplatesCount != 0 =>
          dottedNames(mod.getNameInternedDname)
      }
    } yield DottedName.assertFromSegments(segments)
  }

  "BuiltinMod" should {

    val builtinMod = ModuleName.assertFromString("BuiltinMod")

    "contains all builtins " in {
      forEvery(Table("version", LanguageVersion.All.filter(LanguageVersion.v1_13 <= _): _*)) {
        // We do not check package older that 1.11 as they are used for stable packages only
        version =>
          val Right(dar) =
            UniversalArchiveDecoder
              .readFile(new File(rlocation(s"daml-lf/encoder/test-${version.pretty}.dar")))
          val (_, mainPkg) = dar.main
          val builtinInModule = mainPkg
            .modules(builtinMod)
            .definitions
            .values
            .collect { case Ast.DValue(_, Ast.EBuiltin(builtin), _) => builtin }
            .toSet
          val builtinsInVersion = DecodeCommon.builtinFunctionInfos.collect {
            case DecodeCommon.BuiltinFunctionInfo(_, builtin, minVersion, maxVersion, _)
                if minVersion <= version && maxVersion.forall(version < _) =>
              builtin
          }.toSet

          val missingBuiltins = builtinsInVersion -- builtinInModule
          assert(missingBuiltins.isEmpty, s", missing builtin(s) in BuiltinMod")
          val unexpetedBuiltins = builtinInModule -- builtinsInVersion
          assert(unexpetedBuiltins.isEmpty, s", unexpected builtin(s) in BuiltinMod")
      }
    }
  }

  private implicit def toDottedName(s: String): DottedName =
    DottedName.assertFromString(s)

}
