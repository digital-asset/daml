// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.archive

import java.io.File

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.{Dar, UniversalArchiveReader}
import com.daml.lf.data.Ref.{DottedName, PackageId}
import com.daml.daml_lf_dev.DamlLf
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

class DamlLfEncoderTest
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with BazelRunfiles {

  "dar generated by encoder" should {

    "be readable" in {

      val modules_1_6 = Set[DottedName](
        "BoolMod",
        "ComparisonBuiltinMod",
        "DateMod",
        "DecimalMod",
        "EnumMod",
        "Int64Mod",
        "ListMod",
        "OptionMod",
        "PartyMod",
        "RecordMod",
        "TemplateMod",
        "TextMapMod",
        "TextMod",
        "TimestampMod",
        "UnitMod",
        "VariantMod",
      )

      val modules_1_7 = modules_1_6 + "NumericMod" + "AnyMod"
      val modules_1_8 = modules_1_7 + "SynonymMod"
      val modules_1_11 = modules_1_8 + "GenMapMod"
      val modules_1_13 = modules_1_11 + "BigNumericMod"
      val modules_1_14 = modules_1_13 + "ExceptionMod"
      val modules_1_dev = modules_1_14

      val versions = Table(
        "versions" -> "modules",
        "1.6" -> modules_1_6,
        "1.7" -> modules_1_7,
        "1.8" -> modules_1_8,
        "1.11" -> modules_1_11,
        "1.13" -> modules_1_13,
        "1.14" -> modules_1_14,
        "1.dev" -> modules_1_dev,
      )

      forEvery(versions) { (version, expectedModules) =>
        val dar =
          UniversalArchiveReader()
            .readFile(new File(rlocation(s"daml-lf/encoder/test-$version.dar")))

        dar shouldBe Symbol("success")

        val findModules = dar.toOption.toList.flatMap(getNonEmptyModules).toSet

        findModules diff expectedModules shouldBe Set()
        expectedModules diff findModules shouldBe Set()

      }
    }

  }

  private val preInternalizationVersions = List.range(0, 7).map(_.toString).toSet

  private def getNonEmptyModules(dar: Dar[(PackageId, DamlLf.ArchivePayload)]) = {
    for {
      pkgWithId <- dar.main +: dar.dependencies
      (_, pkg) = pkgWithId
      version = pkg.getMinor
      internedStrings = pkg.getDamlLf1.getInternedStringsList.asScala.toArray
      dottedNames = pkg.getDamlLf1.getInternedDottedNamesList.asScala.map(
        _.getSegmentsInternedStrList.asScala.map(internedStrings(_))
      )
      segments <- pkg.getDamlLf1.getModulesList.asScala.map {
        case mod
            if mod.getSynonymsCount != 0 ||
              mod.getDataTypesCount != 0 ||
              mod.getValuesCount != 0 ||
              mod.getTemplatesCount != 0 =>
          if (preInternalizationVersions(version))
            mod.getNameDname.getSegmentsList.asScala
          else
            dottedNames(mod.getNameInternedDname)

      }
    } yield DottedName.assertFromSegments(segments)
  }

  private implicit def toDottedName(s: String): DottedName =
    DottedName.assertFromString(s)

}
