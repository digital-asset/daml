// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.archive

import java.io.File

import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.{Dar, UniversalArchiveReader}
import com.digitalasset.daml.lf.data.Ref.{DottedName, PackageId}
import com.digitalasset.daml_lf_dev.DamlLf
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

class DamlLfEncoderTest
    extends WordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with BazelRunfiles {

  "dar generated by encoder" should {

    "be readable" in {

      val modules_1_0 = Set[DottedName](
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
        "BuiltinMod"
      )

      val modules_1_1 = modules_1_0 + "OptionMod"
      val modules_1_3 = modules_1_1 + "TextMapMod"
      val modules_1_6 = modules_1_3 + "EnumMod"
      val modules_1_7 = modules_1_6 + "NumericMod"
      val modules_1_8 = modules_1_7 + "SynonymMod"
      val modules_1_dev = modules_1_8 + "GenMapMod"

      val versions = Table(
        "versions" -> "modules",
        "1.0" -> modules_1_0,
        "1.1" -> modules_1_1,
        "1.3" -> modules_1_3,
        "1.6" -> modules_1_6,
        "1.7" -> modules_1_7,
        "1.8" -> modules_1_8,
        "1.dev" -> modules_1_dev
      )

      forEvery(versions) { (version, expectedModules) =>
        val dar =
          UniversalArchiveReader()
            .readFile(new File(rlocation(s"daml-lf/encoder/test-$version.dar")))

        dar shouldBe 'success

        val findModules = dar.toOption.toList.flatMap(getModules).toSet

        findModules shouldBe expectedModules
      }
    }

  }

  private val preInternalizationVersions = List.range(0, 7).map(_.toString).toSet

  private def getModules(dar: Dar[(PackageId, DamlLf.ArchivePayload)]) = {
    for {
      pkgWithId <- dar.main +: dar.dependencies
      (_, pkg) = pkgWithId
      version = pkg.getMinor
      internedStrings = pkg.getDamlLf1.getInternedStringsList.asScala.toArray
      dottedNames = pkg.getDamlLf1.getInternedDottedNamesList.asScala.map(
        _.getSegmentsInternedStrList.asScala.map(internedStrings(_))
      )
      segments <- pkg.getDamlLf1.getModulesList.asScala.map(
        mod =>
          if (preInternalizationVersions(version))
            mod.getNameDname.getSegmentsList.asScala
          else
            dottedNames(mod.getNameInternedDname)
      )
    } yield DottedName.assertFromSegments(segments)
  }

  private implicit def toDottedName(s: String): DottedName =
    DottedName.assertFromString(s)

}
