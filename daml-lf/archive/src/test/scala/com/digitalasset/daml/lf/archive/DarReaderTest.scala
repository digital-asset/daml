// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.io.File

import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml_lf_dev.{DamlLf, DamlLf1}
import org.scalatest.{Inside, Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class DarReaderTest extends WordSpec with Matchers with Inside with BazelRunfiles {

  private val darFile = resource(rlocation("daml-lf/archive/DarReaderTest.dar"))

  private def resource(path: String): File = {
    val f = new File(path).getAbsoluteFile
    require(f.exists, s"File does not exist: $f")
    f
  }

  s"should read dar file: $darFile, main archive: DarReaderTest returned first" in {
    val archives: Try[Dar[((Ref.PackageId, DamlLf.ArchivePayload), LanguageMajorVersion)]] =
      DarReaderWithVersion.readArchiveFromFile(darFile)

    inside(archives) {
      case Success(
          Dar(
            ((packageId1, archive1), LanguageMajorVersion.V1),
            ((packageId2, archive2), LanguageMajorVersion.V1) ::
              ((packageId3, archive3), LanguageMajorVersion.V1) ::
              ((packageId4, archive4), LanguageMajorVersion.V1) ::
              ((packageId5, archive5), LanguageMajorVersion.V1) ::
              ((packageId6, archive6), LanguageMajorVersion.V1) ::
              ((packageId7, archive7), LanguageMajorVersion.V1) ::
              ((packageId8, archive8), LanguageMajorVersion.V1) ::
              ((packageId9, archive9), LanguageMajorVersion.V1) ::
              Nil)) =>
        packageId1 shouldNot be('empty)
        packageId2 shouldNot be('empty)
        packageId3 shouldNot be('empty)
        packageId4 shouldNot be('empty)
        packageId5 shouldNot be('empty)
        packageId6 shouldNot be('empty)
        packageId7 shouldNot be('empty)
        packageId8 shouldNot be('empty)
        packageId9 shouldNot be('empty)
        archive1.getDamlLf1.getModulesCount should be > 0
        archive2.getDamlLf1.getModulesCount should be > 0
        archive3.getDamlLf1.getModulesCount should be > 0
        archive4.getDamlLf1.getModulesCount should be > 0
        archive5.getDamlLf1.getModulesCount should be > 0
        archive6.getDamlLf1.getModulesCount should be > 0
        archive7.getDamlLf1.getModulesCount should be > 0
        archive8.getDamlLf1.getModulesCount should be > 0
        archive9.getDamlLf1.getModulesCount should be > 0

        val archive1Modules = archive1.getDamlLf1.getModulesList.asScala
        val archive1InternedDotted = archive1.getDamlLf1.getInternedDottedNamesList.asScala
        val archive1InternedStrings = archive1.getDamlLf1.getInternedStringsList.asScala
        inside(
          archive1Modules
            .find(
              m =>
                internedName(
                  archive1InternedDotted,
                  archive1InternedStrings,
                  m.getNameInternedDname) == "DarReaderTest")) {
          case Some(module) =>
            val actualTypes: Set[String] =
              module.getDataTypesList.asScala.toSet.map(
                (t: DamlLf1.DefDataType) =>
                  internedName(
                    archive1InternedDotted,
                    archive1InternedStrings,
                    t.getNameInternedDname))
            actualTypes should contain allOf ("Transfer", "Call2", "CallablePayout", "PayOut")
        }

        val archive2Modules = archive2.getDamlLf1.getModulesList.asScala
        val archive2InternedDotted = archive2.getDamlLf1.getInternedDottedNamesList.asScala
        val archive2InternedStrings = archive2.getDamlLf1.getInternedStringsList.asScala
        val archive2ModuleNames: Set[String] = archive2Modules
          .map(m =>
            internedName(archive2InternedDotted, archive2InternedStrings, m.getNameInternedDname))
          .toSet
        archive2ModuleNames shouldBe Set(
          "GHC.Enum",
          "GHC.Show",
          "GHC.Num",
          "GHC.Classes",
          "Control.Exception.Base",
          "GHC.Err",
          "GHC.Base",
          "LibraryModules"
        )
    }
  }

  private def internedName(
      internedDotted: Seq[DamlLf1.InternedDottedName],
      internedStrings: Seq[String],
      n: Int): String = {
    internedDotted(n).getSegmentsInternedStrList.asScala.map(i => internedStrings(i)).mkString(".")
  }
}
