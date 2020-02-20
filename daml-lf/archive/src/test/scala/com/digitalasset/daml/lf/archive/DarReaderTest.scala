// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.io.File

import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml_lf_dev.{DamlLf, DamlLf1}
import org.scalatest.{Inside, Inspectors, Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class DarReaderTest extends WordSpec with Matchers with Inside with BazelRunfiles with Inspectors {

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
      case Success(dar @ Dar(((_, mainArchive), _), dalfDeps)) =>
        forAll(dar.all) {
          case ((packageId, archive), ver) =>
            packageId shouldNot be('empty)
            archive.getDamlLf1.getModulesCount should be > 0
            ver should be(LanguageMajorVersion.V1)
        }

        val mainArchiveModules = mainArchive.getDamlLf1.getModulesList.asScala
        val mainArchiveInternedDotted = mainArchive.getDamlLf1.getInternedDottedNamesList.asScala
        val mainArchiveInternedStrings = mainArchive.getDamlLf1.getInternedStringsList.asScala
        inside(
          mainArchiveModules
            .find(
              m =>
                internedName(
                  mainArchiveInternedDotted,
                  mainArchiveInternedStrings,
                  m.getNameInternedDname) == "DarReaderTest")) {
          case Some(module) =>
            val actualTypes: Set[String] =
              module.getDataTypesList.asScala.toSet.map(
                (t: DamlLf1.DefDataType) =>
                  internedName(
                    mainArchiveInternedDotted,
                    mainArchiveInternedStrings,
                    t.getNameInternedDname))
            actualTypes should contain allOf ("Transfer", "Call2", "CallablePayout", "PayOut")
        }

        forExactly(1, dalfDeps) {
          case ((_, archive), _) =>
            val archiveModules = archive.getDamlLf1.getModulesList.asScala
            val archiveInternedDotted = archive.getDamlLf1.getInternedDottedNamesList.asScala
            val archiveInternedStrings = archive.getDamlLf1.getInternedStringsList.asScala
            val archiveModuleNames = archiveModules
              .map(m =>
                internedName(archiveInternedDotted, archiveInternedStrings, m.getNameInternedDname))
              .toSet
            archiveModuleNames shouldBe Set(
              "GHC.Enum",
              "GHC.Show",
              "GHC.Num",
              "GHC.Classes",
              "Control.Exception.Base",
              "GHC.Err",
              "GHC.Base",
              "LibraryModules")
        }
    }
  }

  private def internedName(
      internedDotted: Seq[DamlLf1.InternedDottedName],
      internedStrings: Seq[String],
      n: Int): String = {
    internedDotted(n).getSegmentsInternedStrList.asScala.map(i => internedStrings(i)).mkString(".")
  }
}
