// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.io.File

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.language.LanguageMajorVersion
import com.daml.daml_lf_dev.DamlLf1
import org.scalatest._

import scala.collection.JavaConverters._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class DarReaderTest
    extends WordSpec
    with Matchers
    with Inside
    with BazelRunfiles
    with Inspectors
    with TryValues {

  private val darFile = resource(rlocation("daml-lf/archive/DarReaderTest.dar"))

  private def resource(path: String): File = {
    val f = new File(path).getAbsoluteFile
    require(f.exists, s"File does not exist: $f")
    f
  }

  s"should read dar file: $darFile, main archive: DarReaderTest returned first" in {

    val dar = DarReaderWithVersion.readArchiveFromFile(darFile).success.value
    val ((_, mainArchive), _) = dar.main

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

    forExactly(1, dar.dependencies) {
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

  private def internedName(
      internedDotted: Seq[DamlLf1.InternedDottedName],
      internedStrings: Seq[String],
      n: Int): String = {
    internedDotted(n).getSegmentsInternedStrList.asScala.map(i => internedStrings(i)).mkString(".")
  }

}
