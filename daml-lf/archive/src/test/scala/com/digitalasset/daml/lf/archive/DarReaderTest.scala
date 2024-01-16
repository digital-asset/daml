// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.io.File

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.language.LanguageMajorVersion
import com.daml.daml_lf_dev.DamlLf1
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class DarReaderTest
    extends AnyWordSpec
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

  "should reject a zip bomb with the proper error" in {
    DarReader
      .readArchiveFromFile(darFile, entrySizeThreshold = 1024) shouldBe Left(Error.ZipBomb)
  }

  s"should read dar file: $darFile, main archive: DarReaderTest returned first" in {

    val Right(dar) = DarReader.readArchiveFromFile(darFile)
    val mainArchive = dar.main.proto

    forAll(dar.all) { case ArchivePayload(packageId, archive, ver) =>
      packageId shouldNot be(Symbol("empty"))
      archive.getDamlLf1.getModulesCount should be > 0
      ver.major should be(LanguageMajorVersion.V1)
    }

    val mainArchiveModules = mainArchive.getDamlLf1.getModulesList.asScala
    val mainArchiveInternedDotted = mainArchive.getDamlLf1.getInternedDottedNamesList.asScala
    val mainArchiveInternedStrings = mainArchive.getDamlLf1.getInternedStringsList.asScala
    inside(
      mainArchiveModules
        .find(m =>
          internedName(
            mainArchiveInternedDotted,
            mainArchiveInternedStrings,
            m.getNameInternedDname,
          ) == "DarReaderTest"
        )
    ) { case Some(module) =>
      val actualTypes: Set[String] =
        module.getDataTypesList.asScala.toSet.map((t: DamlLf1.DefDataType) =>
          internedName(
            mainArchiveInternedDotted,
            mainArchiveInternedStrings,
            t.getNameInternedDname,
          )
        )
      actualTypes should contain.allOf("Transfer", "Call2", "CallablePayout", "PayOut")
    }

    forExactly(1, dar.dependencies) { case ArchivePayload(_, archive, _) =>
      val archiveModules = archive.getDamlLf1.getModulesList.asScala
      val archiveInternedDotted = archive.getDamlLf1.getInternedDottedNamesList.asScala
      val archiveInternedStrings = archive.getDamlLf1.getInternedStringsList.asScala
      val archiveModuleNames = archiveModules
        .map(m =>
          internedName(archiveInternedDotted, archiveInternedStrings, m.getNameInternedDname)
        )
        .toSet
      archiveModuleNames shouldBe Set(
        "GHC.Enum",
        "GHC.Show",
        "GHC.Show.Text",
        "GHC.Num",
        "GHC.Stack.Types",
        "GHC.Classes",
        "Control.Exception.Base",
        "GHC.Err",
        "GHC.Base",
        "LibraryModules",
        "GHC.Tuple.Check",
      )
    }
  }

  private def internedName(
      internedDotted: collection.Seq[DamlLf1.InternedDottedName],
      internedStrings: collection.Seq[String],
      n: Int,
  ): String = {
    internedDotted(n).getSegmentsInternedStrList.asScala.map(i => internedStrings(i)).mkString(".")
  }

}
