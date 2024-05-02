// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.io.File
import com.daml.bazeltools.BazelRunfiles
import com.daml.daml_lf_dev.{DamlLf1, DamlLf2}
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

  private def resource(path: String): File = {
    val f = new File(rlocation(path)).getAbsoluteFile
    require(f.exists, s"File does not exist: $f")
    f
  }

  "should reject a zip bomb with the proper error" in {
    val darFile = resource("daml-lf/archive/DarReaderTest.dar")
    DarReader
      .readArchiveFromFile(darFile, entrySizeThreshold = 1024) shouldBe Left(Error.ZipBomb)
  }

  s"should read LF1 dar file, main archive: DarReaderTest returned first" in {
    assume(System.getProperty("hasLegacyDamlc").toLowerCase() != "false")
    val darFile = resource("daml-lf/archive/DarReaderTest-v115.dar")
    val Right(dar) = DarReader.readArchiveFromFile(darFile)

    forAll(dar.all) {
      case ArchivePayload.Lf1(packageId, protoPkg, _) =>
        packageId shouldNot be(Symbol("empty"))
        protoPkg.getModulesCount should be > 0
      case ArchivePayload.Lf2(_, _, _) =>
        Assertions.fail("unexpected Lf1 payload")
    }

    val mainPkg = dar.main.asInstanceOf[ArchivePayload.Lf1].proto

    val mainArchiveModules = mainPkg.getModulesList.asScala
    val mainArchiveInternedDotted = mainPkg.getInternedDottedNamesList.asScala
    val mainArchiveInternedStrings = mainPkg.getInternedStringsList.asScala
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

    forExactly(1, dar.dependencies) {
      case ArchivePayload.Lf1(_, protoPkg, _) =>
        val archiveModules = protoPkg.getModulesList.asScala
        val archiveInternedDotted = protoPkg.getInternedDottedNamesList.asScala
        val archiveInternedStrings = protoPkg.getInternedStringsList.asScala
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
      case ArchivePayload.Lf2(_, _, _) =>
        Assertions.fail("unexpected Lf2 payload")
    }

    def internedName(
        internedDotted: collection.Seq[DamlLf1.InternedDottedName],
        internedStrings: collection.Seq[String],
        n: Int,
    ): String = {
      internedDotted(n).getSegmentsInternedStrList.asScala
        .map(i => internedStrings(i))
        .mkString(".")
    }
  }

  s"should read LF2 dar file, main archive: DarReaderTest returned first" in {
    val darFile = resource("daml-lf/archive/DarReaderTest.dar")
    val Right(dar) = DarReader.readArchiveFromFile(darFile)

    forAll(dar.all) {
      case ArchivePayload.Lf1(_, _, _) =>
        Assertions.fail("unexpected Lf1 payload")
      case ArchivePayload.Lf2(packageId, protoPkg, _) =>
        packageId shouldNot be(Symbol("empty"))
        protoPkg.getModulesCount should be > 0
    }

    val mainPkg = dar.main.asInstanceOf[ArchivePayload.Lf2].proto

    val mainArchiveModules = mainPkg.getModulesList.asScala
    val mainArchiveInternedDotted = mainPkg.getInternedDottedNamesList.asScala
    val mainArchiveInternedStrings = mainPkg.getInternedStringsList.asScala
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
        module.getDataTypesList.asScala.toSet.map((t: DamlLf2.DefDataType) =>
          internedName(
            mainArchiveInternedDotted,
            mainArchiveInternedStrings,
            t.getNameInternedDname,
          )
        )
      actualTypes should contain.allOf("Transfer", "Call2", "CallablePayout", "PayOut")
    }

    forExactly(1, dar.dependencies) {
      case ArchivePayload.Lf1(_, _, _) =>
        Assertions.fail("unexpected Lf1 payload")
      case ArchivePayload.Lf2(_, protoPkg, _) =>
        val archiveModules = protoPkg.getModulesList.asScala
        val archiveInternedDotted = protoPkg.getInternedDottedNamesList.asScala
        val archiveInternedStrings = protoPkg.getInternedStringsList.asScala
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

    def internedName(
        internedDotted: collection.Seq[DamlLf2.InternedDottedName],
        internedStrings: collection.Seq[String],
        n: Int,
    ): String = {
      internedDotted(n).getSegmentsInternedStrList.asScala
        .map(i => internedStrings(i))
        .mkString(".")
    }
  }

}
