// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.util.zip.ZipInputStream
import com.daml.crypto.MessageDigestPrototype
import com.google.protobuf
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class DarReaderTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with Inspectors
    with TryValues {

  private def resource(path: String): ZipInputStream = {
    val stream = getClass.getClassLoader.getResourceAsStream(path)
    new ZipInputStream(stream)
  }

  "should reject a zip bomb with the proper error" in {
    val darFile = "DarReaderTest.dar"
    DarReader
      .readArchive(darFile, resource(darFile), entrySizeThreshold = 1024) shouldBe Left(Error.ZipBomb)
  }

  s"should read LF1 dar file, main archive: DarReaderTest returned first" in {
    val darFile = "DarReaderTest-v115.dar"
    val Right(dar) = DarReader.readArchive(darFile, resource(darFile))

    forAll(dar.all) {
      case ArchivePayload.Lf1(packageId, protoPkg, _) =>
        packageId shouldNot be(Symbol("empty"))
        protoPkg.getModulesCount should be > 0
      case ArchivePayload.Lf2(_, _, _, _) =>
        Assertions.fail("unexpected Lf2 payload")
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
      case ArchivePayload.Lf2(_, _, _, _) =>
        Assertions.fail("unexpected LF2 payload")
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
    val darFile = "DarReaderTest.dar"
    val Right(dar) = DarReader.readArchive(darFile, resource(darFile))

    forAll(dar.all) {
      case ArchivePayload.Lf1(_, _, _) =>
        Assertions.fail("unexpected Lf1 payload")
      case ArchivePayload.Lf2(packageId, protoPkg, _, _) =>
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
      case ArchivePayload.Lf2(_, protoPkg, _, _) =>
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

  private def addUnknownField(
      msg: protobuf.Message,
      i: Int,
      content: protobuf.ByteString,
  ): protobuf.Message = {
    require(!content.isEmpty)
    val builder = msg.toBuilder
    val knownFieldIndex = builder.getDescriptorForType.getFields.asScala.map(_.getNumber).toSet
    assert(!knownFieldIndex(i))
    val field = protobuf.UnknownFieldSet.Field.newBuilder().addLengthDelimited(content).build()
    val extraFields = protobuf.UnknownFieldSet.newBuilder().addField(i, field).build()
    builder.setUnknownFields(extraFields).build()
  }

  val extraData = protobuf.ByteString.fromHex("0123456789abcdef")

  val darFile = "DarReaderTest.dar"
  val Right(dar) = DarParser.readArchive(darFile, resource(darFile))
  val archive: DamlLf.Archive = dar.main

  def testRejectUnknownFields(
      messageName: String
  )(testCases: DamlLf.Archive => (protobuf.ByteString, protobuf.ByteString)) = {
    val (negativeTestCase, positiveTestCase) = testCases(archive)

    s"ArchiveReader should reject $messageName with unknown fields" in {
      ArchiveReader.fromByteString(negativeTestCase) shouldBe a[Right[_, _]]
      inside(ArchiveReader.fromByteString(positiveTestCase)) { case Left(Error.Parsing(err)) =>
        err should include(s"$messageName contains unknown field")
      }
    }

    s"ArchiveSchemaReader should accept $messageName with unknown fields" in {
      ArchiveSchemaReader.fromByteString(negativeTestCase) shouldBe a[Right[_, _]]
      ArchiveSchemaReader.fromByteString(positiveTestCase) shouldBe a[Right[_, _]]
    }
  }

  testRejectUnknownFields("Archive") { archive =>
    val negativeTestCase = archive.toByteString
    val positiveTestCase = addUnknownField(archive, 42, extraData).toByteString
    (negativeTestCase, positiveTestCase)
  }

  private def toProto(archivePayload: protobuf.ByteString): DamlLf.Archive = {
    val hash = MessageDigestPrototype.Sha256.newDigest
      .digest(archivePayload.toByteArray)
      .map("%02x" format _)
      .mkString
    DamlLf.Archive
      .newBuilder()
      .setHashFunction(DamlLf.HashFunction.SHA256)
      .setPayload(archivePayload)
      .setHash(hash)
      .build()
  }

  testRejectUnknownFields("ArchivePayload") { archive =>
    val payload: DamlLf.ArchivePayload = DamlLf.ArchivePayload.parseFrom(archive.getPayload)

    val negativeTestCase =
      toProto(payload.toByteString).toByteString
    val positiveTestCase =
      toProto(addUnknownField(payload, 12, extraData).toByteString).toByteString

    (negativeTestCase, positiveTestCase)
  }

  testRejectUnknownFields("Package") { archive =>
    val payload: DamlLf.ArchivePayload = DamlLf.ArchivePayload.parseFrom(archive.getPayload)
    val pkg = DamlLf2.Package.parseFrom(payload.getDamlLf2)

    val negativeTestCase = toProto(payload.toByteString).toByteString
    val positiveTestCase = {
      val mutatedPayload =
        payload.toBuilder.setDamlLf2(addUnknownField(pkg, 17, extraData).toByteString).build()
      toProto(mutatedPayload.toByteString).toByteString
    }

    (negativeTestCase, positiveTestCase)
  }

}
