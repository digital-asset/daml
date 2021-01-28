// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import java.nio.file.{Files, Path, Paths}
import java.util.zip.ZipFile

import com.daml.bazeltools.BazelRunfiles._
import com.digitalasset.{daml_lf_1_6, daml_lf_1_7, daml_lf_1_8}
import com.daml.{daml_lf_1_12, daml_lf_dev}
import com.google.protobuf.CodedInputStream
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class ProtoTest extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  private val darFile = resource(rlocation("daml-lf/archive/DarReaderTest.dar"))

  private def resource(path: String): Path = {
    val f = Paths.get(path)
    require(Files.exists(f), s"File does not exist: $f")
    f
  }

  "daml_lf_dev.DamlLf" should {
    "read dalf" in {

      decodeTestWrapper(
        darFile,
        { cis =>
          val archive = daml_lf_dev.DamlLf.Archive.parseFrom(cis)
          val payload = daml_lf_dev.DamlLf.ArchivePayload.parseFrom(archive.getPayload)
          payload.hasDamlLf1 shouldBe true
        },
      )

    }
  }

  "daml_lf_1_6.DamlLf" should {
    "read dalf" in {
      decodeTestWrapper(
        darFile,
        { cis =>
          val archive = daml_lf_1_6.DamlLf.Archive.parseFrom(cis)
          val payload = daml_lf_1_6.DamlLf.ArchivePayload.parseFrom(archive.getPayload)
          payload.hasDamlLf1 shouldBe true
        },
      )
    }
  }

  "daml_lf_1_6 files" should {

    // Do not change this test.
    // The test checks the snapshot of the proto definition are not modified.

    val rootDir = "daml-lf/archive/src/main/protobuf/com/digitalasset/daml_lf_1_6"

    def resolve(file: String) =
      resource(rlocation(s"$rootDir/$file"))

    "not be modified" in {

      val files = Table(
        ("file", "Linux hash", "windows hash"),
        (
          "daml_lf_0.proto",
          "1554427a861be3fb00a2445711fffe3c54bf0e541be6e748d0a18c5fe01afc03",
          "e79a319ed16dd7cd533d252c2884241759bd9af72f83a4516ff937db53cbcbf8",
        ),
        (
          "daml_lf_1.proto",
          "711b129f810248d20526144d2d56c85fc486b14ba1eb2ab6b13a659a710ad5d2",
          "d75b661509079c12327e18296350aeffb77ca88f8ee110fd1c774ce8c0cb16f0",
        ),
        (
          "daml_lf.proto",
          "4064870ecefaa3b727d67f0b8fa31590c17c7b11ad9ca7c73f2925013edf410e",
          "6b5990fe8ed2de64e3ec56a0d1d0d852983832c805cf1c3f931a0dae5b8e5ae2",
        ),
      )

      forEvery(files) { case (fileName, linuxHash, windowsHash) =>
        List(linuxHash, windowsHash) should contain(hashFile(resolve(fileName)))
      }
    }
  }

  "daml_lf_1_7.DamlLf" should {
    "read dalf" in {
      decodeTestWrapper(
        darFile,
        { cis =>
          val archive = daml_lf_1_7.DamlLf.Archive.parseFrom(cis)
          val payload = daml_lf_1_7.DamlLf.ArchivePayload.parseFrom(archive.getPayload)
          payload.hasDamlLf1 shouldBe true
        },
      )
    }
  }

  "daml_lf_1_7 files" should {

    // Do not change this test.
    // The test checks the snapshot of the proto definition are not modified.

    val rootDir = "daml-lf/archive/src/main/protobuf/com/digitalasset/daml_lf_1_7"

    def resolve(file: String) =
      resource(rlocation(s"$rootDir/$file"))

    "not be modified" in {

      val files = Table(
        ("file", "Linux hash", "windows hash"),
        (
          "daml_lf_0.proto",
          "23e3a81020f2133b55c5a263d5130e01ee7d40187a1ecb12581f673a4db68487",
          "a240dcaf4301604403b08163276f8e547b7fddcc2e76e854c76161d5793f1ca3",
        ),
        (
          "daml_lf_1.proto",
          "dcfff4470a4097cd5e16453193c78f41319750ccb58e098ddaa9fa7fedd919b4",
          "f70b1be35c36c5e949f50193073581a7336caabcfcec859139d81e8d63dc2de3",
        ),
        (
          "daml_lf.proto",
          "deb1988ae66f7146348dd116a3e5e9eb61d6068423f352a3f8892acda2159431",
          "333b571a41d54def3424bb2ce64e87ec263c74d45dca1985ca5b66d2c00c47fa",
        ),
      )

      forEvery(files) { case (fileName, linuxHash, windowsHash) =>
        List(linuxHash, windowsHash) should contain(hashFile(resolve(fileName)))
      }
    }
  }

  "daml_lf_1_8.DamlLf" should {
    "read dalf" in {
      decodeTestWrapper(
        darFile,
        { cis =>
          val archive = daml_lf_1_8.DamlLf.Archive.parseFrom(cis)
          val payload = daml_lf_1_8.DamlLf.ArchivePayload.parseFrom(archive.getPayload)
          payload.hasDamlLf1 shouldBe true
        },
      )
    }
  }

  "daml_lf_1_8 files" should {

    // Do not change this test.
    // The test checks the snapshot of the proto definition are not modified.

    val rootDir = "daml-lf/archive/src/main/protobuf/com/digitalasset/daml_lf_1_8"

    def resolve(file: String) =
      resource(rlocation(s"$rootDir/$file"))

    "not be modified" in {

      val files = Table(
        ("file", "Linux hash", "windows hash"),
        (
          "daml_lf_0.proto",
          "6bc1965f67dd8010725ae0a55da668de4e1f2609ed322cf1897c4476ecfb312a",
          "79abe84c81428eddf5d2d6c773f9688279f6d06e88fd5895c5a51b5f5e302f55",
        ),
        (
          "daml_lf_1.proto",
          "4ed8b5a43fee3394926939fd06ad97e630448b47a7ae96a5dbdd7ec6185c0e8d",
          "dda1794a845e5cf262e57e18d3c5266b3a8decf7fbea263c75fefac3a27163b5",
        ),
        (
          "daml_lf.proto",
          "27dd2169bc20c02ca496daa7fc9b06103edbc143e3097373917f6f4b566255b2",
          "ffbf3d5911bc57ff3a9ea2812f93596ba8cefb16d06dcf8bdeb0c32e910520dc",
        ),
      )

      forEvery(files) { case (fileName, linuxHash, windowsHash) =>
        List(linuxHash, windowsHash) should contain(hashFile(resolve(fileName)))
      }
    }
  }

  "daml_lf_1_11 files" should {

    // Do not change this test.
    // The test checks the snapshot of the proto definition are not modified.

    val rootDir = "daml-lf/archive/src/main/protobuf/com/daml/daml_lf_1_11"

    def resolve(file: String) =
      resource(rlocation(s"$rootDir/$file"))

    "not be modified" in {

      val files = Table(
        ("file", "Linux hash", "windows hash"),
        (
          "daml_lf_1.proto",
          "9a9c86f4072ec08ac292517d377bb07b1436c2b9133da9ba03216c3ae8d3d27c",
          "777d2e86086eeca236d80c6dc4e411690f6dc050ad27dc90f7b7de23f2ce1e93",
        ),
        (
          "daml_lf.proto",
          "05eb95f6bb15042624d2ca89d366e3bcd8618934471c6093efeecc09bb9d7df4",
          "be0a1530cfe0727f2078c0db6bd27d15004549d3778beac235ad976d07b507f4",
        ),
      )

      forEvery(files) { case (fileName, linuxHash, windowsHash) =>
        List(linuxHash, windowsHash) should contain(hashFile(resolve(fileName)))
      }
    }
  }

  // We do not test daml_lf_1_11 as the proto definition is the same as daml_lf_1_12
  // and we test for immutability.
  "daml_lf_1_12.DamlLf" should {
    "read dalf" in {
      decodeTestWrapper(
        darFile,
        { cis =>
          val archive = daml_lf_1_12.DamlLf.Archive.parseFrom(cis)
          val payload = daml_lf_1_12.DamlLf.ArchivePayload.parseFrom(archive.getPayload)
          payload.hasDamlLf1 shouldBe true
        },
      )
    }
  }

  "daml_lf_1_12 files" should {

    // Do not change this test.
    // The test checks the snapshot of the proto definition are not modified.

    val rootDir = "daml-lf/archive/src/main/protobuf/com/daml/daml_lf_1_12"

    def resolve(file: String) =
      resource(rlocation(s"$rootDir/$file"))

    "not be modified" in {

      val files = Table(
        ("file", "Linux hash", "windows hash"),
        (
          "daml_lf_1.proto",
          "83207610fc117b47ef1da586e36c791706504911ff41cbee8fc5d1da12128147",
          "777d2e86086eeca236d80c6dc4e411690f6dc050ad27dc90f7b7de23f2ce1e93",
        ),
        (
          "daml_lf.proto",
          "bdb7d343274c47adad9b753ad12ace4feffab64981f3b8175245f6d7653430c4",
          "be0a1530cfe0727f2078c0db6bd27d15004549d3778beac235ad976d07b507f4",
        ),
      )

      forEvery(files) { case (fileName, linuxHash, windowsHash) =>
        List(linuxHash, windowsHash) should contain(hashFile(resolve(fileName)))
      }
    }
  }

  private def decodeTestWrapper(dar: Path, test: CodedInputStream => Assertion) = {
    val zipFile = new ZipFile(dar.toFile)
    val entries = zipFile.entries().asScala.filter(_.getName.endsWith(".dalf")).toList

    assert(entries.size >= 2)
    assert(entries.exists(_.getName.contains("daml-stdlib")))

    entries.foreach { entry =>
      val inputStream = zipFile.getInputStream(entry)
      try {
        val cos: CodedInputStream = com.google.protobuf.CodedInputStream.newInstance(inputStream)
        test(cos)
      } finally {
        inputStream.close()
      }
    }
  }

  private def hashFile(file: Path) = {
    val bytes = Files.readAllBytes(file)
    java.security.MessageDigest.getInstance("SHA-256").digest(bytes).map("%02x" format _).mkString
  }
}
