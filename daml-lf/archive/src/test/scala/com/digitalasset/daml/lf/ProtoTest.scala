// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import java.nio.file.{Files, Path, Paths}
import java.util.zip.ZipFile

import com.daml.bazeltools.BazelRunfiles._
import com.daml.daml_lf.ArchiveOuterClass.Archive
import com.google.protobuf.CodedInputStream
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class ProtoTest extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  private val darFile = Paths.get(rlocation("daml-lf/archive/DarReaderTest.dar"))

  decodeTest("1_6") { cis =>
    import com.digitalasset.daml_lf_1_6.DamlLf.ArchivePayload
    ArchivePayload.parseFrom(Archive.parseFrom(cis).getPayload).hasDamlLf1
  }

  // DO NOT MODIFY THIS TEST.
  // The test checks the snapshot of the proto definition are not modified.
  checkSnapshot(
    6,
    damlHashes = List(
      "daml_lf.proto",
      "4064870ecefaa3b727d67f0b8fa31590c17c7b11ad9ca7c73f2925013edf410e",
      "6b5990fe8ed2de64e3ec56a0d1d0d852983832c805cf1c3f931a0dae5b8e5ae2",
    ),
    daml0Hashes = List(
      "1554427a861be3fb00a2445711fffe3c54bf0e541be6e748d0a18c5fe01afc03",
      "e79a319ed16dd7cd533d252c2884241759bd9af72f83a4516ff937db53cbcbf8",
    ),
    daml1Hashes = List(
      "daml_lf_1.proto",
      "711b129f810248d20526144d2d56c85fc486b14ba1eb2ab6b13a659a710ad5d2",
      "d75b661509079c12327e18296350aeffb77ca88f8ee110fd1c774ce8c0cb16f0",
    ),
  )

  decodeTest("1_7") { cis =>
    import com.digitalasset.daml_lf_1_7.DamlLf.ArchivePayload
    ArchivePayload.parseFrom(Archive.parseFrom(cis).getPayload).hasDamlLf1
  }

  // DO NOT MODIFY THIS TEST.
  // The test checks the snapshot of the proto definition are not modified.
  checkSnapshot(
    7,
    damlHashes = List(
      "deb1988ae66f7146348dd116a3e5e9eb61d6068423f352a3f8892acda2159431",
      "333b571a41d54def3424bb2ce64e87ec263c74d45dca1985ca5b66d2c00c47fa",
    ),
    daml0Hashes = List(
      "23e3a81020f2133b55c5a263d5130e01ee7d40187a1ecb12581f673a4db68487",
      "a240dcaf4301604403b08163276f8e547b7fddcc2e76e854c76161d5793f1ca3",
    ),
    daml1Hashes = List(
      "dcfff4470a4097cd5e16453193c78f41319750ccb58e098ddaa9fa7fedd919b4",
      "f70b1be35c36c5e949f50193073581a7336caabcfcec859139d81e8d63dc2de3",
    ),
  )

  decodeTest("1_8") { cis =>
    import com.digitalasset.daml_lf_1_8.DamlLf.ArchivePayload
    ArchivePayload.parseFrom(Archive.parseFrom(cis).getPayload).hasDamlLf1
  }

  // DO NOT MODIFY THIS TEST.
  // The test checks the snapshot of the proto definition are not modified.
  checkSnapshot(
    version = 8,
    damlHashes = List(
      "daml_lf.proto",
      "27dd2169bc20c02ca496daa7fc9b06103edbc143e3097373917f6f4b566255b2",
      "ffbf3d5911bc57ff3a9ea2812f93596ba8cefb16d06dcf8bdeb0c32e910520dc",
    ),
    daml0Hashes = List(
      "6bc1965f67dd8010725ae0a55da668de4e1f2609ed322cf1897c4476ecfb312a",
      "79abe84c81428eddf5d2d6c773f9688279f6d06e88fd5895c5a51b5f5e302f55",
    ),
    daml1Hashes = List(
      "4ed8b5a43fee3394926939fd06ad97e630448b47a7ae96a5dbdd7ec6185c0e8d",
      "dda1794a845e5cf262e57e18d3c5266b3a8decf7fbea263c75fefac3a27163b5",
    ),
  )

  decodeTest("1_11") { cis =>
    import com.daml.daml_lf_1_11.DamlLf.ArchivePayload
    ArchivePayload.parseFrom(Archive.parseFrom(cis).getPayload).hasDamlLf1
  }

  // DO NOT MODIFY THIS TEST.
  // The test checks the snapshot of the proto definition are not modified.
  checkSnapshot(
    version = 11,
    damlHashes = List(
      "05eb95f6bb15042624d2ca89d366e3bcd8618934471c6093efeecc09bb9d7df4",
      "be0a1530cfe0727f2078c0db6bd27d15004549d3778beac235ad976d07b507f4",
    ),
    daml0Hashes = List.empty,
    daml1Hashes = List(
      "9a9c86f4072ec08ac292517d377bb07b1436c2b9133da9ba03216c3ae8d3d27c",
      "777d2e86086eeca236d80c6dc4e411690f6dc050ad27dc90f7b7de23f2ce1e93",
    ),
  )

  decodeTest("1_12") { cis =>
    import com.daml.daml_lf_1_12.DamlLf.ArchivePayload
    ArchivePayload.parseFrom(Archive.parseFrom(cis).getPayload).hasDamlLf1
  }

  // DO NOT MODIFY THIS TEST.
  // The test checks the snapshot of the proto definition are not modified.
  checkSnapshot(
    version = 12,
    damlHashes = List(
      "6dbc0a0288c2447af690284e786c3fc1b58a296f2786e9cd5b4053069ff7c045",
      "ac464cafb1cc777bb7a3c62868edffa224b5d314499fa8016423e93226a3903d",
    ),
    daml0Hashes = List.empty,
    daml1Hashes = List(
      "83207610fc117b47ef1da586e36c791706504911ff41cbee8fc5d1da12128147",
      "758fde11797b7db56d90c092309b03ca88024721d1bde5b32a757d5dce81c351",
    ),
  )

  decodeTest("1_13") { cis =>
    import com.daml.daml_lf_1_13.DamlLf.ArchivePayload
    ArchivePayload.parseFrom(Archive.parseFrom(cis).getPayload).hasDamlLf1
  }

  // DO NOT MODIFY THIS TEST.
  // The test checks the snapshot of the proto definition are not modified.
  checkSnapshot(
    version = 13,
    damlHashes = List(
      "2038b49e33825c4730b0119472073f3d5da9b0bd3df2f6d21d9d338c04a49c47",
      "3a00793bbb591746778b13994ba1abb1763dad0612bbdafd88d97f250da37d7d",
    ),
    daml0Hashes = List.empty,
    daml1Hashes = List(
      "6d0869fd8b326cc82f7507ec9deb37520af23ccc4c03a78af623683eb5df2bee",
      "7cc33b6549ce425608858b1cd1c5a56fd4e928bb1df015f8ad24019377eb4154",
    ),
  )

  decodeTest("1_14") { cis =>
    import com.daml.daml_lf_1_14.DamlLf.ArchivePayload
    ArchivePayload.parseFrom(Archive.parseFrom(cis).getPayload).hasDamlLf1
  }

  // DO NOT MODIFY THIS TEST.
  // The test checks the snapshot of the proto definition are not modified.
  checkSnapshot(
    version = 14,
    damlHashes = List(
      "455dfb894ce9648a86dadb408d1ee96c36d180e0f1d625706371ea9eca95c767",
      "0dbd4947753cab68ce8ebab9cfeb0c286a1bf948186429964cbeb9cc9f7ef0dd",
    ),
    daml0Hashes = List.empty,
    daml1Hashes = List(
      "500eefd480e9af6940adf12e7ec4c2cf4975d4cb9b25096c15edb0d57d364de8",
      "13254e65dba50ab348964aadf5e13348ab35e09dc166d7d95d2e54357a457a6b",
    ),
  )

  decodeTest("dev") { cis =>
    import com.daml.daml_lf_dev.DamlLf.ArchivePayload
    ArchivePayload.parseFrom(Archive.parseFrom(cis).getPayload).hasDamlLf1
  }

  private[this] def decodeTest(version: String)(hashPayload: CodedInputStream => Boolean) =
    s"daml_lf_$version.DamlLf" should {
      "read dalf" in {
        val zipFile = new ZipFile(darFile.toFile)
        val entries = zipFile.entries().asScala.filter(_.getName.endsWith(".dalf")).toList

        assert(entries.size >= 2)
        assert(entries.exists(_.getName.contains("daml-stdlib")))

        entries.foreach { entry =>
          val inputStream = zipFile.getInputStream(entry)
          try {
            val cos: CodedInputStream =
              com.google.protobuf.CodedInputStream.newInstance(inputStream)
            hashPayload(cos) shouldBe true
          } finally {
            inputStream.close()
          }
        }
      }
    }

  private[this] def checkSnapshot(
      version: Int,
      damlHashes: List[String], // Linux and Windows hashes for daml_lf.proto
      daml0Hashes: List[String], // Linux and Windows hashes for daml_lf_0.proto
      daml1Hashes: List[String], // Linux and Windows hashes for daml_lf_1.proto
  ) =
    s"daml_lf_1_$version files" should {

      val pkg = if (version < 11) "digitalasset" else "daml"
      val rootDir = s"daml-lf/archive/src/main/protobuf/com/$pkg/daml_lf_1_$version"
      "not be modified" in {
        List(
          "daml_lf.proto" -> damlHashes,
          "daml_lf_0.proto" -> daml0Hashes,
          "daml_lf_1.proto" -> daml1Hashes,
        ).foreach { case (file, expectedHashes) =>
          // on Windows rlocation may return null if the file does not exists
          val maybeHashes =
            Option(rlocation(rootDir + "/" + file)).map(Paths.get(_)).filter(Files.exists(_))
          maybeHashes match {
            case Some(fullPath) =>
              expectedHashes should contain(hashFile(fullPath))
            case None =>
              if (expectedHashes.nonEmpty) fail(s"""file "$file" should exist""")
          }
        }
      }
    }

  private[this] def hashFile(file: Path) = {
    val bytes = Files.readAllBytes(file)
    java.security.MessageDigest.getInstance("SHA-256").digest(bytes).map("%02x" format _).mkString
  }

}
