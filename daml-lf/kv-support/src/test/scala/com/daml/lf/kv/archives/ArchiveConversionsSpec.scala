// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.kv.archives

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.lf.data.Ref
import com.daml.lf.kv.ConversionError
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ArchiveConversionsSpec extends AnyWordSpec with Matchers {

  "parsePackageId" should {
    "successfully parse a package ID" in {
      ArchiveConversions.parsePackageId(
        RawArchive(Archive.newBuilder().setHash("aPackageId").build().toByteString)
      ) shouldBe Right(Ref.PackageId.assertFromString("aPackageId"))
    }

    "fail on a broken archive" in {
      ArchiveConversions.parsePackageId(RawArchive(ByteString.copyFromUtf8("wrong"))) shouldBe Left(
        ConversionError.ParseError(
          "IO error: com.google.protobuf.InvalidProtocolBufferException$InvalidWireTypeException: Protocol message tag had invalid wire type."
        )
      )
    }

    "fail on a broken package ID" in {
      ArchiveConversions.parsePackageId(
        RawArchive(Archive.newBuilder().setHash("???").build().toByteString)
      ) shouldBe Left(
        ConversionError.ParseError("non expected character 0x3f in Daml-LF Package ID \"???\"")
      )
    }
  }

  "parsePackageIdsAndRawArchives" should {
    "successfully parse package IDs and raw archives" in {
      val packageId1 = Ref.PackageId.assertFromString("packageId1")
      val archive1 = Archive.newBuilder().setHash(packageId1).build()
      val packageId2 = Ref.PackageId.assertFromString("packageId2")
      val archive2 = Archive.newBuilder().setHash(packageId2).build()

      ArchiveConversions.parsePackageIdsAndRawArchives(List(archive1, archive2)) shouldBe Right(
        Map(
          packageId1 -> RawArchive(archive1.toByteString),
          packageId2 -> RawArchive(archive2.toByteString),
        )
      )
    }

    "fail on a broken package ID" in {
      val archive = Archive.newBuilder().setHash("???").build()

      ArchiveConversions.parsePackageIdsAndRawArchives(List(archive)) shouldBe Left(
        ConversionError.ParseError("non expected character 0x3f in Daml-LF Package ID \"???\"")
      )
    }
  }
}
