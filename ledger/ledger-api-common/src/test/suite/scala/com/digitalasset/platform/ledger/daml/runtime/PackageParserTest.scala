// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.ledger.daml.runtime

import java.io.InputStream

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.platform.damllf.PackageParser
import org.scalatest.{DoNotDiscover, Matchers, WordSpec}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
@DoNotDiscover // TODO LedgerPing.dalf is now invalid
class PackageParserTest extends WordSpec with Matchers {
  private def getDarStream: InputStream = getClass.getClassLoader.getResourceAsStream("Test.dar")

  "PackageParserTest" should {
    "successfully parse DAR file" in {
      val Some(pkgId) =
        Ref.PackageId
          .fromString("497b17f4f5148e0a7102a5a402c3e35e67aa67a663520089922c677819ee4875")
          .right
          .toOption
      val result = PackageParser.parseDarOrDalf(() => getDarStream)
      result should matchPattern {
        case Right((`pkgId`, Ast.Package(_))) =>
      }
    }

    "successfully get package id" in {
      PackageParser.getPackageIdFromDar(getDarStream) shouldBe Right(
        "497b17f4f5148e0a7102a5a402c3e35e67aa67a663520089922c677819ee4875")
    }
  }
}
