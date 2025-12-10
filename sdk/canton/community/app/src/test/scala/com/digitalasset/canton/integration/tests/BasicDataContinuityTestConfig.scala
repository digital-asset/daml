// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.integration.tests.manual.S3Synchronization
import org.scalatest.wordspec.AnyWordSpec

/** Simple test that loads config files in the /config folder for each release and verifies they
  * parse in this Canton version
  */
class BasicDataContinuityTestConfig extends AnyWordSpec with BaseTest with S3Synchronization {
//  "Data continuity config" should {
//    S3Dump.getConfigDumpDirectories(None).foreach { case (directory, releaseVersion) =>
//      s"parse default config for version $releaseVersion" in {
//        directory.localDownloadPath.children.foreach { configFile =>
//          CantonConfig
//            .parseAndLoad(
//              Seq(configFile).map(_.toJava),
//              None,
//            )
//            .isRight shouldBe true
//        }
//      }
//    }
//  }
}
