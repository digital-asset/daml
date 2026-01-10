// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.integration.tests.ConfigContinuityReaderTest.Transforms
import com.digitalasset.canton.integration.tests.manual.S3Synchronization
import com.digitalasset.canton.version.ReleaseVersion
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec

/** Simple test that loads config files in the /config folder for each release and verifies they
  * parse in this Canton version
  */
class ConfigContinuityReaderTest extends AnyWordSpec with BaseTest with S3Synchronization {

  private lazy val allTransforms: Map[(Int, Int, Int), Transforms] = Map.empty

  /** Make the config parsable by applying some transformations. It basically makes some breaking
    * changes legitimate.
    */
  private def makeParsable(parsedConfig: Config, sourceVersion: ReleaseVersion): Config = {
    val transforms = allTransforms.getOrElse(
      (sourceVersion.major, sourceVersion.minor, sourceVersion.patch),
      Transforms.empty,
    )

    transforms.removePaths.foldLeft(parsedConfig) { case (config, removedPath) =>
      config.withoutPath(removedPath)
    }
  }

  "Data continuity config" should {
    S3Dump
      .getDumpBaseDirectoriesForVersion(None)
      // Filter-out dumps that don't contain the config
      .filter { case (_, releaseVersion) => releaseVersion >= ReleaseVersion.tryCreate("3.4.10-a") }
      .foreach { case (directory, releaseVersion) =>
        s"parse default config for version $releaseVersion" in {
          val initialConfigFile = directory.localDownloadPath / "default.conf"
          val transformedConfigFile = directory.localDownloadPath / "transformed_default.conf"

          val parsedConfig = ConfigFactory.parseFile(initialConfigFile.toJava)
          val updatedConfig = makeParsable(parsedConfig, releaseVersion)

          transformedConfigFile.write(
            updatedConfig.root().render(CantonConfig.defaultConfigRenderer)
          )

          CantonConfig
            .parseAndLoad(Seq(transformedConfigFile).map(_.toJava), None)
            .isRight shouldBe true
        }
      }
  }
}

private object ConfigContinuityReaderTest {
  final case class Transforms(removePaths: Seq[String])

  object Transforms {
    lazy val empty: Transforms = Transforms(Nil)
  }
}
