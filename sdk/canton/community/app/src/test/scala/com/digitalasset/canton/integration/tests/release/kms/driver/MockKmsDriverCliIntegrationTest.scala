// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.release.kms.driver

import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.integration.tests.release.kms.KmsCliIntegrationTest

/** Cli integration test for the Mock KMS Driver configurations.
  *
  * Before being able to run these tests locally, you need to execute `sbt bundle` and `sbt
  * mock-kms-driver/package`.
  */
class MockKmsDriverCliIntegrationTest extends KmsCliIntegrationTest {

  private lazy val driverVersion = sys.env.getOrElse("RELEASE_SUFFIX", BuildInfo.version)
  private lazy val driverJar =
    s"community/mock-kms-driver/target/scala-2.13/mock-kms-driver_2.13-$driverVersion.jar"

  override lazy val kmsConfigs: Seq[String] =
    Seq("community/app/src/test/resources/mock-kms-driver.conf")

  // Run Canton with EXTRA_CLASSPATH set to the driver.jar
  override lazy val cantonProcessEnvVar: Seq[(String, String)] =
    Seq("EXTRA_CLASSPATH" -> driverJar)

  override protected def bootstrapScript: String =
    "community/app/src/test/resources/scripts/mock_kms_participant1.canton"

  override lazy val testName: String = s"mock-kms-$packageName"
}
