// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.release.kms.driver

import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.integration.tests.release.kms.KmsCliIntegrationTest

/** Cli integration test for AWS KMS Driver configurations.
  *
  * Before being able to run these tests locally, you need to execute `sbt bundle` and `sbt
  * aws-kms-driver/assembly`.
  */
class AwsKmsDriverCliIntegrationTest extends KmsCliIntegrationTest {

  private lazy val driverVersion = sys.env.getOrElse("RELEASE_SUFFIX", BuildInfo.version)
  private lazy val driverJar =
    s"community/aws-kms-driver/target/scala-2.13/aws-kms-driver_2.13-$driverVersion.jar"

  override lazy val kmsConfigs: Seq[String] =
    Seq("community/app/src/test/resources/aws-kms-driver.conf")

  // Run Canton with EXTRA_CLASSPATH set to the driver.jar
  override lazy val cantonProcessEnvVar: Seq[(String, String)] =
    Seq("AWS_PROFILE" -> "sts", "EXTRA_CLASSPATH" -> driverJar)

  override lazy val bootstrapScript: String =
    "community/app/src/test/resources/scripts/aws_kms_participant1.canton"

  override lazy val testName: String = s"aws-kms-$packageName"
}
