// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.release.kms.aws

import com.digitalasset.canton.integration.tests.release.kms.KmsCliIntegrationTest

/** Cli integration test for AWS KMS configurations Before being able to run these tests locally,
  * you need to execute `sbt bundle`.
  */
class AwsCliIntegrationTest extends KmsCliIntegrationTest {
  override lazy val kmsConfigs: Seq[String] = Seq(
    "community/app/src/test/resources/aws-kms-provider-tagged.conf",
    "community/app/src/test/resources/participant1-manual-init.conf",
  )
  override lazy val cantonProcessEnvVar: Seq[(String, String)] = Seq("AWS_PROFILE" -> "sts")
  override lazy val bootstrapScript: String =
    "community/app/src/test/resources/scripts/aws_kms_participant1.canton"
  override lazy val testName: String = "aws"
}
