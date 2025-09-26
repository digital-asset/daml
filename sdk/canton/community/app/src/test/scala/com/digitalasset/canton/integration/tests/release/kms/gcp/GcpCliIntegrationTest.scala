// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.release.kms.gcp

import com.digitalasset.canton.integration.tests.release.kms.KmsCliIntegrationTest

/** Cli integration test for GCP KMS configurations Before being able to run these tests locally,
  * you need to execute `sbt bundle`.
  */
class GcpCliIntegrationTest extends KmsCliIntegrationTest {
  override lazy val kmsConfigs: Seq[String] = Seq(
    "enterprise/app/src/test/resources/gcp-kms-provider-tagged.conf",
    "enterprise/app/src/test/resources/participant1-manual-init.conf",
  )
  override lazy val cantonProcessEnvVar: Seq[(String, String)] = Seq.empty
  override lazy val bootstrapScript: String =
    "enterprise/app/src/test/resources/scripts/gcp_kms_participant1.canton"
  override lazy val testName: String = "gcp"
}
