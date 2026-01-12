// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.fast.kms.aws

import com.digitalasset.canton.integration.plugins.toxiproxy.{ParticipantToAwsKms, ProxyConfig}
import com.digitalasset.canton.integration.tests.security.kms.aws.AwsKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.toxiproxy.fast.kms.KmsFaultTest

import scala.concurrent.duration.*

class AwsKmsFaultTest extends KmsFaultTest with AwsKmsCryptoIntegrationTestBase {

  override lazy val proxyConf: ProxyConfig =
    ParticipantToAwsKms(s"p1-to-aws-to-kms", "participant1")
  override lazy val timeoutName: String = "upstreamawskmspause"
  override lazy val timeoutProxy: Long = 1000
  override lazy val downFor: Duration = 60.seconds

}
