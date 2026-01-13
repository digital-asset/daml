// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.fast.kms.gcp

import com.digitalasset.canton.integration.plugins.toxiproxy.{ParticipantToGcpKms, ProxyConfig}
import com.digitalasset.canton.integration.tests.security.kms.gcp.GcpKmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.toxiproxy.fast.kms.KmsFaultTest

import scala.concurrent.duration.*

class GcpKmsFaultTest extends KmsFaultTest with GcpKmsCryptoIntegrationTestBase {

  override lazy val proxyConf: ProxyConfig =
    ParticipantToGcpKms(s"p1-to-gcp-to-kms", "participant1")
  override lazy val timeoutName: String = "upstreamgcpkmspause"
  override lazy val timeoutProxy: Long = 1000
  override lazy val downFor: Duration = 60.seconds

}
