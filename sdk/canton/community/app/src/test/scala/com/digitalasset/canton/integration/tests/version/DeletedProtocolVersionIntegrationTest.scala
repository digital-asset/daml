// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.version

import com.digitalasset.canton.integration.CommunityConfigTransforms.updateAllDomainsProtocolVersion
import com.digitalasset.canton.integration.CommunityEnvironmentDefinition
import com.digitalasset.canton.integration.CommunityTests.{
  CommunityIntegrationTest,
  CommunityTestConsoleEnvironment,
  SharedCommunityEnvironment,
}
import com.digitalasset.canton.version.ProtocolVersion

class DeletedProtocolVersionIntegrationTest
    extends CommunityIntegrationTest
    with SharedCommunityEnvironment {

  private val deletedProtocolVersion = ProtocolVersion.v4

  override lazy val environmentDefinition: CommunityEnvironmentDefinition =
    CommunityEnvironmentDefinition.simpleTopology.withManualStart
      .addConfigTransform(updateAllDomainsProtocolVersion(deletedProtocolVersion))

  "Cannot start a domain with a deleted protocol version" in {
    implicit env: CommunityTestConsoleEnvironment =>
      deletedProtocolVersion.isDeleted shouldBe true

      clue(s"== domain does not start for unsupported protocol version $deletedProtocolVersion") {
        assertThrowsAndLogsCommandFailures(
          env.d("mydomain").start(),
          _.errorMessage should include(
            s"protocol version $deletedProtocolVersion is not supported."
          ),
        )
      }
  }

}
