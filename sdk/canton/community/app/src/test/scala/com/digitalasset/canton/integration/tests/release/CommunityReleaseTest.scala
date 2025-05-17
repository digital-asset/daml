// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.release

/** Marker trait for release tests running on the enterprise bundle.
  */
trait CommunityReleaseTest extends ReleaseArtifactIntegrationTestUtils {
  override protected val isEnterprise: Boolean = false
}
