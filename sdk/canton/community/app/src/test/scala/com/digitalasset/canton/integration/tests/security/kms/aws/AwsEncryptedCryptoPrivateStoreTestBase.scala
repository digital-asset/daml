// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.aws

import com.digitalasset.canton.integration.plugins.UseAwsKms
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentSetup,
  EnvironmentSetupPlugin,
}

trait AwsEncryptedCryptoPrivateStoreTestBase {
  self: CommunityIntegrationTest with EnvironmentSetup =>

  protected def setupPlugins(
      protectedNodes: Set[String],
      storagePlugin: Option[EnvironmentSetupPlugin],
      sequencerPlugin: EnvironmentSetupPlugin,
      withPreGenKey: Boolean = true,
      multiRegion: Boolean = false,
  ): Unit = {
    registerPlugin(
      if (withPreGenKey)
        new UseAwsKms(
          nodes = protectedNodes,
          multiRegion = multiRegion,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
        )
      else
        new UseAwsKms(
          nodes = protectedNodes,
          multiRegion = multiRegion,
          keyId = None,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
        )
    )
    registerPlugin(sequencerPlugin)
    storagePlugin.foreach(registerPlugin)
  }

}
