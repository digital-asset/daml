// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.gcp

import com.digitalasset.canton.config.KmsConfig
import com.digitalasset.canton.integration.plugins.UseGcpKms
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentSetup,
  EnvironmentSetupPlugin,
}

trait GcpEncryptedCryptoPrivateStoreTestBase {
  self: CommunityIntegrationTest with EnvironmentSetup =>

  protected def setupPlugins(
      protectedNodes: Set[String],
      storagePlugin: Option[EnvironmentSetupPlugin],
      sequencerPlugin: EnvironmentSetupPlugin,
      withPreGenKey: Boolean = true,
      multiRegion: Boolean = false,
  ): Unit = {
    val kmsConfig =
      if (multiRegion) KmsConfig.Gcp.multiRegionTestConfig
      else KmsConfig.Gcp.defaultTestConfig

    registerPlugin(
      if (withPreGenKey)
        new UseGcpKms(
          nodes = protectedNodes,
          kmsConfig = kmsConfig,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
        )
      else
        new UseGcpKms(
          nodes = protectedNodes,
          keyId = None,
          kmsConfig = kmsConfig,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
        )
    )
    registerPlugin(sequencerPlugin)
    storagePlugin.foreach(registerPlugin)
  }

}
