// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.mock

import com.digitalasset.canton.crypto.kms.mock.v1.MockKmsDriverFactory.mockKmsDriverName
import com.digitalasset.canton.integration.plugins.UseKmsDriver
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentSetup,
  EnvironmentSetupPlugin,
}

trait MockEncryptedCryptoPrivateStoreTestBase {
  self: CommunityIntegrationTest with EnvironmentSetup =>

  protected def setupPlugins(
      protectedNodes: Set[String],
      storagePlugin: Option[EnvironmentSetupPlugin],
      sequencerPlugin: EnvironmentSetupPlugin,
  ): Unit = {
    registerPlugin(
      new UseKmsDriver(
        nodes = protectedNodes,
        keyId = None,
        driverName = mockKmsDriverName,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
      )
    )
    registerPlugin(sequencerPlugin)
    storagePlugin.foreach(registerPlugin)
  }

}
