// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.gcp

import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoProvider,
  KmsConfig,
  PrivateKeyStoreConfig,
}
import com.digitalasset.canton.integration.plugins.{
  EncryptedPrivateStoreStatus,
  UseConfigTransforms,
  UseGcpKms,
}
import com.digitalasset.canton.integration.tests.security.kms.KmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentSetup,
  EnvironmentSetupPlugin,
}

trait GcpKmsCryptoIntegrationTestBase extends KmsCryptoIntegrationTestBase {
  self: CommunityIntegrationTest with EnvironmentSetup =>

  protected val kmsConfig: KmsConfig = KmsConfig.Gcp.defaultTestConfig

  protected val topologyPreDefinedKeys: TopologyKmsKeys = TopologyKmsKeys(
    namespaceKeyId = Some(s"canton-kms-test-namespace-key"),
    sequencerAuthKeyId = Some(s"canton-kms-test-authentication-key"),
    signingKeyId = Some(s"canton-kms-test-signing-key"),
    encryptionKeyId = Some(s"canton-kms-test-asymmetric-key"),
  )

  protected def setupPlugins(
      withAutoInit: Boolean,
      storagePlugin: Option[EnvironmentSetupPlugin],
      sequencerPlugin: EnvironmentSetupPlugin,
  ): Unit = {
    if (!withAutoInit)
      registerPlugin(
        new UseConfigTransforms(
          Seq(ConfigTransforms.disableAutoInit(protectedNodes)),
          loggerFactory,
        )
      )
    registerPlugin(
      new UseGcpKms(
        nodes = protectedNodes,
        nodesWithSessionSigningKeysDisabled = nodesWithSessionSigningKeysDisabled,
        enableEncryptedPrivateStore = EncryptedPrivateStoreStatus.Disable,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
      )
    )
    registerPlugin(sequencerPlugin)
    storagePlugin.foreach(registerPlugin)
  }

}

object GcpKmsCryptoIntegrationTestBase {
  val defaultGcpKmsCryptoConfig: CryptoConfig = CryptoConfig(
    provider = CryptoProvider.Kms,
    kms = Some(KmsConfig.Gcp.defaultTestConfig),
    privateKeyStore = PrivateKeyStoreConfig(None),
  )
}
