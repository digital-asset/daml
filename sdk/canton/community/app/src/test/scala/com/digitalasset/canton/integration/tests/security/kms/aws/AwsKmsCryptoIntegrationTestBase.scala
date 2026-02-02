// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.aws

import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoProvider,
  KmsConfig,
  PrivateKeyStoreConfig,
}
import com.digitalasset.canton.integration.plugins.{
  EncryptedPrivateStoreStatus,
  UseAwsKms,
  UseConfigTransforms,
}
import com.digitalasset.canton.integration.tests.security.kms.KmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentSetup,
  EnvironmentSetupPlugin,
}

trait AwsKmsCryptoIntegrationTestBase extends KmsCryptoIntegrationTestBase {
  self: CommunityIntegrationTest with EnvironmentSetup =>

  protected val kmsConfig: KmsConfig = KmsConfig.Aws.defaultTestConfig

  protected val topologyPreDefinedKeys: TopologyKmsKeys = TopologyKmsKeys(
    namespaceKeyId = Some(s"alias/canton-kms-test-namespace-key"),
    sequencerAuthKeyId = Some(s"alias/canton-kms-test-authentication-key"),
    signingKeyId = Some(s"alias/canton-kms-test-signing-key"),
    encryptionKeyId = Some(s"alias/canton-kms-test-asymmetric-key"),
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
      new UseAwsKms(
        nodes = protectedNodes,
        enableEncryptedPrivateStore = EncryptedPrivateStoreStatus.Disable,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
      )
    )
    registerPlugin(sequencerPlugin)
    storagePlugin.foreach(registerPlugin)
  }

}

object AwsKmsCryptoIntegrationTestBase {
  val defaultAwsKmsCryptoConfig: CryptoConfig = CryptoConfig(
    provider = CryptoProvider.Kms,
    kms = Some(KmsConfig.Aws.defaultTestConfig),
    privateKeyStore = PrivateKeyStoreConfig(None),
  )
}
