// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms.mock

import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoProvider,
  KmsConfig,
  PrivateKeyStoreConfig,
}
import com.digitalasset.canton.crypto.kms.mock.v1.MockKmsDriverFactory.mockKmsDriverName
import com.digitalasset.canton.integration.plugins.{EncryptedPrivateStoreStatus, UseKmsDriver}
import com.digitalasset.canton.integration.tests.security.kms.KmsCryptoIntegrationTestBase
import com.digitalasset.canton.integration.tests.security.kms.mock.MockKmsDriverCryptoIntegrationTestBase.mockKmsDriverConfig
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentSetup,
  EnvironmentSetupPlugin,
}
import com.typesafe.config.ConfigValueFactory

import scala.jdk.CollectionConverters.*

trait MockKmsDriverCryptoIntegrationTestBase extends KmsCryptoIntegrationTestBase {
  self: CommunityIntegrationTest with EnvironmentSetup =>

  protected val kmsConfig: KmsConfig = mockKmsDriverConfig

  // TODO(#25069): Add persistence to mock KMS driver
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
      // TODO(#25069): Add persistence to mock KMS driver
      throw new IllegalArgumentException(
        s"Cannot run with auto-init = false because there is not persistence in the Mock KMS Driver"
      )
    registerPlugin(
      new UseKmsDriver(
        nodes = protectedNodes,
        enableEncryptedPrivateStore = EncryptedPrivateStoreStatus.Disable,
        driverName = mockKmsDriverName,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
      )
    )
    registerPlugin(sequencerPlugin)
    storagePlugin.foreach(registerPlugin)
  }

}

object MockKmsDriverCryptoIntegrationTestBase {
  lazy val mockKmsDriverConfig: KmsConfig.Driver =
    KmsConfig.Driver(
      mockKmsDriverName,
      ConfigValueFactory.fromMap(Map.empty[String, AnyRef].asJava),
    )
  val mockKmsDriverCryptoConfig: CryptoConfig = CryptoConfig(
    provider = CryptoProvider.Kms,
    kms = Some(mockKmsDriverConfig),
    privateKeyStore = PrivateKeyStoreConfig(None),
  )
}
