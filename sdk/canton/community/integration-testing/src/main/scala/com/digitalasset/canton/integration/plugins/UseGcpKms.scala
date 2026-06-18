// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.{KmsConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.logging.NamedLoggerFactory

/** Integration test plugin for setting up GCP KMS clients for nodes and to optionally enabled
  * encrypted crypto private stores using the KMS. For more info please check [[UseAwsKms]].
  */
class UseGcpKms(
    protected val keyId: Option[KmsKeyId] = Some(UseGcpKms.DefaultCantonTestKeyId),
    protected val nodes: Set[String],
    protected val nodesWithSessionSigningKeysDisabled: Set[String] = Set.empty,
    protected val enableEncryptedPrivateStore: EncryptedPrivateStoreStatus =
      EncryptedPrivateStoreStatus.Enable,
    protected val kmsConfig: KmsConfig.Gcp = KmsConfig.Gcp.defaultTestConfig,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
) extends UseKms

object UseGcpKms {
  lazy val DefaultCantonTestKeyId: KmsKeyId = KmsKeyId(
    String300.tryCreate("canton-kms-test-key")
  )
  lazy val DefaultCantonRotationTestKeyId: KmsKeyId = KmsKeyId(
    String300.tryCreate("canton-kms-rotation-test-key")
  )
}
