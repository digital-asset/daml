// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.{KmsConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.logging.NamedLoggerFactory

/** Integration test plugin for setting up AWS KMS clients for nodes and to optionally enabled
  * encrypted crypto private stores using the KMS.
  *
  * @param keyId
  *   defines whether we use a pre-defined key (identified by its key identifier) or generate a new
  *   key if left as None.
  * @param multiRegion
  *   whether it will be a single or multi-region key.
  * @param nodes
  *   specifies (i.e. by its InstanceName) for which nodes we will configure a KMS (for now using
  *   AWS KMS). If nodes = UseKms.AllNodesSelected all environment nodes are selected.
  * @param enableEncryptedPrivateStore
  *   enable encrypted private stores
  * @return
  */
class UseAwsKms(
    protected val keyId: Option[KmsKeyId] = Some(UseAwsKms.DefaultCantonTestKeyId),
    protected val multiRegion: Boolean = false,
    protected val nodes: Set[String],
    protected val nodesWithSessionSigningKeysDisabled: Set[String] = Set.empty,
    protected val enableEncryptedPrivateStore: EncryptedPrivateStoreStatus =
      EncryptedPrivateStoreStatus.Enable,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
) extends UseKms {
  protected val kmsConfig: KmsConfig.Aws =
    KmsConfig.Aws.defaultTestConfig.copy(multiRegionKey = multiRegion)
}

object UseAwsKms {
  lazy val DefaultCantonTestKeyId: KmsKeyId = KmsKeyId(
    String300.tryCreate("alias/canton-kms-test-key")
  )
  lazy val DefaultCantonRotationTestKeyId: KmsKeyId = KmsKeyId(
    String300.tryCreate("alias/canton-kms-rotation-test-key")
  )
}
