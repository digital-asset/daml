// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.config.{KmsConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.typesafe.config.{ConfigValue, ConfigValueFactory}

import scala.jdk.CollectionConverters.*

/** A plugin that allows the use of a generic KMS (Key Management Service) driver for integration
  * tests.
  *
  * @param keyId
  *   An optional wrapper key ID. If provided, it will attempt to use this pre-generated KMS-stored
  *   key to encrypt Canton’s private key store.
  * @param nodes
  *   Specifies which nodes (by their InstanceName) will be configured with a KMS. If nodes =
  *   UseKms.AllNodesSelected all environment nodes are selected.
  * @param enableEncryptedPrivateStore
  *   A flag to enable encrypted private stores. When enabled, private stores will be encrypted for
  *   additional security. By default, is enabled.
  * @param driverName
  *   The name of the external KMS driver.
  * @param driverConfig
  *   The driver specific raw config section. By default, is empty.
  */
class UseKmsDriver(
    protected val keyId: Option[KmsKeyId] = None,
    protected val nodes: Set[String],
    protected val enableEncryptedPrivateStore: EncryptedPrivateStoreStatus =
      EncryptedPrivateStoreStatus.Enable,
    val driverName: String,
    val driverConfig: ConfigValue = ConfigValueFactory.fromMap(Map.empty[String, AnyRef].asJava),
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
) extends UseCommunityKms {
  protected val kmsConfig: KmsConfig.Driver = KmsConfig.Driver(
    driverName,
    driverConfig,
  )
}
