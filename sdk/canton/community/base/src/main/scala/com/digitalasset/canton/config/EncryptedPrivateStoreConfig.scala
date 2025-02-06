// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.crypto.kms.KmsKeyId

/** Encapsulates possible configurations for different encrypted private stores */
sealed trait EncryptedPrivateStoreConfig extends UniformCantonConfigValidation {
  def reverted: Boolean
}

object EncryptedPrivateStoreConfig {

  implicit val encryptedPrivateStoreConfigCantonConfigValidator
      : CantonConfigValidator[EncryptedPrivateStoreConfig] =
    CantonConfigValidatorDerivation[EncryptedPrivateStoreConfig]

  /** Configuration for a KMS encrypted private store
    * @param wrapperKeyId defines key identifier for the wrapper key (e.g. ARN for AWS SDK). When it's None
    *                     Canton will either create a new key or use a previous existent key
    * @param reverted when true decrypts the stored private keys and stores them in clear, disabling the
    *                 encrypted crypto private key store in the process
    */
  final case class Kms(
      wrapperKeyId: Option[KmsKeyId] = None,
      reverted: Boolean = false,
  ) extends EncryptedPrivateStoreConfig
}
