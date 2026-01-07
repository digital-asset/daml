// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.crypto.{EncryptionKeySpec, SigningKeySpec}

/** Stores the pre-generated keys for the different KMSs */
trait HasPredefinedKmsKeys {
  def predefinedSymmetricEncryptionKey: KmsKeyId
  def predefinedSigningKeys: Map[SigningKeySpec, (KmsKeyId, KmsKeyId)]
  def predefinedAsymmetricEncryptionKeys: Map[EncryptionKeySpec, (KmsKeyId, KmsKeyId)]
}
