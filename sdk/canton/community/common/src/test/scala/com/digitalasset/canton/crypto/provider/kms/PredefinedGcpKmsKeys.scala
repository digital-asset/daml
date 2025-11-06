// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.crypto.{EncryptionKeySpec, SigningKeySpec}

trait PredefinedGcpKmsKeys extends PredefinedKmsKeys {

  override val predefinedSymmetricEncryptionKey: KmsKeyId =
    KmsKeyId(String300.tryCreate("canton-kms-test-key"))

  override val predefinedSigningKeys: Map[SigningKeySpec, (KmsKeyId, KmsKeyId)] = Map(
    SigningKeySpec.EcP256 -> (
      KmsKeyId(String300.tryCreate("canton-kms-test-signing-key")),
      KmsKeyId(String300.tryCreate("canton-kms-test-another-signing-key"))
    ),
    SigningKeySpec.EcP384 -> (
      KmsKeyId(String300.tryCreate("canton-kms-test-signing-key-P384")),
      KmsKeyId(String300.tryCreate("canton-kms-test-another-signing-key-P384"))
    ),
    SigningKeySpec.EcSecp256k1 -> (
      KmsKeyId(String300.tryCreate("canton-kms-test-signing-key-secp256k1")),
      KmsKeyId(String300.tryCreate("canton-kms-test-another-signing-key-secp256k1"))
    ),
    SigningKeySpec.EcCurve25519 -> (
      KmsKeyId(String300.tryCreate("canton-kms-test-signing-key-ed25519")),
      KmsKeyId(String300.tryCreate("canton-kms-test-another-signing-key-ed25519"))
    ),
  )

  override val predefinedAsymmetricEncryptionKeys: Map[EncryptionKeySpec, (KmsKeyId, KmsKeyId)] =
    Map(
      EncryptionKeySpec.Rsa2048 -> (
        KmsKeyId(String300.tryCreate("canton-kms-test-asymmetric-key")),
        KmsKeyId(String300.tryCreate("canton-kms-test-another-asymmetric-key"))
      )
    )

}
