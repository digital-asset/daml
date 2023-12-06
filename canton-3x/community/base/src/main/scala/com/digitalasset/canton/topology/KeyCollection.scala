// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.crypto.{EncryptionPublicKey, KeyPurpose, PublicKey, SigningPublicKey}

final case class KeyCollection(
    signingKeys: Seq[SigningPublicKey],
    encryptionKeys: Seq[EncryptionPublicKey],
) {

  def forPurpose(purpose: KeyPurpose): Seq[PublicKey] = purpose match {
    case KeyPurpose.Signing => signingKeys
    case KeyPurpose.Encryption => encryptionKeys
  }

  def hasBothKeys(): Boolean = signingKeys.nonEmpty && encryptionKeys.nonEmpty

  def addTo(key: PublicKey): KeyCollection = (key: @unchecked) match {
    case sigKey: SigningPublicKey => copy(signingKeys = signingKeys :+ sigKey)
    case encKey: EncryptionPublicKey => copy(encryptionKeys = encryptionKeys :+ encKey)
  }

  def removeFrom(key: PublicKey): KeyCollection =
    (key: @unchecked) match {
      case _: SigningPublicKey => copy(signingKeys = signingKeys.filter(_.id != key.id))
      case _: EncryptionPublicKey => copy(encryptionKeys = encryptionKeys.filter(_.id != key.id))
    }
}

object KeyCollection {

  val empty: KeyCollection = KeyCollection(Seq(), Seq())

}
