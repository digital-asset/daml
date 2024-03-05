// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import com.digitalasset.canton.crypto.{
  Crypto,
  EncryptionTest,
  PasswordBasedEncryptionTest,
  PrivateKeySerializationTest,
  RandomTest,
  SigningTest,
}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class SymbolicCryptoTest
    extends AsyncWordSpec
    with SigningTest
    with EncryptionTest
    with PrivateKeySerializationTest
    with PasswordBasedEncryptionTest
    with RandomTest {

  "SymbolicCrypto" can {

    def symbolicCrypto(): Future[Crypto] =
      Future.successful(
        SymbolicCrypto.create(
          testedReleaseProtocolVersion,
          timeouts,
          loggerFactory,
        )
      )

    behave like signingProvider(SymbolicCryptoProvider.supportedSigningKeySchemes, symbolicCrypto())
    behave like encryptionProvider(
      SymbolicCryptoProvider.supportedEncryptionKeySchemes,
      SymbolicCryptoProvider.supportedSymmetricKeySchemes,
      symbolicCrypto(),
    )
    behave like privateKeySerializerProvider(
      SymbolicCryptoProvider.supportedSigningKeySchemes,
      SymbolicCryptoProvider.supportedEncryptionKeySchemes,
      symbolicCrypto(),
    )
    behave like randomnessProvider(symbolicCrypto().map(_.pureCrypto))

    behave like pbeProvider(
      SymbolicCryptoProvider.supportedPbkdfSchemes,
      SymbolicCryptoProvider.supportedSymmetricKeySchemes,
      symbolicCrypto().map(_.pureCrypto),
    )

    // Symbolic crypto does not satisfy golden tests for HKDF
    // Symbolic crypto does not support Java key conversion, thus not tested

    // Symbolic crypto does not support public key validation, thus not tested
  }

}
