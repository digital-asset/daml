// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.ledger.api.v2
import com.daml.ledger.api.v2.crypto as lapiCrypto
import io.scalaland.chimney.Transformer

/** Utility methods to convert between Canton crypto classes and their equivalent on the ledger API.
  */
object LedgerApiCryptoConversions {
  implicit val cantonToLAPISignatureFormatTransformer
      : Transformer[v30.SignatureFormat, lapiCrypto.SignatureFormat] = {
    case v30.SignatureFormat.SIGNATURE_FORMAT_UNSPECIFIED =>
      lapiCrypto.SignatureFormat.SIGNATURE_FORMAT_UNSPECIFIED
    case v30.SignatureFormat.SIGNATURE_FORMAT_DER => lapiCrypto.SignatureFormat.SIGNATURE_FORMAT_DER
    case v30.SignatureFormat.SIGNATURE_FORMAT_CONCAT =>
      lapiCrypto.SignatureFormat.SIGNATURE_FORMAT_CONCAT
    case v30.SignatureFormat.SIGNATURE_FORMAT_RAW => lapiCrypto.SignatureFormat.SIGNATURE_FORMAT_RAW
    case v30.SignatureFormat.SIGNATURE_FORMAT_SYMBOLIC =>
      lapiCrypto.SignatureFormat.SIGNATURE_FORMAT_SYMBOLIC
    case v30.SignatureFormat.Unrecognized(unrecognizedValue) =>
      lapiCrypto.SignatureFormat.Unrecognized(unrecognizedValue)
  }

  implicit val LAPIToCantonSignatureFormatTransformer
      : Transformer[lapiCrypto.SignatureFormat, v30.SignatureFormat] = {
    case lapiCrypto.SignatureFormat.SIGNATURE_FORMAT_UNSPECIFIED =>
      v30.SignatureFormat.SIGNATURE_FORMAT_UNSPECIFIED
    case lapiCrypto.SignatureFormat.SIGNATURE_FORMAT_DER => v30.SignatureFormat.SIGNATURE_FORMAT_DER
    case lapiCrypto.SignatureFormat.SIGNATURE_FORMAT_CONCAT =>
      v30.SignatureFormat.SIGNATURE_FORMAT_CONCAT
    case lapiCrypto.SignatureFormat.SIGNATURE_FORMAT_RAW => v30.SignatureFormat.SIGNATURE_FORMAT_RAW
    case lapiCrypto.SignatureFormat.SIGNATURE_FORMAT_SYMBOLIC =>
      v30.SignatureFormat.SIGNATURE_FORMAT_SYMBOLIC
    case lapiCrypto.SignatureFormat.Unrecognized(unrecognizedValue) =>
      v30.SignatureFormat.Unrecognized(unrecognizedValue)
  }

  implicit val LAPIToCantonSigningKeySpec
      : Transformer[v2.crypto.SigningKeySpec, v30.SigningKeySpec] = {
    case v2.crypto.SigningKeySpec.SIGNING_KEY_SPEC_UNSPECIFIED =>
      v30.SigningKeySpec.SIGNING_KEY_SPEC_UNSPECIFIED
    case v2.crypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519 =>
      v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519
    case v2.crypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_P256 =>
      v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_P256
    case v2.crypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_P384 =>
      v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_P384
    case v2.crypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_SECP256K1 =>
      v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_SECP256K1
    case v2.crypto.SigningKeySpec.Unrecognized(x) => v30.SigningKeySpec.Unrecognized(x)
  }

  implicit val CantonToLAPISigningKeySpec
      : Transformer[v30.SigningKeySpec, v2.crypto.SigningKeySpec] = {
    case v30.SigningKeySpec.SIGNING_KEY_SPEC_UNSPECIFIED =>
      v2.crypto.SigningKeySpec.SIGNING_KEY_SPEC_UNSPECIFIED
    case v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519 =>
      v2.crypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519
    case v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_P256 =>
      v2.crypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_P256
    case v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_P384 =>
      v2.crypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_P384
    case v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_SECP256K1 =>
      v2.crypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_SECP256K1
    case v30.SigningKeySpec.Unrecognized(x) => v2.crypto.SigningKeySpec.Unrecognized(x)
  }

  implicit val LAPIToCantonCryptoFormat
      : Transformer[v2.crypto.CryptoKeyFormat, v30.CryptoKeyFormat] = {
    case v2.crypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_UNSPECIFIED =>
      v30.CryptoKeyFormat.CRYPTO_KEY_FORMAT_UNSPECIFIED
    case v2.crypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER =>
      v30.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER
    case v2.crypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_RAW =>
      v30.CryptoKeyFormat.CRYPTO_KEY_FORMAT_RAW
    case v2.crypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO =>
      v30.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO
    case v2.crypto.CryptoKeyFormat.Unrecognized(x) => v30.CryptoKeyFormat.Unrecognized(x)
  }

  implicit val CantonToLAPICryptFormat
      : Transformer[v30.CryptoKeyFormat, v2.crypto.CryptoKeyFormat] = {
    case v30.CryptoKeyFormat.CRYPTO_KEY_FORMAT_UNSPECIFIED =>
      v2.crypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_UNSPECIFIED
    case v30.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER =>
      v2.crypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER
    case v30.CryptoKeyFormat.CRYPTO_KEY_FORMAT_RAW =>
      v2.crypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_RAW
    case v30.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO =>
      v2.crypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO
    // not supported on the API
    case v30.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_PKCS8_PRIVATE_KEY_INFO =>
      v2.crypto.CryptoKeyFormat.Unrecognized(-1)
    case v30.CryptoKeyFormat.CRYPTO_KEY_FORMAT_SYMBOLIC =>
      v2.crypto.CryptoKeyFormat.Unrecognized(-1)
    case v30.CryptoKeyFormat.Unrecognized(x) => v2.crypto.CryptoKeyFormat.Unrecognized(x)
  }

}
