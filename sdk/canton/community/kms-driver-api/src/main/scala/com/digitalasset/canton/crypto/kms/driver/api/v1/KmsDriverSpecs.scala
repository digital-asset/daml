// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.api.v1

sealed trait KeySpec extends Product with Serializable

sealed trait SigningKeySpec extends KeySpec
object SigningKeySpec {

  /** Elliptic Curve Key from the Curve25519 curve as defined in http://ed25519.cr.yp.to/
    */
  case object EcCurve25519 extends SigningKeySpec

  /** Elliptic Curve Signing Key based on NIST P-256 (aka secp256r1) as defined in
    * https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcP256 extends SigningKeySpec

  /** Elliptic Curve Signing Key based on NIST P-384 as defined in
    * https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcP384 extends SigningKeySpec

  /** Elliptic Curve Key from SECG P256k1 curve (aka secp256k1) commonly used in bitcoin and
    * ethereum as defined in https://www.secg.org/sec2-v2.pdf
    */
  case object EcSecp256k1 extends SigningKeySpec
}

sealed trait SigningAlgoSpec
object SigningAlgoSpec {

  /** EdDSA signature scheme based on Curve25519 and SHA512 as defined in http://ed25519.cr.yp.to/
    */
  case object Ed25519 extends SigningAlgoSpec

  /** Elliptic Curve Digital Signature Algorithm with SHA256 as defined in
    * https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcDsaSha256 extends SigningAlgoSpec

  /** Elliptic Curve Digital Signature Algorithm with SHA384 as defined in
    * https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcDsaSha384 extends SigningAlgoSpec
}

sealed trait EncryptionKeySpec extends KeySpec
object EncryptionKeySpec {

  /** Elliptic Curve Key from the P-256 curve (aka Secp256r1) as defined in
    * https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcP256 extends EncryptionKeySpec

  /** RSA 2048 bit */
  case object Rsa2048 extends EncryptionKeySpec
}

sealed trait EncryptionAlgoSpec
object EncryptionAlgoSpec {

  /** ECIES with ECDH, AES128 CBC, and HKDF and authentication (MAC) with HMAC-SHA256. This requires
    * a P-256 key because we use SHA256, and we need to align the lengths of the curve and the hash
    * function.
    */
  case object EciesHkdfHmacSha256Aes128Cbc extends EncryptionAlgoSpec

  /** RSA Encryption Scheme with Optimal Asymmetric Encryption Padding (OAEP) using SHA256 as
    * defined in https://datatracker.ietf.org/doc/html/rfc8017#section-7.1
    */
  case object RsaEsOaepSha256 extends EncryptionAlgoSpec
}
