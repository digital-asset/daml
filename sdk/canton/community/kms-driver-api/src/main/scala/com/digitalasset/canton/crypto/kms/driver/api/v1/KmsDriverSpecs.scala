// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.api.v1

sealed trait KeySpec extends Product with Serializable

sealed trait SigningKeySpec extends KeySpec
object SigningKeySpec {

  /** Elliptic Curve Signing Key based on NIST P-256 (aka Secp256r1)
    * as defined in https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcP256 extends SigningKeySpec

  /** Elliptic Curve Signing Key based on NIST P-384
    * as defined in https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcP384 extends SigningKeySpec
}

sealed trait SigningAlgoSpec
object SigningAlgoSpec {

  /** Elliptic Curve Digital Signature Algorithm with SHA256
    * as defined in https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcDsaSha256 extends SigningAlgoSpec

  /** Elliptic Curve Digital Signature Algorithm with SHA384
    * as defined in https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcDsaSha384 extends SigningAlgoSpec
}

sealed trait EncryptionKeySpec extends KeySpec
object EncryptionKeySpec {

  /** RSA 2048 bit */
  case object Rsa2048 extends EncryptionKeySpec
}

sealed trait EncryptionAlgoSpec
object EncryptionAlgoSpec {

  /** RSA Encryption Scheme with Optimal Asymmetric Encryption Padding (OAEP) using SHA256
    * as defined in https://datatracker.ietf.org/doc/html/rfc8017#section-7.1
    */
  case object RsaEsOaepSha256 extends EncryptionAlgoSpec
}
