// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.crypto

sealed trait SigningKeyScheme extends Product with Serializable {
  def name: String

  override def toString: String = name
}

/** Schemes for signature keys.
  *
  * Ed25519 is the best performing curve and should be the default.
  * EC-DSA is slower than Ed25519 but has better compatibility with other systems (such as CCF).
  */
object SigningKeyScheme {
  case object Ed25519 extends SigningKeyScheme {
    override val name: String = "Ed25519"
  }

  case object EcDsaP256 extends SigningKeyScheme {
    override def name: String = "ECDSA-P256"
  }

  case object EcDsaP384 extends SigningKeyScheme {
    override def name: String = "ECDSA-P384"
  }
}
