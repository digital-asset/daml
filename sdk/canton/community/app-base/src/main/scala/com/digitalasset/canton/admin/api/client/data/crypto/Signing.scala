// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.crypto

import com.daml.nonempty.NonEmpty

final case class RequiredSigningSpecs(
    algorithms: NonEmpty[Set[SigningAlgorithmSpec]],
    keys: NonEmpty[Set[SigningKeySpec]],
)

/** Schemes for signature keys. */
sealed trait SigningKeySpec extends Product with Serializable {
  def name: String
  override def toString: String = name
}

object SigningKeySpec {
  case object EcCurve25519 extends SigningKeySpec {
    override val name: String = "EC-Curve25519"
  }

  case object EcP256 extends SigningKeySpec {
    override val name: String = "EC-P256"
  }

  case object EcP384 extends SigningKeySpec {
    override val name: String = "EC-P384"
  }
}

/** Algorithm schemes for signing. */
sealed trait SigningAlgorithmSpec extends Product with Serializable {
  def name: String
  override def toString: String = name
}

object SigningAlgorithmSpec {
  case object Ed25519 extends SigningAlgorithmSpec {
    override val name: String = "Ed25519"
  }

  case object EcDsaSha256 extends SigningAlgorithmSpec {
    override val name: String = "EC-DSA-SHA256"
  }

  case object EcDsaSha384 extends SigningAlgorithmSpec {
    override val name: String = "EC-DSA-SHA384"
  }
}
