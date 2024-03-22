// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.crypto

sealed trait CryptoKeyFormat extends Product with Serializable {
  def name: String
  override def toString: String = name
}

object CryptoKeyFormat {
  case object Tink extends CryptoKeyFormat {
    override val name: String = "Tink"
  }

  case object Der extends CryptoKeyFormat {
    override val name: String = "DER"
  }

  case object Raw extends CryptoKeyFormat {
    override val name: String = "Raw"
  }

  case object Symbolic extends CryptoKeyFormat {
    override val name: String = "Symbolic"
  }
}
