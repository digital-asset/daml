// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.crypto

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

sealed trait CryptoKeyFormat extends Product with Serializable with PrettyPrinting {
  def name: String
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object CryptoKeyFormat {
  case object DerX509Spki extends CryptoKeyFormat {
    override val name: String = "DER-encoded X.509 SubjectPublicKeyInfo"
  }

  case object DerPkcs8Pki extends CryptoKeyFormat {
    override val name: String = "DER-encoded PKCS #8 PrivateKeyInfo"
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
