// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.crypto

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

sealed abstract class HashAlgorithm(val name: String) extends PrettyPrinting {
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object HashAlgorithm {
  case object Sha256 extends HashAlgorithm("SHA-256")
}
