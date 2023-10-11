// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.crypto

sealed abstract class HashAlgorithm(val name: String)

object HashAlgorithm {
  case object Sha256 extends HashAlgorithm("SHA-256")
}
