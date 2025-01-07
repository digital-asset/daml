// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.Hash
import com.digitalasset.daml.lf.data.Bytes

/** A hash-based identifier for contracts.
  * Must be paired with a discriminator to obtain a complete contract ID.
  */
final case class Unicum(unwrap: Hash) extends AnyVal {
  def toContractIdSuffix(contractIdVersion: CantonContractIdVersion): Bytes =
    contractIdVersion.versionPrefixBytes ++
      Bytes.fromByteString(unwrap.getCryptographicEvidence)
}
