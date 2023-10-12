// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.data.Bytes
import com.digitalasset.canton.crypto.Hash

/** A hash-based identifier for contracts.
  * Must be paired with a discriminator to obtain a complete contract ID.
  */
final case class Unicum(unwrap: Hash) extends AnyVal {
  def toContractIdSuffix(contractIdVersion: CantonContractIdVersion): Bytes =
    contractIdVersion.versionPrefixBytes ++
      Bytes.fromByteString(unwrap.getCryptographicEvidence)
}
