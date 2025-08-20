// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.daml.lf.data.Bytes

/** Represents the suffix of a relative contract ID, i.e., between suffixing and absolutization */
sealed trait RelativeContractIdSuffix {
  def toBytes: Bytes
}

final case class ContractIdSuffixV1(
    contractIdVersion: CantonContractIdV1Version,
    unicum: Unicum,
) extends RelativeContractIdSuffix {
  override def toBytes: Bytes = unicum.toContractIdSuffix(contractIdVersion)
}

// TODO(#23971) Add RelativeContractIdSuffixV2
