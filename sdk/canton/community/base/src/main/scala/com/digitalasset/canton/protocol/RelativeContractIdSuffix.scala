// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.daml.lf.data.Bytes
import com.google.protobuf.ByteString

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

final case class RelativeContractIdSuffixV2(
    contractIdVersion: CantonContractIdV2Version,
    suffix: Bytes,
) extends RelativeContractIdSuffix
    with PrettyPrinting {
  override def toBytes: Bytes = contractIdVersion.versionPrefixBytesRelative ++ suffix

  override protected def pretty: Pretty[RelativeContractIdSuffixV2] = prettyOfClass(
    unnamedParam(_.toBytes.toHexString)
  )
}

object RelativeContractIdSuffixV2 {
  implicit val orderingRelativeContractIdSuffixV2: Ordering[RelativeContractIdSuffixV2] =
    Ordering.by[RelativeContractIdSuffixV2, ByteString](_.toBytes.toByteString)(
      ByteStringUtil.orderingByteString
    )
}
