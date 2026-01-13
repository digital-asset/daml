// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.daml.lf.data.Bytes

/** Represents a local contract ID without suffix */
// TODO(#23971) Move this to Daml-LF's contract ID definitions and incorporate in `suffixCid`
sealed trait LocalContractId extends Product with Serializable {
  def withSuffix(suffix: Bytes): Either[String, LfContractId]
}

object LocalContractId {
  final case class V1(
      discriminator: LfHash
  ) extends LocalContractId {
    override def withSuffix(suffix: Bytes): Either[String, LfContractId] =
      LfContractId.V1.build(discriminator, suffix)

    override def toString: String =
      s"${LfContractId.V1.prefix.toHexString}${discriminator.toHexString}"
  }

  final case class V2(
      local: Bytes
  ) extends LocalContractId {
    override def withSuffix(suffix: Bytes): Either[String, LfContractId] =
      LfContractId.V2.build(local, suffix)

    override def toString: String = s"${LfContractId.V2.prefix.toHexString}${local.toHexString}"
  }

  def fromContractId(contractId: LfContractId): Either[String, LocalContractId] =
    Either.cond(
      contractId.isLocal,
      extractFromContractId(contractId),
      s"Contract ID $contractId is not local",
    )

  def extractFromContractId(contractId: LfContractId): LocalContractId =
    contractId match {
      case LfContractId.V1(discriminator, _) => V1(discriminator)
      case LfContractId.V2(local, _) => V2(local)
    }
}
