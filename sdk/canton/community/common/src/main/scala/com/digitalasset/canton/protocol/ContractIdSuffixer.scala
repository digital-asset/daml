// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.{HashOps, HmacOps}
import com.digitalasset.canton.protocol.ContractIdSuffixer.RelativeSuffixResult
import com.digitalasset.canton.util.LfTransactionUtil
import com.digitalasset.daml.lf.transaction.CreationTime
import com.digitalasset.daml.lf.value.Value.ThinContractInstance

/** Turns local contract IDs into relative contract IDs */
class ContractIdSuffixer(hashOps: HashOps & HmacOps, contractIdVersion: CantonContractIdVersion) {

  private val unicumGenerator: UnicumGenerator = new UnicumGenerator(hashOps)

  def relativeSuffixForLocalContract(
      contractSalt: ContractSalt,
      ledgerTime: CreationTime,
      createWithSuffixedArg: LfNodeCreate,
  ): Either[String, RelativeSuffixResult] = {
    val contractMetadata = LfTransactionUtil.metadataFromCreate(createWithSuffixedArg)

    for {
      localId <- LocalContractId.fromContractId(createWithSuffixedArg.coid)
      suffixAndAuth <- suffixAndAuthenticationData(
        contractSalt,
        ledgerTime,
        contractMetadata,
        createWithSuffixedArg.coinst,
      )
      (relativeSuffix, authenticationData) = suffixAndAuth
      suffixedContractId <- localId.withSuffix(relativeSuffix.toBytes)
    } yield {
      val suffixedCreateNode = createWithSuffixedArg.copy(coid = suffixedContractId)
      RelativeSuffixResult(suffixedCreateNode, localId, relativeSuffix, authenticationData)
    }
  }

  private def suffixAndAuthenticationData(
      contractSalt: ContractSalt,
      ledgerTime: CreationTime,
      contractMetadata: ContractMetadata,
      contractInst: ThinContractInstance,
  ): Either[String, (RelativeContractIdSuffix, ContractAuthenticationData)] =
    contractIdVersion match {
      case v1: CantonContractIdV1Version =>
        for {
          createdAt <- ledgerTime match {
            case created @ CreationTime.CreatedAt(_) => Right(created)
            case _ => Left(s"Invalid creation time for created contract: $ledgerTime")
          }
          unicum = unicumGenerator.generateUnicum(
            contractSalt,
            createdAt,
            contractMetadata,
            contractInst,
            v1,
          )
        } yield {
          val relativeSuffix = ContractIdSuffixV1(v1, unicum)
          val authenticationData = ContractAuthenticationDataV1(contractSalt.unwrap)(v1)
          (relativeSuffix, authenticationData)
        }
      case _: CantonContractIdV2Version =>
        // TODO(#23971) Implement this
        Left(s"Unsupported contract ID version: $contractIdVersion")
    }
}

object ContractIdSuffixer {
  final case class RelativeSuffixResult(
      suffixedCreateNode: LfNodeCreate,
      localContractId: LocalContractId,
      suffix: RelativeContractIdSuffix,
      authenticationData: ContractAuthenticationData,
  )
}
