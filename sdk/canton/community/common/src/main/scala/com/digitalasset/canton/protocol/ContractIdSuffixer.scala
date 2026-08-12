// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.{HashOps, HmacOps}
import com.digitalasset.canton.protocol.ContractIdSuffixer.RelativeSuffixResult
import com.digitalasset.canton.util.LfTransactionUtil
import com.digitalasset.daml.lf.crypto.Hash.HashingMethod
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.CreationTime

/** Turns local contract IDs into relative contract IDs */
class ContractIdSuffixer(hashOps: HashOps & HmacOps, contractIdVersion: CantonContractIdVersion) {

  private val unicumGenerator: UnicumGenerator = new UnicumGenerator(hashOps)

  val contractHashingMethod: HashingMethod = contractIdVersion.contractHashingMethod

  def relativeSuffixForLocalContract(
      contractSalt: ContractSalt,
      ledgerTime: CreationTime,
      createWithSuffixedArg: LfNodeCreate,
      contractHash: LfHash,
  ): Either[String, RelativeSuffixResult] = {
    val contractMetadata = LfTransactionUtil.metadataFromCreate(createWithSuffixedArg)

    for {
      localId <- LocalContractId.fromContractId(createWithSuffixedArg.coid)
      suffixAndAuth <- suffixAndAuthenticationData(
        contractSalt,
        ledgerTime,
        contractMetadata,
        contractHash,
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
      contractHash: LfHash,
  ): Either[String, (RelativeContractIdSuffix, ContractAuthenticationData)] =
    contractIdVersion match {
      case v1: CantonContractIdV1Version =>
        for {
          createdAt <- ledgerTime match {
            case created @ CreationTime.CreatedAt(_) => Right(created)
            case _ => Left(s"Invalid creation time for created contract: $ledgerTime")
          }
        } yield {
          val relativeSuffix = unicumGenerator.generateSuffixV1(
            contractSalt,
            createdAt,
            contractMetadata,
            v1,
            contractHash,
          )
          val authenticationData = ContractAuthenticationDataV1(contractSalt.unwrap)(v1)
          (relativeSuffix, authenticationData)
        }
      case v2: CantonContractIdV2Version =>
        for {
          _ <- Either.cond(
            ledgerTime == CreationTime.Now,
            (),
            s"Invalid creation time $ledgerTime for relative contract ID suffix of version $contractIdVersion",
          )
        } yield {
          val relativeSuffix = unicumGenerator.generateRelativeSuffixV2(
            contractSalt,
            contractMetadata,
            v2,
            contractHash,
          )
          val authenticationData = ContractAuthenticationDataV2(
            Bytes.fromByteString(contractSalt.unwrap.forHashing),
            None,
            Seq.empty,
          )(v2)
          (relativeSuffix, authenticationData)
        }
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
