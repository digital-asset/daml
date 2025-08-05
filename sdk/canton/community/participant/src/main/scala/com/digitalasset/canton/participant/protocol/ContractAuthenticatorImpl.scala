// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.implicits.toBifunctorOps
import com.digitalasset.canton.crypto.{HashOps, HmacOps}
import com.digitalasset.canton.protocol.*
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance, Versioned}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ContractId, ThinContractInstance}

trait ContractAuthenticator {

  /** Authenticates the contract payload and metadata (consisted of ledger create time, contract
    * instance and authentication data) against the contract id.
    *
    * @param contract
    *   the fat contract contract
    */
  def authenticate(contract: FatContractInstance): Either[String, Unit]

  /** This method is used in contract upgrade verification to ensure that the metadata computed by
    * the upgraded template matches the original metadata.
    *
    * @param contract
    *   the contract whose metadata has been re-calculated
    * @param metadata
    *   the recalculated metadata
    */
  def verifyMetadata(
      contract: GenContractInstance,
      metadata: ContractMetadata,
  ): Either[String, Unit]

}

object ContractAuthenticator {

  def apply(cryptoOps: HashOps & HmacOps): ContractAuthenticator =
    new ContractAuthenticatorImpl(
      // This unicum generator is used for all synchronizers uniformly. This means that synchronizers cannot specify
      // different unicum generator strategies (e.g., different hash functions).
      new UnicumGenerator(cryptoOps)
    )

}

class ContractAuthenticatorImpl(unicumGenerator: UnicumGenerator) extends ContractAuthenticator {

  private def toThin(inst: FatContractInstance): ThinContractInstance =
    Value.ThinContractInstance(
      inst.packageName,
      inst.templateId,
      inst.createArg,
    )

  override def authenticate(contract: FatContractInstance): Either[String, Unit] = {
    val gk = contract.contractKeyWithMaintainers.map(Versioned(contract.version, _))
    for {
      metadata <- ContractMetadata.create(contract.signatories, contract.stakeholders, gk)
      contractIdVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(
          contract.contractId
        )
        .leftMap(_.toString)
      authenticationData <- ContractAuthenticationData
        .fromLfBytes(contractIdVersion, contract.authenticationData)
        .leftMap(_.toString)
      _ <- authenticate(
        contract.contractId,
        authenticationData,
        contract.createdAt,
        metadata,
        toThin(contract),
      )
    } yield ()
  }

  override def verifyMetadata(
      contract: GenContractInstance,
      metadata: ContractMetadata,
  ): Either[String, Unit] = for {
    cad <- contract.contractAuthenticationData
    result <- authenticate(
      contract.contractId,
      cad,
      contract.inst.createdAt,
      metadata,
      toThin(contract.inst),
    )
  } yield result

  def authenticate(
      contractId: LfContractId,
      authenticationData: ContractAuthenticationData,
      ledgerTime: CreationTime,
      metadata: ContractMetadata,
      suffixedContractInstance: ThinContractInstance,
  ): Either[String, Unit] = {
    val ContractId.V1(_, cantonContractSuffix) = contractId match {
      case cid: LfContractId.V1 => cid
      case _ => sys.error("ContractId V2 are not supported")
    }
    val optContractIdVersion = CantonContractIdVersion.extractCantonContractIdVersion(contractId)
    val createdAt = ledgerTime match {
      case x: CreationTime.CreatedAt => x
      case CreationTime.Now => sys.error("Cannot authenticate contract with creation time Now")
    }
    optContractIdVersion match {
      case Right(contractIdVersion: CantonContractIdV1Version) =>
        for {
          salt <- authenticationData match {
            case ContractAuthenticationDataV1(salt) => Right(salt)
            case ContractAuthenticationDataV2() =>
              Left("Cannot authenticate contract with V1 contract ID via a V2 authentication data")
          }
          recomputedUnicum <- unicumGenerator
            .recomputeUnicum(
              contractSalt = salt,
              ledgerCreateTime = createdAt,
              metadata = metadata,
              suffixedContractInstance = suffixedContractInstance,
              cantonContractIdVersion = contractIdVersion,
            )
          recomputedSuffix = recomputedUnicum.toContractIdSuffix(contractIdVersion)
          _ <- Either.cond(
            recomputedSuffix == cantonContractSuffix,
            (),
            s"Mismatching contract id suffixes. Expected: $recomputedSuffix vs actual: $cantonContractSuffix",
          )
        } yield ()
      case Right(v2) =>
        // TODO(#23971) implement this
        Left(s"Unsupported contract ID version $v2")
      case Left(scheme) => Left(s"Unsupported contract authentication id scheme: $scheme")
    }
  }
}
