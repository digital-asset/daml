// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.implicits.toBifunctorOps
import com.digitalasset.canton.crypto.{HashOps, HmacOps}
import com.digitalasset.canton.protocol.*
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance, Versioned}
import com.digitalasset.daml.lf.value.Value.ContractId

/** Contract authenticator that verifies that the payload of the contract is consistent with the
  * contract id
  */
trait ContractAuthenticator {

  /** authenticates the contract based on the externally generated contract hash */
  def authenticate(contract: FatContractInstance, contractHash: LfHash): Either[String, Unit]

  /** Authenticates the contract payload and metadata (consisted of ledger create time, contract
    * instance and authentication data) against the contract id. This is the legacy function as it
    * does not support externally generated hashes.
    */
  // TODO(#27344) - Future versions of contract hash will require a minimal type calculated by the engine
  def legacyAuthenticate(contract: FatContractInstance): Either[String, Unit]

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

  override def legacyAuthenticate(contract: FatContractInstance): Either[String, Unit] =
    for {
      idVersion <- CantonContractIdVersion.extractCantonContractIdVersion(contract.contractId)
      idVersionV1 <- idVersion match {
        case v: CantonContractIdV1Version => Right(v)
        case other => Left(s"Unsupported contract authentication id version: $other")
      }
      contractHash = LegacyContractHash.tryFatContractHash(
        contract,
        idVersionV1.useUpgradeFriendlyHashing,
      )
      result <- authenticate(contract, contractHash)
    } yield result

  override def authenticate(
      contract: FatContractInstance,
      contractHash: LfHash,
  ): Either[String, Unit] = {
    val gk = contract.contractKeyWithMaintainers.map(Versioned(contract.version, _))
    for {
      metadata <- ContractMetadata.create(contract.signatories, contract.stakeholders, gk)
      contractIdVersion <- CantonContractIdVersion
        .extractCantonContractIdVersion(contract.contractId)
      authenticationData <- ContractAuthenticationData
        .fromLfBytes(contractIdVersion, contract.authenticationData)
        .leftMap(_.toString)
      _ <- authenticate(
        contract.contractId,
        contractIdVersion,
        authenticationData,
        contract.createdAt,
        metadata,
        contractHash,
      )
    } yield ()
  }

  private def authenticate(
      contractId: LfContractId,
      contractIdVersion: CantonContractIdVersion,
      authenticationData: ContractAuthenticationData,
      ledgerTime: CreationTime,
      metadata: ContractMetadata,
      contractHash: LfHash,
  ): Either[String, Unit] = {
    val ContractId.V1(_, cantonContractSuffix) = contractId match {
      case cid: LfContractId.V1 => cid
      case _ => sys.error("ContractId V2 are not supported")
    }
    val createdAt = ledgerTime match {
      case x: CreationTime.CreatedAt => x
      case CreationTime.Now =>
        sys.error("Cannot authenticate contract with creation time Now")
    }
    contractIdVersion match {
      case contractIdVersion: CantonContractIdV1Version =>
        for {
          salt <- authenticationData match {
            case ContractAuthenticationDataV1(salt) => Right(salt)
            case _: ContractAuthenticationDataV2 =>
              Left("Cannot authenticate contract with V1 contract ID via a V2 authentication data")
          }
          recomputedUnicum <- unicumGenerator.recomputeUnicum(
            salt,
            createdAt,
            metadata,
            contractHash,
          )
          recomputedSuffix = recomputedUnicum
            .toContractIdSuffix(contractIdVersion)
          _ <- Either.cond(
            recomputedSuffix == cantonContractSuffix,
            (),
            s"Mismatching contract id suffixes. Expected: $recomputedSuffix vs actual: $cantonContractSuffix",
          )
        } yield ()
      case _ =>
        // TODO(#23971) implement this for V2
        Left(s"Unsupported contract ID version $contractIdVersion")
    }
  }

}
