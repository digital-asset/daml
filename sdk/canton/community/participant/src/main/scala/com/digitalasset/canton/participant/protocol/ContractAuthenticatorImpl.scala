// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.implicits.toBifunctorOps
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersionV10,
  CantonContractIdVersion,
  ContractMetadata,
  DriverContractMetadata,
  LfContractId,
  SerializableContract,
  SerializableRawContractInstance,
  UnicumGenerator,
}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Versioned}
import com.digitalasset.daml.lf.value.Value.{ContractId, ContractInstance}

trait ContractAuthenticator {

  /** Authenticates the contract payload and metadata (consisted of ledger create time, contract instance and
    * contract salt) against the contract id, iff the contract id has a [[com.digitalasset.canton.protocol.AuthenticatedContractIdVersionV10]] format.
    *
    * @param contract the serializable contract
    */
  def authenticateSerializable(contract: SerializableContract): Either[String, Unit]

  /** Authenticates the contract payload and metadata (consisted of ledger create time, contract instance and
    * contract salt) against the contract id, iff the contract id has a [[com.digitalasset.canton.protocol.AuthenticatedContractIdVersionV10]] format.
    *
    * @param contract the fat contract contract
    */
  def authenticateFat(contract: FatContractInstance): Either[String, Unit]

  /** This method is used in contract upgrade verification to ensure that the metadata computed by the upgraded
    * template matches the original metadata.
    *
    * @param contract the contract whose metadata has been re-calculated
    * @param metadata the recalculated metadata
    */
  def verifyMetadata(
      contract: SerializableContract,
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

  def authenticateFat(contract: FatContractInstance): Either[String, Unit] = {
    val gk = contract.contractKeyWithMaintainers.map(Versioned(contract.version, _))
    for {
      metadata <- ContractMetadata.create(contract.signatories, contract.stakeholders, gk)
      driverMetadata <- DriverContractMetadata
        .fromTrustedByteString(contract.cantonData.toByteString)
        .leftMap(_.toString)
      createTime <- CantonTimestamp.fromInstant(contract.createdAt.toInstant)
      contractInstance <- SerializableRawContractInstance
        .create(
          Versioned(
            contract.version,
            ContractInstance(
              contract.packageName,
              contract.packageVersion,
              contract.templateId,
              contract.createArg,
            ),
          )
        )
        .leftMap(_.toString)
      _ <- authenticate(
        contract.contractId,
        Some(driverMetadata.salt),
        LedgerCreateTime(createTime),
        metadata,
        contractInstance,
      )
    } yield ()
  }

  def authenticateSerializable(contract: SerializableContract): Either[String, Unit] =
    authenticate(
      contract.contractId,
      contract.contractSalt,
      contract.ledgerCreateTime,
      contract.metadata,
      contract.rawContractInstance,
    )

  def verifyMetadata(
      contract: SerializableContract,
      metadata: ContractMetadata,
  ): Either[String, Unit] =
    authenticate(
      contract.contractId,
      contract.contractSalt,
      contract.ledgerCreateTime,
      metadata,
      contract.rawContractInstance,
    )

  def authenticate(
      contractId: LfContractId,
      contractSalt: Option[Salt],
      ledgerTime: LedgerCreateTime,
      metadata: ContractMetadata,
      rawContractInstance: SerializableRawContractInstance,
  ): Either[String, Unit] = {
    val ContractId.V1(_, cantonContractSuffix) = contractId
    val optContractIdVersion = CantonContractIdVersion.fromContractSuffix(cantonContractSuffix)
    optContractIdVersion match {
      case Right(AuthenticatedContractIdVersionV10) =>
        for {
          contractIdVersion <- optContractIdVersion
          salt <- contractSalt.toRight(
            s"Contract salt missing in serializable contract with authenticating contract id ($contractId)"
          )
          recomputedUnicum <- unicumGenerator
            .recomputeUnicum(
              contractSalt = salt,
              ledgerCreateTime = ledgerTime,
              metadata = metadata,
              suffixedContractInstance = rawContractInstance,
            )
          recomputedSuffix = recomputedUnicum.toContractIdSuffix(contractIdVersion)
          _ <- Either.cond(
            recomputedSuffix == cantonContractSuffix,
            (),
            s"Mismatching contract id suffixes. Expected: $recomputedSuffix vs actual: $cantonContractSuffix",
          )
        } yield ()
      case Left(scheme) => Left(s"Unsupported contract authentication id scheme: $scheme")
    }
  }
}
