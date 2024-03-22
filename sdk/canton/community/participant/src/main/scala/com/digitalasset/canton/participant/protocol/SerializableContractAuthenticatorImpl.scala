// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersion,
  AuthenticatedContractIdVersionV2,
  CantonContractIdVersion,
  ContractMetadata,
  LfContractId,
  NonAuthenticatedContractIdVersion,
  SerializableContract,
  SerializableRawContractInstance,
  UnicumGenerator,
}

trait SerializableContractAuthenticator {

  /** Authenticates the contract payload and metadata (consisted of ledger create time, contract instance and
    * contract salt) against the contract id, iff the contract id has a [[com.digitalasset.canton.protocol.AuthenticatedContractIdVersion]] format.
    *
    * @param contract the serializable contract
    */
  def authenticate(contract: SerializableContract): Either[String, Unit]

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

class SerializableContractAuthenticatorImpl(unicumGenerator: UnicumGenerator)
    extends SerializableContractAuthenticator {

  def authenticate(contract: SerializableContract): Either[String, Unit] = {
    authenticate(
      contract.contractId,
      contract.contractSalt,
      contract.ledgerCreateTime,
      contract.metadata,
      contract.rawContractInstance,
    )
  }

  def verifyMetadata(
      contract: SerializableContract,
      metadata: ContractMetadata,
  ): Either[String, Unit] = {
    authenticate(
      contract.contractId,
      contract.contractSalt,
      contract.ledgerCreateTime,
      metadata,
      contract.rawContractInstance,
    )
  }

  def authenticate(
      contractId: LfContractId,
      contractSalt: Option[Salt],
      ledgerTime: LedgerCreateTime,
      metadata: ContractMetadata,
      rawContractInstance: SerializableRawContractInstance,
  ): Either[String, Unit] = {
    val ContractId.V1(_discriminator, cantonContractSuffix) = contractId
    val optContractIdVersion = CantonContractIdVersion.fromContractSuffix(cantonContractSuffix)
    optContractIdVersion match {
      case Right(AuthenticatedContractIdVersionV2) | Right(AuthenticatedContractIdVersion) =>
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
              contractIdVersion = contractIdVersion,
            )
          recomputedSuffix = recomputedUnicum.toContractIdSuffix(contractIdVersion)
          _ <- Either.cond(
            recomputedSuffix == cantonContractSuffix,
            (),
            s"Mismatching contract id suffixes. expected: $recomputedSuffix vs actual: $cantonContractSuffix",
          )
        } yield ()
      // Future upgrades to the contract id scheme must also be supported
      // - hence we treat non-recognized contract id schemes as non-authenticated contract ids.
      case Left(_) | Right(NonAuthenticatedContractIdVersion) =>
        Right(())
    }
  }
}
