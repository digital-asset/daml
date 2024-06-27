// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.protocol.SerializableContract.LedgerCreateTime
import com.digitalasset.canton.protocol.{
  AuthenticatedContractIdVersionV10,
  CantonContractIdVersion,
  ContractMetadata,
  LfContractId,
  SerializableContract,
  SerializableRawContractInstance,
  UnicumGenerator,
}
import com.digitalasset.daml.lf.value.Value.ContractId

trait SerializableContractAuthenticator {

  /** Authenticates the contract payload and metadata (consisted of ledger create time, contract instance and
    * contract salt) against the contract id, iff the contract id has a [[com.digitalasset.canton.protocol.AuthenticatedContractIdVersionV10]] format.
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

object SerializableContractAuthenticator {

  def apply(
      cryptoOps: HashOps & HmacOps,
      parameters: ParticipantNodeParameters,
  ): SerializableContractAuthenticator = new SerializableContractAuthenticatorImpl(
    // This unicum generator is used for all domains uniformly. This means that domains cannot specify
    // different unicum generator strategies (e.g., different hash functions).
    new UnicumGenerator(cryptoOps),
    parameters.allowForUnauthenticatedContractIds,
  )

}

class SerializableContractAuthenticatorImpl(
    unicumGenerator: UnicumGenerator,
    allowForUnauthenticatedContractIds: Boolean,
) extends SerializableContractAuthenticator {

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
              contractIdVersion = contractIdVersion,
            )
          recomputedSuffix = recomputedUnicum.toContractIdSuffix(contractIdVersion)
          _ <- Either.cond(
            recomputedSuffix == cantonContractSuffix,
            (),
            s"Mismatching contract id suffixes. expected: $recomputedSuffix vs actual: $cantonContractSuffix",
          )
        } yield ()
      case Left(scheme) =>
        if (allowForUnauthenticatedContractIds) Right(())
        else Left(s"Unsupported contract authentication id scheme: $scheme")
    }
  }
}
