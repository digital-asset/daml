// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.crypto.{HashOps, HmacOps}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator.AuthenticationPurpose
import com.digitalasset.canton.protocol.*

trait SerializableContractAuthenticator {

  /** Authenticates the provided contract contents against its contract-id
    * for the given purpose.
    * @param purpose The purpose for which the authentication is required.
    * @param contract The contract to be authenticated.
    */
  private[protocol] def authenticate(
      purpose: AuthenticationPurpose,
      contract: SerializableContract,
  ): Either[String, Unit]

  def authenticateUpgradableContract(contract: SerializableContract): Either[String, Unit] =
    authenticate(AuthenticationPurpose.ContractUpgradeValidation, contract)

  def authenticateInputContract(contract: SerializableContract): Either[String, Unit] =
    authenticate(AuthenticationPurpose.BasicContractAuthentication, contract)
}

class SerializableContractAuthenticatorImpl(
    unicumGenerator: UnicumGenerator,
    allowForUnauthenticatedContractIds: Boolean,
) extends SerializableContractAuthenticator {

  private[protocol] def authenticate(
      authenticationPurpose: AuthenticationPurpose,
      serializableContract: SerializableContract,
  ): Either[String, Unit] = {
    import serializableContract.*

    val ContractId.V1(_discriminator, cantonContractSuffix) = contractId
    CantonContractIdVersion.fromContractSuffix(cantonContractSuffix) match {
      case Right(contractIdVersion) if contractIdVersion.isAuthenticated =>
        for {
          _ <- Either.cond(
            contractIdVersion >= authenticationPurpose.minimumContractIdVersion,
            (),
            s"Authentication for ${authenticationPurpose.purposeDescription} requires at least contract id version ${authenticationPurpose.minimumContractIdVersion} instead of the provided $contractIdVersion",
          )
          salt <- contractSalt.toRight(
            s"Contract salt missing in serializable contract with authenticating contract id ($contractId)"
          )
          recomputedUnicum <- unicumGenerator
            .computeUnicum(
              contractSalt = salt,
              ledgerCreateTime = ledgerCreateTime,
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
      case _ if allowForUnauthenticatedContractIds => Right(())
      case Right(scheme) =>
        Left(s"Contract id scheme does not support authentication: $scheme")
      case Left(errMsg) =>
        throw new IllegalArgumentException(
          s"Unsupported contract id scheme detected. Please contact support. Context: $errMsg"
        )
    }
  }
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

  sealed trait AuthenticationPurpose extends Product with Serializable {
    def minimumContractIdVersion: CantonContractIdVersion

    def purposeDescription: String
  }

  private[protocol] object AuthenticationPurpose {

    /** Basic contract-id authentication purpose which ensures that the contract-id
      * is authenticated minimally against the contract's create argument, its template-id, ledger create time
      * and its salt (see [[ContractSalt]]).
      * Authentication against this contract data disallows tampering with contracts received on non-stakeholder participants.
      */
    case object BasicContractAuthentication extends AuthenticationPurpose {
      override val minimumContractIdVersion: CantonContractIdVersion = AuthenticatedContractIdV1

      override def purposeDescription: String = "basic contract authentication"
    }

    /** Purpose showing that the contract needs to be authenticated for smart contract upgrade validation
      * on [[com.daml.lf.engine.ResultNeedUpgradeVerification]] questions asked by the Daml Engine during command re/interpretation.
      * Contract-id schemes starting with [[AuthenticatedContractIdV3]], in addition to [[AuthenticatedContractIdV1]], authenticate
      * the contract against its signatories, stakeholders, key and its maintainers, and against the package-name of the contract's template.
      * Authentication against this contract data is needed as a replacement of the Engine checks which can't be
      * performed when the contract's source package is not available on the participant.
      */
    case object ContractUpgradeValidation extends AuthenticationPurpose {
      override val minimumContractIdVersion: CantonContractIdVersion = AuthenticatedContractIdV3

      override def purposeDescription: String = "contract upgrade validation"
    }
  }
}
