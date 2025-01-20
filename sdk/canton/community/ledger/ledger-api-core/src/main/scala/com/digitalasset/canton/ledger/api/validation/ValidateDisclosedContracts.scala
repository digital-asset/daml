// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.implicits.toBifunctorOps
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.commands.{
  Commands as ProtoCommands,
  DisclosedContract as ProtoDisclosedContract,
}
import com.daml.lf.data.ImmArray
import com.daml.lf.transaction.{FatContractInstance, TransactionCoder}
import com.daml.lf.value.Value as Lf
import com.digitalasset.canton.ledger.api.domain.{DisclosedContract, UpgradableDisclosedContract}
import com.digitalasset.canton.ledger.api.validation.FieldValidator.{
  requireContractId,
  requirePresence,
  validateIdentifier,
}
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.platform.apiserver.execution.SerializableContractAuthenticators.AuthenticateSerializableContract
import io.grpc.StatusRuntimeException

import scala.collection.mutable

class ValidateDisclosedContracts(
    explicitDisclosureFeatureEnabled: Boolean,
    authenticateSerializableContract: AuthenticateSerializableContract,
) {
  def apply(commands: ProtoCommands)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract]] =
    for {
      _ <- Either.cond(
        explicitDisclosureFeatureEnabled || commands.disclosedContracts.isEmpty,
        (),
        RequestValidationErrors.InvalidField
          .Reject(
            "disclosed_contracts",
            "feature disabled: disclosed_contracts should not be set",
          )
          .asGrpcError,
      )
      validatedDisclosedContracts <- validateDisclosedContracts(commands.disclosedContracts)
    } yield validatedDisclosedContracts

  private def validateDisclosedContracts(
      disclosedContracts: Seq[ProtoDisclosedContract]
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract]] = {
    type ZeroType =
      Either[
        StatusRuntimeException,
        mutable.Builder[DisclosedContract, ImmArray[DisclosedContract]],
      ]

    disclosedContracts
      .foldLeft[ZeroType](Right(ImmArray.newBuilder))((contracts, contract) =>
        for {
          validatedContracts <- contracts
          validatedContract <- validateDisclosedContract(contract)
        } yield validatedContracts.addOne(validatedContract)
      )
      .map(_.result())
  }

  private def validateDisclosedContract(
      disclosedContract: ProtoDisclosedContract
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, DisclosedContract] = {

    def build(
        fatContractInstance: FatContractInstance,
        validatedContractId: Lf.ContractId,
    ): UpgradableDisclosedContract = {
      import fatContractInstance.*
      UpgradableDisclosedContract(
        version = version,
        contractId = validatedContractId,
        packageName = packageName,
        templateId = templateId,
        argument = createArg,
        createdAt = createdAt,
        keyHash = contractKeyWithMaintainers.map(_.globalKey.hash),
        driverMetadata = cantonData,
        keyMaintainers = contractKeyWithMaintainers.map(_.maintainers),
        signatories = signatories,
        stakeholders = stakeholders,
        keyValue = contractKeyWithMaintainers.map(_.value),
      )

    }

    if (disclosedContract.createdEventBlob.isEmpty)
      Left(ValidationErrors.missingField("DisclosedContract.createdEventBlob"))
    else
      for {
        rawTemplateId <- requirePresence(
          disclosedContract.templateId,
          "DisclosedContract.template_id",
        )
        validatedTemplateId <- validateIdentifier(rawTemplateId)
        validatedContractId <- requireContractId(
          disclosedContract.contractId,
          "DisclosedContract.contract_id",
        )
        fatContractInstance <- TransactionCoder
          .decodeFatContractInstance(disclosedContract.createdEventBlob)
          .left
          .map(decodeError =>
            invalidArgument(s"Unable to decode disclosed contract event payload: $decodeError")
          )
        _ <- Either.cond(
          validatedContractId == fatContractInstance.contractId,
          (),
          invalidArgument(
            s"Mismatch between DisclosedContract.contract_id (${disclosedContract.contractId}) and contract_id from decoded DisclosedContract.created_event_blob (${fatContractInstance.contractId.coid})"
          ),
        )
        _ <- Either.cond(
          validatedTemplateId == fatContractInstance.templateId,
          (),
          invalidArgument(
            s"Mismatch between DisclosedContract.template_id ($validatedTemplateId) and template_id from decoded DisclosedContract.created_event_blob (${fatContractInstance.templateId})"
          ),
        )
        contract = build(fatContractInstance, validatedContractId)
        _ <- contract.validate(authenticateSerializableContract).leftMap { error =>
          invalidArgument(
            s"Contract authentication failed for attached disclosed contract with id (${disclosedContract.contractId}): $error"
          )
        }
      } yield contract
  }
}

object ValidateDisclosedContracts {
  val DisclosedContractsDisabled = new ValidateDisclosedContracts(false, _ => Right(()))
}
