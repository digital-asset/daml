// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.implicits.toBifunctorOps
import com.daml.ledger.api.v2.commands.{
  Commands as ProtoCommands,
  DisclosedContract as ProtoDisclosedContract,
}
import com.digitalasset.base.error.ContextualizedErrorLogger
import com.digitalasset.canton.ledger.api.DisclosedContract
import com.digitalasset.canton.ledger.api.validation.FieldValidator.{
  requireContractId,
  requireSynchronizerId,
}
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.platform.apiserver.execution.ContractAuthenticators.AuthenticateFatContractInstance
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.TransactionCoder
import com.google.common.annotations.VisibleForTesting
import io.grpc.StatusRuntimeException

import scala.collection.mutable

class ValidateDisclosedContracts(authenticateFatContractInstance: AuthenticateFatContractInstance) {

  def apply(commands: ProtoCommands)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract]] =
    fromDisclosedContracts(commands.disclosedContracts)

  def fromDisclosedContracts(disclosedContracts: Seq[ProtoDisclosedContract])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract]] =
    for {
      validatedDisclosedContracts <- validateDisclosedContracts(disclosedContracts)
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
  ): Either[StatusRuntimeException, DisclosedContract] =
    if (disclosedContract.createdEventBlob.isEmpty)
      Left(ValidationErrors.missingField("DisclosedContract.createdEventBlob"))
    else
      for {
        rawTemplateId <- requirePresence(
          disclosedContract.templateId,
          "DisclosedContract.template_id",
        )
        validatedTemplateIdWithPackage <- validateIdentifierWithPackageNameO(rawTemplateId)
        (validatedTemplateId, validatedPackageNameO) = validatedTemplateIdWithPackage
        validatedContractId <- requireContractId(
          disclosedContract.contractId,
          "DisclosedContract.contract_id",
        )
        synchronizerIdO <- OptionUtil
          .emptyStringAsNone(disclosedContract.synchronizerId)
          .map(requireSynchronizerId(_, "DisclosedContract.synchronizer_id").map(Some(_)))
          .getOrElse(Right(None))
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
        _ <- Either.cond(
          validatedPackageNameO.forall(_ == fatContractInstance.packageName),
          (),
          invalidArgument(
            s"Mismatch between package_name of DisclosedContract.template_id ($validatedTemplateId) and package_name from decoded DisclosedContract.created_event_blob (${fatContractInstance.packageName})"
          ),
        )
        _ <- authenticateFatContractInstance(fatContractInstance).leftMap { error =>
          invalidArgument(
            s"Contract authentication failed for attached disclosed contract with id (${disclosedContract.contractId}): $error"
          )
        }
      } yield DisclosedContract(
        fatContractInstance = fatContractInstance,
        synchronizerIdO = synchronizerIdO,
      )
}

object ValidateDisclosedContracts {
  @VisibleForTesting
  val WithContractIdVerificationDisabled = new ValidateDisclosedContracts(_ => Right(()))
}
