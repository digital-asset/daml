// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.commands.{
  Commands as ProtoCommands,
  DisclosedContract as ProtoDisclosedContract,
}
import com.digitalasset.canton.ledger.api.domain.DisclosedContract
import com.digitalasset.canton.ledger.api.validation.FieldValidator.requireContractId
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.TransactionCoder
import io.grpc.StatusRuntimeException

import scala.collection.mutable

class ValidateDisclosedContracts {
  def apply(commands: ProtoCommands)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract]] =
    for {
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
  ): Either[StatusRuntimeException, DisclosedContract] =
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
      } yield {
        import fatContractInstance.*
        DisclosedContract(
          contractId = validatedContractId,
          templateId = templateId,
          packageName = packageName,
          packageVersion = packageVersion,
          argument = createArg,
          createdAt = createdAt,
          keyHash = contractKeyWithMaintainers.map(_.globalKey.hash),
          driverMetadata = cantonData,
          keyMaintainers = contractKeyWithMaintainers.map(_.maintainers),
          signatories = signatories,
          stakeholders = stakeholders,
          keyValue = contractKeyWithMaintainers.map(_.value),
          transactionVersion = version,
        )
      }
}
