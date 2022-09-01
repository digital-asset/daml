// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.api.util.TimestampConversion
import com.daml.error.ContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.lf.command.{ContractMetadata, DisclosedContract}
import com.daml.lf.data.ImmArray
import com.daml.ledger.api.v1.commands.{
  Commands => ProtoCommands,
  DisclosedContract => ProtoDisclosedContract,
}
import com.daml.ledger.api.validation.ValidationErrors.invalidArgument
import com.daml.ledger.api.validation.ValueValidator.{
  validateOptionalIdentifier,
  validateRecordFields,
}
import com.daml.lf.value.Value.ValueRecord
import com.daml.platform.server.api.validation.FieldValidations.{
  requireContractId,
  requirePresence,
  validateHash,
  validateIdentifier,
}
import com.google.protobuf.timestamp.Timestamp
import io.grpc.StatusRuntimeException
import com.daml.lf.data.Time

import scala.collection.mutable
import scala.util.Try

class ValidateDisclosedContracts(explicitDisclosureFeatureEnabled: Boolean) {
  def apply(commands: ProtoCommands)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract]] =
    for {
      _ <- Either.cond(
        explicitDisclosureFeatureEnabled || commands.disclosedContracts.isEmpty,
        (),
        LedgerApiErrors.RequestValidation.InvalidField
          .Reject(
            "disclosed_contracts",
            "feature in development: disclosed_contracts should not be set",
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
  ): Either[StatusRuntimeException, DisclosedContract] =
    for {
      templateId <- requirePresence(disclosedContract.templateId, "template_id")
      validatedTemplateId <- validateIdentifier(templateId)
      contractId <- requireContractId(disclosedContract.contractId, "contract_id")
      createArguments <- requirePresence(disclosedContract.arguments, "arguments")
      recordId <- validateOptionalIdentifier(createArguments.recordId)
      validatedRecordField <- validateRecordFields(createArguments.fields)
      metadata <- requirePresence(disclosedContract.metadata, "metadata")
      createdAt <- requirePresence(metadata.createdAt, "created_at")
      validatedCreatedAt <- validateCreatedAt(createdAt)
      keyHash <- validateHash(metadata.contractKeyHash, "contract_key_hash")
    } yield DisclosedContract(
      contractId = contractId,
      templateId = validatedTemplateId,
      argument = ValueRecord(recordId, validatedRecordField),
      metadata = ContractMetadata(
        createdAt = validatedCreatedAt,
        keyHash = keyHash,
        driverMetadata = ImmArray.from(metadata.driverMetadata.toByteArray),
      ),
    )

  // TODO ED: Extract in utility library
  private def validateCreatedAt(createdAt: Timestamp)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Time.Timestamp] =
    Try(
      TimestampConversion
        .toLf(
          protoTimestamp = createdAt,
          mode = TimestampConversion.ConversionMode.Exact,
        )
    ).toEither.left
      .map(errMsg =>
        invalidArgument(
          s"Can not represent disclosed contract createdAt ($createdAt) as a Daml timestamp: ${errMsg.getMessage}"
        )
      )
}
