// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.v1.commands.{
  Commands => ProtoCommands,
  DisclosedContract => ProtoDisclosedContract,
}
import com.daml.ledger.api.validation.ValueValidator.{
  validateOptionalIdentifier,
  validateRecordFields,
}
import com.daml.lf.command.{ClientProvidedContractMetadata, DisclosedContract}
import com.daml.lf.data.ImmArray
import com.daml.lf.value.Value.ValueRecord
import com.daml.lf.value.ValueOuterClass.VersionedValue
import com.daml.lf.value.{Value, ValueCoder}
import com.daml.platform.server.api.validation.FieldValidations._
import com.google.protobuf.any.Any.toJavaProto
import io.grpc.StatusRuntimeException

import scala.collection.mutable
import scala.util.Try

class ValidateDisclosedContracts(explicitDisclosureFeatureEnabled: Boolean) {
  def apply(commands: ProtoCommands)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract[ClientProvidedContractMetadata]]] =
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
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract[ClientProvidedContractMetadata]]] = {
    type ZeroType =
      Either[
        StatusRuntimeException,
        mutable.Builder[DisclosedContract[ClientProvidedContractMetadata], ImmArray[
          DisclosedContract[ClientProvidedContractMetadata]
        ]],
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

  private def validateDisclosedContractArguments(arguments: ProtoDisclosedContract.Arguments)(
      implicit contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Value] =
    arguments match {
      case ProtoDisclosedContract.Arguments.CreateArguments(value) =>
        for {
          recordId <- validateOptionalIdentifier(value.recordId)
          validatedRecordField <- validateRecordFields(value.fields)
        } yield ValueRecord(recordId, validatedRecordField)
      case ProtoDisclosedContract.Arguments.CreateArgumentsBlob(value) =>
        for {
          protoAny <- validateProtoAny(value)
          versionedValue <- validateVersionedValue(protoAny)
        } yield versionedValue.unversioned
      case ProtoDisclosedContract.Arguments.Empty =>
        Left(ValidationErrors.missingField("DisclosedContract.arguments"))
    }

  private def validateProtoAny(value: com.google.protobuf.any.Any)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, VersionedValue] =
    Try(toJavaProto(value).unpack(classOf[VersionedValue])).toEither.left.map(err =>
      ValidationErrors.invalidField("blob", err.getMessage)
    )

  private def validateVersionedValue(
      versionedValue: VersionedValue
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Value.VersionedValue] = {
    ValueCoder
      .decodeVersionedValue(ValueCoder.CidDecoder, versionedValue)
      .left
      .map(err => ValidationErrors.invalidField("blob", err.errorMessage))
  }

  private def validateDisclosedContract(
      disclosedContract: ProtoDisclosedContract
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, DisclosedContract[ClientProvidedContractMetadata]] =
    for {
      templateId <- requirePresence(disclosedContract.templateId, "DisclosedContract.template_id")
      validatedTemplateId <- validateIdentifier(templateId)
      contractId <- requireContractId(
        disclosedContract.contractId,
        "DisclosedContract.contract_id",
      )
      argument <- validateDisclosedContractArguments(disclosedContract.arguments)
      metadata <- requirePresence(disclosedContract.metadata, "DisclosedContract.metadata")
      createdAt <- requirePresence(
        metadata.createdAt,
        "DisclosedContract.metadata.created_at",
      )
      validatedCreatedAt <- validateTimestamp(
        createdAt,
        "DisclosedContract.metadata.created_at",
      )
      keyHash <- validateHash(
        metadata.contractKeyHash,
        "DisclosedContract.metadata.contract_key_hash",
      )
    } yield DisclosedContract(
      contractId = contractId,
      templateId = validatedTemplateId,
      argument = argument,
      metadata = ClientProvidedContractMetadata(
        createdAt = validatedCreatedAt,
        keyHash = keyHash,
        driverMetadata = ImmArray.from(metadata.driverMetadata.toByteArray),
      ),
    )
}
