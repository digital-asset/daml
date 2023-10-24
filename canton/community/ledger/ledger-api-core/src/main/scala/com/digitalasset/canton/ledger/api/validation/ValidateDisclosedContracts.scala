// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.commands.{
  Commands as ProtoCommands,
  DisclosedContract as ProtoDisclosedContract,
}
import com.daml.lf.data.{Bytes, ImmArray}
import com.daml.lf.value.Value.ValueRecord
import com.daml.lf.value.ValueOuterClass.VersionedValue
import com.daml.lf.value.{Value, ValueCoder}
import com.digitalasset.canton.ledger.api.domain.DisclosedContract
import com.digitalasset.canton.ledger.api.validation.ValueValidator.{
  validateOptionalIdentifier,
  validateRecordFields,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.google.protobuf.any.Any.toJavaProto
import io.grpc.StatusRuntimeException

import scala.collection.mutable
import scala.util.Try

import FieldValidator.*

class ValidateDisclosedContracts(explicitDisclosureFeatureEnabled: Boolean) {
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

  // Allow using deprecated Protobuf fields for backwards compatibility
  @annotation.nowarn(
    "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.commands\\.DisclosedContract.*"
  )
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

  // Allow using deprecated Protobuf fields for backwards compatibility
  @annotation.nowarn(
    "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.commands\\.DisclosedContract.*"
  )
  private def validateDisclosedContract(
      disclosedContract: ProtoDisclosedContract
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, DisclosedContract] =
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
      createdAt = validatedCreatedAt,
      keyHash = keyHash,
      driverMetadata = Bytes.fromByteString(metadata.driverMetadata),
    )
}
