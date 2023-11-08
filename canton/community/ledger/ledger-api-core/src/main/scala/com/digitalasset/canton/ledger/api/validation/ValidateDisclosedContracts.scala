// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.commands.{
  Commands as ProtoCommands,
  DisclosedContract as ProtoDisclosedContract,
}
import com.daml.lf.data.{Bytes, ImmArray}
import com.daml.lf.transaction.TransactionCoder
import com.daml.lf.value.Value.ValueRecord
import com.daml.lf.value.ValueOuterClass.VersionedValue
import com.daml.lf.value.{Value, ValueCoder}
import com.digitalasset.canton.ledger.api.domain.{
  DisclosedContract,
  NonUpgradableDisclosedContract,
  UpgradableDisclosedContract,
}
import com.digitalasset.canton.ledger.api.validation.FieldValidator.*
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.ledger.api.validation.ValueValidator.{
  validateOptionalIdentifier,
  validateRecordFields,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.google.protobuf.any.Any.toJavaProto
import io.grpc.StatusRuntimeException

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

  private def validateDisclosedContract(
      disclosedContract: ProtoDisclosedContract
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, DisclosedContract] =
    // TODO(#15058): For backwards compatibility with existing clients that rely on explicit disclosure,
    //               we support the deprecated disclosedContract.arguments if the preferred createdEventBlob is not provided.
    //               However, using the deprecated format in command submission is not compatible with contract upgrading.
    if (disclosedContract.createdEventBlob.isEmpty)
      validateDeprecatedDisclosedContractFormat(disclosedContract)
    else
      validateUpgradableDisclosedContractFormat(disclosedContract)

  @annotation.nowarn(
    "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.commands\\.DisclosedContract.*"
  )
  private def validateUpgradableDisclosedContractFormat(disclosedContract: ProtoDisclosedContract)(
      implicit contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, UpgradableDisclosedContract] =
    if (disclosedContract.arguments.isDefined || disclosedContract.metadata.isDefined)
      Left(
        invalidArgument(
          "DisclosedContract.arguments or DisclosedContract.metadata cannot be set together with DisclosedContract.created_event_blob"
        )
      )
    else
      for {
        fatContractInstance <- TransactionCoder
          .decodeFatContractInstance(disclosedContract.createdEventBlob)
          .left
          .map(decodeError =>
            invalidArgument(s"Unable to decode disclosed contract event payload: $decodeError")
          )
        _ <- Either.cond(
          disclosedContract.contractId == fatContractInstance.contractId.coid,
          (),
          invalidArgument(
            s"Mismatch between DisclosedContract.contract_id (${disclosedContract.contractId}) and contract_id from decoded DisclosedContract.created_event_blob (${fatContractInstance.contractId.coid})"
          ),
        )
      } yield {
        import fatContractInstance.*
        UpgradableDisclosedContract(
          contractId = contractId,
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

  // Allow using deprecated Protobuf fields for backwards compatibility
  @annotation.nowarn(
    "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.commands\\.DisclosedContract.*"
  )
  private def validateDeprecatedDisclosedContractFormat(disclosedContract: ProtoDisclosedContract)(
      implicit contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, NonUpgradableDisclosedContract] =
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
    } yield NonUpgradableDisclosedContract(
      contractId = contractId,
      templateId = validatedTemplateId,
      argument = argument,
      createdAt = validatedCreatedAt,
      keyHash = keyHash,
      driverMetadata = Bytes.fromByteString(metadata.driverMetadata),
    )
}
