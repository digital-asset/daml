// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.implicits.{toFoldableOps, toTraverseOps}
import com.daml.ledger.api.v2.commands.{
  Commands as ProtoCommands,
  DisclosedContract as ProtoDisclosedContract,
}
import com.digitalasset.canton.ledger.api.DisclosedContract
import com.digitalasset.canton.ledger.api.validation.FieldValidator.{
  requireSynchronizerId,
  validateOptional,
}
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.{
  invalidArgument,
  invalidField,
}
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol.LfFatContractInst
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.{CreationTime, TransactionCoder}
import com.digitalasset.daml.lf.value.Value.ContractId
import io.grpc.StatusRuntimeException

trait ValidateDisclosedContracts {

  def validateCommands(commands: ProtoCommands)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract]]

  def validateDisclosedContracts(disclosedContracts: Seq[ProtoDisclosedContract])(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract]]
}

object ValidateDisclosedContracts extends ValidateDisclosedContracts {

  def validateCommands(commands: ProtoCommands)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract]] =
    validateDisclosedContracts(commands.disclosedContracts)

  def validateDisclosedContracts(disclosedContracts: Seq[ProtoDisclosedContract])(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract]] =
    for {
      validatedDisclosedContracts <- validateContracts(disclosedContracts)
      _ <- verifyNoDuplicates(validatedDisclosedContracts.map(_.fatContractInstance).toSeq)
    } yield validatedDisclosedContracts

  private def verifyNoDuplicates(
      disclosedContracts: Seq[LfFatContractInst]
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, Unit] =
    for {
      _ <- disclosedContracts
        .map(_.contractId)
        .groupBy(identity)
        .collectFirst {
          case (contractId, occurrences) if occurrences.sizeIs > 1 => contractId.coid
        }
        .map(id => invalidArgument(s"Disclosed contracts contain duplicate contract id ($id)"))
        .toLeft(())
      _ <- disclosedContracts
        .flatMap(_.contractKeyWithMaintainers)
        .groupBy(identity)
        .collectFirst {
          case (key, occurrences) if occurrences.sizeIs > 1 => key
        }
        .map(key => invalidArgument(s"Disclosed contracts contain duplicate contract key ($key)"))
        .toLeft(())
    } yield ()

  private def validateContracts(
      disclosedContracts: Seq[ProtoDisclosedContract]
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, ImmArray[DisclosedContract]] =
    disclosedContracts.toList
      .foldM(ImmArray.newBuilder[DisclosedContract])((acc, contract) =>
        validateDisclosedContract(contract).map(acc.addOne)
      )
      .map(_.result())

  private def validateDisclosedContract(
      disclosedContract: ProtoDisclosedContract
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[StatusRuntimeException, DisclosedContract] =
    if (disclosedContract.createdEventBlob.isEmpty)
      Left(ValidationErrors.missingField("DisclosedContract.createdEventBlob"))
    else
      for {
        validatedTemplateIdO <- validateOptionalIdentifier(disclosedContract.templateId)
        validatedContractIdO <- validateOptional(
          OptionUtil.emptyStringAsNone(disclosedContract.contractId)
        )(ContractId.fromString(_).left.map(invalidField("DisclosedContract.contract_id", _)))
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
        _ <- validatedContractIdO.traverse(validatedContractId =>
          Either.cond(
            validatedContractId == fatContractInstance.contractId,
            (),
            invalidArgument(
              s"Mismatch between DisclosedContract.contract_id (${validatedContractId.coid}) and contract_id from decoded DisclosedContract.created_event_blob (${fatContractInstance.contractId.coid})"
            ),
          )
        )
        _ <- validatedTemplateIdO.traverse(validatedTemplateId =>
          Either.cond(
            validatedTemplateId == fatContractInstance.templateId,
            (),
            invalidArgument(
              s"Mismatch between DisclosedContract.template_id ($validatedTemplateId) and template_id from decoded DisclosedContract.created_event_blob (${fatContractInstance.templateId})"
            ),
          )
        )
        lfFatContractInst <- fatContractInstance.traverseCreateAt {
          case time: CreationTime.CreatedAt => Right(time)
          case _ => Left(invalidArgument("Contract creation time cannot be 'Now'"))
        }
      } yield DisclosedContract(
        fatContractInstance = lfFatContractInst,
        synchronizerIdO = synchronizerIdO,
      )
}
