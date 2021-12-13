// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox

import error.ContextualizedErrorLogger
import error.definitions.LedgerApiErrors
import ledger.sandbox.ConflictCheckingWriteService.Submission
import ledger.sandbox.ConflictCheckingWriteService.Submission.Transaction
import lf.data.Time.Timestamp
import lf.transaction.GlobalKey
import platform.server.api.validation.ErrorFactories
import platform.store.appendonlydao.events.ContractId

import ledger.configuration.LedgerTimeModel
import com.google.protobuf.any.Any
import com.google.rpc.code.Code
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.Status

sealed trait SoxRejection extends Submission {
  def toStatus: Status

  val originalTx: Submission.Transaction
}

object SoxRejection {
  private val useSelfServiceErrorCodes = false
  // Temporary until consuming applications can deal with error codes properly
  private val errorFactories = ErrorFactories(useSelfServiceErrorCodes = useSelfServiceErrorCodes)

  final case class DuplicateKey(key: GlobalKey)(override val originalTx: Transaction)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends SoxRejection {
    override def toStatus: Status = {
      if (!useSelfServiceErrorCodes)
        Status.of(Code.ABORTED.value, "Invalid contract key", Seq.empty)
      else
        LedgerApiErrors.ConsistencyErrors.DuplicateContractKey
          .RejectWithContractKeyArg("DuplicateKey: contract key is not unique", key)
          .rpcStatus(None)
    }
  }

  final case class InconsistentContractKey(
      expectation: Option[ContractId],
      result: Option[ContractId],
  )(override val originalTx: Transaction)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends SoxRejection {
    override def toStatus: Status = if (!useSelfServiceErrorCodes)
      Status.of(Code.ABORTED.value, "Invalid contract key", Seq.empty)
    else
      LedgerApiErrors.ConsistencyErrors.InconsistentContractKey
        .Reject(
          s"Contract key lookup with different results. Expected: $expectation, result: $result"
        )
        .rpcStatus(None)
  }

  final case class GenericRejectionFailure(details: String)(override val originalTx: Transaction)(
      implicit contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends SoxRejection {
    override def toStatus: Status = if (!useSelfServiceErrorCodes)
      Status.of(Code.ABORTED.value, "Invalid contract key", Seq.empty)
    else
      // TODO wrong error
      LedgerApiErrors.InternalError.VersionService(details).rpcStatus(None)
  }

  final case class CausalMonotonicityViolation(
      contractLedgerEffectiveTime: Timestamp,
      transactionLedgerEffectiveTime: Timestamp,
  )(override val originalTx: Transaction)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends SoxRejection {
    override def toStatus: Status = if (!useSelfServiceErrorCodes)
      Status.of(Code.ABORTED.value, "ADD DETAILS FOR LET failure", Seq.empty)
    else
      LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
        .RejectSimple("ADD DETAILS FOR LET failure")
        .rpcStatus(None)
  }

  final case class UnknownContracts(ids: Set[ContractId])(override val originalTx: Transaction)(
      implicit contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends SoxRejection {
    override def toStatus: Status = if (!useSelfServiceErrorCodes)
      Status.of(Code.ABORTED.value, "Invalid contract key", Seq.empty)
    else
      LedgerApiErrors.ConsistencyErrors.ContractNotFound
        .MultipleContractsNotFound(ids.map(_.coid))
        .rpcStatus(None)
  }

  final case class UnallocatedParties(unallocatedParties: Set[String])(
      override val originalTx: Transaction
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger)
      extends SoxRejection {
    override def toStatus: Status =
      errorFactories.CommandRejections.partiesNotKnownToLedger(unallocatedParties)
  }

  final case class NoLedgerConfiguration(override val originalTx: Transaction)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends SoxRejection {
    override def toStatus: Status = errorFactories.missingLedgerConfig(
      Status.of(
        code = io.grpc.Status.Code.ABORTED.value(),
        message = "Ledger configuration not found",
        details = Seq(
          Any.pack(
            ErrorInfo.of(
              reason = "NO_LEDGER_CONFIGURATION",
              domain = "com.daml.sandbox",
              metadata = Map.empty,
            )
          )
        ),
      ),
      "Cannot validate ledger time",
    )
  }

  final case class InvalidLedgerTime(outOfRange: LedgerTimeModel.OutOfRange)(
      override val originalTx: Transaction
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends SoxRejection {
    override def toStatus: Status = errorFactories.CommandRejections.invalidLedgerTime(
      Status.of(
        code = io.grpc.Status.Code.ABORTED.value(),
        message = outOfRange.message,
        details = Seq(
          Any.pack(
            ErrorInfo.of(
              reason = "INVALID_LEDGER_TIME",
              domain = "com.daml.sandbox",
              metadata = Map(
                "ledgerTime" -> outOfRange.ledgerTime.toString,
                "lowerBound" -> outOfRange.lowerBound.toString,
                "upperBound" -> outOfRange.upperBound.toString,
              ),
            )
          )
        ),
      ),
      outOfRange.ledgerTime.toInstant,
      outOfRange.lowerBound.toInstant,
      outOfRange.upperBound.toInstant,
    )
  }
}
