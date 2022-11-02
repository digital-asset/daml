// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox.domain

import com.daml.ledger.participant.state.v2.{ChangeId, CompletionInfo, Update}
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import error.ContextualizedErrorLogger
import ledger.configuration.LedgerTimeModel
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.ContractId
import com.daml.ledger.errors.LedgerApiErrors
import com.google.rpc.status.Status

import java.time.Duration

private[sandbox] sealed trait Rejection extends Product with Serializable {
  def toStatus: Status
  def completionInfo: CompletionInfo
  def toCommandRejectedUpdate(recordTime: Timestamp): Update.CommandRejected =
    Update.CommandRejected(
      recordTime = recordTime,
      completionInfo = completionInfo,
      reasonTemplate = FinalReason(toStatus),
    )
}

private[sandbox] object Rejection {

  final case class DuplicateKey(key: GlobalKey)(
      val completionInfo: CompletionInfo
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      LedgerApiErrors.ConsistencyErrors.DuplicateContractKey
        .RejectWithContractKeyArg(
          cause = "DuplicateKey: contract key is not unique",
          key = key.toString,
        )
        .rpcStatus()
  }

  final case class InconsistentContractKey(
      expectation: Option[ContractId],
      result: Option[ContractId],
  )(val completionInfo: CompletionInfo)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      LedgerApiErrors.ConsistencyErrors.InconsistentContractKey
        .Reject(
          s"Contract key lookup with different results: expected [$expectation], actual [$result]"
        )
        .rpcStatus()
  }

  final case class LedgerBridgeInternalError(_err: Throwable, completionInfo: CompletionInfo)(
      implicit contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status = LedgerApiErrors.InternalError
      .UnexpectedOrUnknownException(_err)
      .rpcStatus()
  }

  final case class OffsetDeduplicationPeriodUnsupported(completionInfo: CompletionInfo)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status = LedgerApiErrors.UnsupportedOperation
      .Reject("command deduplication with periods specified using offsets")
      .rpcStatus()
  }

  final case class TransactionInternallyInconsistentKey(
      key: GlobalKey,
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      LedgerApiErrors.WriteServiceRejections.Internal.InternallyInconsistentKeys
        .Reject(
          cause = "The transaction references a contract key inconsistently",
          keyO = Some(key.toString),
        )
        .rpcStatus()
  }

  final case class TransactionInternallyDuplicateKeys(
      key: GlobalKey,
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      LedgerApiErrors.WriteServiceRejections.Internal.InternallyDuplicateKeys
        .Reject(
          cause = "The transaction attempts to create two contracts with the same contract key",
          keyO = Some(key.toString),
        )
        .rpcStatus()
  }

  final case class CausalMonotonicityViolation(
      contractLedgerEffectiveTime: Timestamp,
      transactionLedgerEffectiveTime: Timestamp,
  )(val completionInfo: CompletionInfo)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
        .RejectSimple(
          s"Ledger effective time for one of the contracts ($contractLedgerEffectiveTime) is greater than the ledger effective time of the transaction ($transactionLedgerEffectiveTime)"
        )
        .rpcStatus()
  }

  final case class UnknownContracts(ids: Set[ContractId])(
      val completionInfo: CompletionInfo
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status = {
      val missingContractIds = ids.map(_.coid)
      LedgerApiErrors.ConsistencyErrors.ContractNotFound
        .MultipleContractsNotFound(missingContractIds)
        .rpcStatus()
    }
  }

  final case class UnallocatedParties(unallocatedParties: Set[String])(
      val completionInfo: CompletionInfo
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger
        .Reject(unallocatedParties)
        .rpcStatus()
  }

  final case class DuplicateCommand(
      changeId: ChangeId,
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      LedgerApiErrors.ConsistencyErrors.DuplicateCommand
        .Reject(definiteAnswer = false, None, Some(changeId))
        .rpcStatus()
  }

  final case class MaxDeduplicationDurationExceeded(
      duration: Duration,
      maxDeduplicationDuration: Duration,
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField
        .Reject(
          s"The given deduplication duration of $duration exceeds the maximum deduplication duration of $maxDeduplicationDuration",
          Some(maxDeduplicationDuration),
        )
        .rpcStatus()
  }

  final case class NoLedgerConfiguration(
      completionInfo: CompletionInfo
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      LedgerApiErrors.RequestValidation.NotFound.LedgerConfiguration
        .RejectWithMessage(
          "Cannot validate ledger time"
        )
        .rpcStatus()
  }

  final case class InvalidLedgerTime(
      completionInfo: CompletionInfo,
      outOfRange: LedgerTimeModel.OutOfRange,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status = {
      val ledgerTime = outOfRange.ledgerTime.toInstant
      val ledgerTimeLowerBound = outOfRange.lowerBound.toInstant
      val ledgerTimeUpperBound = outOfRange.upperBound.toInstant
      LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
        .RejectEnriched(
          s"Ledger time $ledgerTime outside of range [$ledgerTimeLowerBound, $ledgerTimeUpperBound]",
          ledgerTime,
          ledgerTimeLowerBound,
          ledgerTimeUpperBound,
        )
        .rpcStatus()
    }
  }

  final case class DisclosedContractInvalid(
      contractId: ContractId,
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      LedgerApiErrors.ConsistencyErrors.DisclosedContractInvalid
        .Reject(contractId.coid)
        .rpcStatus()
  }
}
