// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox.domain

import com.daml.ledger.participant.state.v2.{CompletionInfo, Update}
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import error.ContextualizedErrorLogger
import error.definitions.LedgerApiErrors
import ledger.configuration.LedgerTimeModel
import ledger.participant.state.kvutils.errors.KVErrors
import lf.data.Time.Timestamp
import lf.transaction.GlobalKey
import platform.server.api.validation.ErrorFactories
import platform.store.appendonlydao.events.ContractId
import com.google.protobuf.any.Any
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.Status

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
      val completionInfo: CompletionInfo,
      errorFactories: ErrorFactories,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      errorFactories.CommandRejections.duplicateContractKey(
        reason = "DuplicateKey: contract key is not unique",
        key = key,
      )
  }

  final case class InconsistentContractKey(
      expectation: Option[ContractId],
      result: Option[ContractId],
  )(val completionInfo: CompletionInfo, errorFactories: ErrorFactories)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      errorFactories.CommandRejections.inconsistentContractKeys(expectation, result)
  }

  final case class LedgerBridgeInternalError(_err: Throwable, completionInfo: CompletionInfo)(
      implicit contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status = LedgerApiErrors.InternalError
      .UnexpectedOrUnknownException(_err)
      .rpcStatus(None)
  }

  final case class TransactionInternallyInconsistentKey(
      key: GlobalKey,
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status = {
      // TODO SoX: Verify and extract the error from KV domain
      KVErrors.Internal.InternallyInconsistentKeys.Reject().asStatus
    }
  }

  final case class TransactionInternallyInconsistentContract(
      key: GlobalKey,
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status = {
      // TODO SoX: Verify and extract the error from KV domain
      KVErrors.Internal.InternallyInconsistentKeys.Reject().asStatus
    }
  }

  final case class CausalMonotonicityViolation(
      contractLedgerEffectiveTime: Timestamp,
      transactionLedgerEffectiveTime: Timestamp,
  )(val completionInfo: CompletionInfo, errorFactories: ErrorFactories)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      errorFactories.CommandRejections.invalidLedgerTime(
        s"Ledger effective time for one of the contracts ($contractLedgerEffectiveTime) is greater than the ledger effective time of the transaction ($transactionLedgerEffectiveTime)"
      )
  }

  final case class UnknownContracts(ids: Set[ContractId])(
      val completionInfo: CompletionInfo,
      errorFactories: ErrorFactories,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      errorFactories.CommandRejections.contractsNotFound(ids.map(_.coid))
  }

  final case class UnallocatedParties(unallocatedParties: Set[String])(
      val completionInfo: CompletionInfo,
      errorFactories: ErrorFactories,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      errorFactories.CommandRejections.partiesNotKnownToLedger(unallocatedParties)
  }

  final case class NoLedgerConfiguration(
      completionInfo: CompletionInfo,
      errorFactories: ErrorFactories,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
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

  final case class InvalidLedgerTime(
      completionInfo: CompletionInfo,
      outOfRange: LedgerTimeModel.OutOfRange,
  )(
      errorFactories: ErrorFactories
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
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
