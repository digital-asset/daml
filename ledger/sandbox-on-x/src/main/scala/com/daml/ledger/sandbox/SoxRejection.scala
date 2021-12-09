// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox

import error.ContextualizedErrorLogger
import error.definitions.LedgerApiErrors
import ledger.sandbox.ConflictCheckingLedgerBridge.Submission
import ledger.sandbox.ConflictCheckingLedgerBridge.Submission.Transaction
import lf.data.Time.Timestamp
import lf.transaction.GlobalKey
import platform.server.api.validation.ErrorFactories
import platform.store.appendonlydao.events.ContractId

import com.google.rpc.code.Code
import com.google.rpc.status.Status

sealed trait SoxRejection extends Submission {
  def toStatus: Status

  val originalTx: Submission.Transaction
}

object SoxRejection {
  private val useSelfServiceErrorCodes = true
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
}
