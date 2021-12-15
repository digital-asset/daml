// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox.domain

import error.ContextualizedErrorLogger
import error.definitions.LedgerApiErrors
import lf.data.Time.Timestamp
import lf.transaction.GlobalKey
import platform.server.api.validation.ErrorFactories
import platform.store.appendonlydao.events.ContractId

import ledger.configuration.LedgerTimeModel
import com.google.protobuf.any.Any
import com.google.rpc.code.Code
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.Status

private[sandbox] sealed trait Rejection extends Product with Serializable {
  def toStatus: Status
}

private[sandbox] object Rejection {
  private val useSelfServiceErrorCodes = true
  // Temporary until consuming applications can deal with error codes properly
  private val errorFactories = ErrorFactories(useSelfServiceErrorCodes = useSelfServiceErrorCodes)

  final case class DuplicateKey(key: GlobalKey)(implicit
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
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      errorFactories.CommandRejections.inconsistentContractKeys(expectation, result)
  }

  final case class GenericRejectionFailure(details: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status = if (!useSelfServiceErrorCodes)
      Status.of(Code.ABORTED.value, "Invalid contract key", Seq.empty)
    else
      // TODO wrong error
      LedgerApiErrors.InternalError.VersionService(details).rpcStatus(None)
  }

  final case class CausalMonotonicityViolation(
      contractLedgerEffectiveTime: Timestamp,
      transactionLedgerEffectiveTime: Timestamp,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status = if (!useSelfServiceErrorCodes)
      Status.of(Code.ABORTED.value, "ADD DETAILS FOR LET failure", Seq.empty)
    else
      LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
        .RejectSimple("ADD DETAILS FOR LET failure")
        .rpcStatus(None)
  }

  final case class UnknownContracts(ids: Set[ContractId])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      errorFactories.CommandRejections.contractsNotFound(ids.map(_.coid))
  }

  final case class UnallocatedParties(unallocatedParties: Set[String])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ) extends Rejection {
    override def toStatus: Status =
      errorFactories.CommandRejections.partiesNotKnownToLedger(unallocatedParties)
  }

  final case class NoLedgerConfiguration()(implicit
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

  final case class InvalidLedgerTime(outOfRange: LedgerTimeModel.OutOfRange)(implicit
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
