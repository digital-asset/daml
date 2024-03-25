// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error._
import com.daml.error.definitions.ErrorGroups.ParticipantErrorGroup.LedgerApiErrorGroup
import com.daml.lf.engine.Error.Validation.ReplayMismatch
import com.daml.lf.engine.{Error => LfError}

@Explanation(
  "Errors raised by or forwarded by the Ledger API."
)
object LedgerApiErrors extends LedgerApiErrorGroup {

  val Admin: groups.AdminServices.type = groups.AdminServices
  val CommandExecution: groups.CommandExecution.type = groups.CommandExecution
  val AuthorizationChecks: groups.AuthorizationChecks.type = groups.AuthorizationChecks
  val ConsistencyErrors: groups.ConsistencyErrors.type = groups.ConsistencyErrors
  val RequestValidation: groups.RequestValidation.type = groups.RequestValidation
  val WriteServiceRejections: groups.WriteServiceRejections.type = groups.WriteServiceRejections

  val EarliestOffsetMetadataKey = "earliest_offset"

  @Explanation("""This error occurs if there was an unexpected error in the Ledger API.""")
  @Resolution("Contact support.")
  object InternalError
      extends ErrorCode(
        id = "LEDGER_API_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    case class Generic(
        message: String,
        override val throwableO: Option[Throwable] = None,
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = message,
          extraContext = Map("throwableO" -> throwableO.toString),
        )

    case class Preprocessing(
        err: LfError.Preprocessing.Internal
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = err.message)

    case class Validation(reason: ReplayMismatch)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"Observed un-expected replay mismatch: $reason"
        )

    case class Interpretation(
        where: String,
        message: String,
        detailMessage: Option[String],
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"Daml-Engine interpretation failed with internal error: $where / $message",
          extraContext = Map("detailMessage" -> detailMessage),
        )

    case class VersionService(message: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = message)

    case class Buffer(message: String, override val throwableO: Option[Throwable])(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = message, throwableO = throwableO)
  }
}
