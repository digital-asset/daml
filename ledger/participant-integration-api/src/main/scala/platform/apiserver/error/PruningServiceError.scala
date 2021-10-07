// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

import com.daml.error._
import com.daml.platform.apiserver.error.ErrorGroups.ParticipantErrorGroup.PruningServiceErrorGroup

trait PruningServiceError extends BaseError
object PruningServiceError extends PruningServiceErrorGroup {
  @Explanation("""Pruning has failed because of an internal server error.""")
  @Resolution("Identify the error in the server log.")
  object InternalServerError
      extends ErrorCode(
        id = "INTERNAL_PRUNING_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Error(reason: String)(implicit
        val loggingContext: ErrorCodeLoggingContext
    ) extends BaseError.Impl(
          cause = "Internal error such as the inability to write to the database"
        )
        with PruningServiceError
  }
}
