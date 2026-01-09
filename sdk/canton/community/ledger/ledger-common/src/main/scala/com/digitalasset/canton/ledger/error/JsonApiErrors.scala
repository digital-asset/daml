// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error

import com.digitalasset.base.error.{
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCode,
  Explanation,
  Resolution,
}
import com.digitalasset.canton.ledger.error.ParticipantErrorGroup.LedgerApiErrorGroup.JsonApiErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext

@Explanation(
  "Errors specific for Json Ledger API."
)
object JsonApiErrors extends JsonApiErrorGroup {
  @Explanation(
    "This error occurs when the Ledger JSON API server fails to find a suitable set of package-ids for decoding a given set of commands in a request."
  )
  @Resolution(
    """Inspect the error message and ensure that:
      |1. The requested packages are uploaded on the participant node.
      |2. The topology state satisfies the required vetting for the interested parties"""
  )
  object JsonApiPackageSelectionFailed
      extends ErrorCode(
        id = "JSON_API_PACKAGE_SELECTION_FAILED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(override val cause: String)(implicit
        errorLogger: ErrorLoggingContext
    ) extends DamlErrorWithDefiniteAnswer(cause = cause)
  }

  @Explanation(
    s"""This happens when the number of returned elements is equal or greater to the node limit.
       |The limit is defined by the node configuration and can be changed by the operator.
       | Check 'canton.participants.<participant-id>.http-ledger-api.websocket-config.http-list-max-elements-limit'.
       | Notice: If configured in the participant node config, the actual returned number is the minimum between the "limit" query parameter in the request and this one .
       | If the request query "limit" is the same as the mentioned configuration, it allows to return result set up to server limit without producing error.
       |"""
  )
  @Resolution("""
   |1. Preferred solution is to use websocket endpoint to get results in chunks.
   |2. It is possible to increase the limit by changing the node configuration 'http-list-max-elements-limit' but this may have
   |a severe impact on a node performance.""")
  object MaximumNumberOfElements
      extends ErrorCode(
        id = "JSON_API_MAXIMUM_LIST_ELEMENTS_NUMBER_REACHED",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Reject(
        value: Int,
        limit: Long,
    )(implicit
        errorLogger: ErrorLoggingContext
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The number of matching elements ($value) is greater than the node limit ($limit)."
        )
  }
}
