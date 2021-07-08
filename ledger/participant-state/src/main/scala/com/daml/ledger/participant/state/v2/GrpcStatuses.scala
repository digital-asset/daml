// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import scala.util.Try

object GrpcStatuses {
  val DefiniteAnswerKey = "definite_answer"

  def isDefiniteAnswer(status: com.google.rpc.status.Status): Boolean =
    status.details.exists { any =>
      if (any.is(com.google.rpc.error_details.ErrorInfo.messageCompanion)) {
        Try(any.unpack(com.google.rpc.error_details.ErrorInfo.messageCompanion))
          .toOption
          .exists(isDefiniteAnswer)
      } else {
        false
      }
    }

  def isDefiniteAnswer(errorInfo: com.google.rpc.error_details.ErrorInfo): Boolean =
    errorInfo.metadata.getOrElse(DefiniteAnswerKey, "false") == "true"
}
