// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.{Status => StatusProto}

import scala.util.Try

object GrpcStatuses {
  val DefiniteAnswerKey = "definite_answer"
  val CompletionOffsetKey = "completion_offset"

  def isDefiniteAnswer(status: StatusProto): Boolean =
    status.details.exists { any =>
      if (any.is(ErrorInfo.messageCompanion)) {
        Try(any.unpack(ErrorInfo.messageCompanion)).toOption
          .exists(isDefiniteAnswer)
      } else {
        false
      }
    }

  private def isDefiniteAnswer(errorInfo: ErrorInfo): Boolean =
    errorInfo.metadata.get(DefiniteAnswerKey).exists(value => java.lang.Boolean.valueOf(value))
}
