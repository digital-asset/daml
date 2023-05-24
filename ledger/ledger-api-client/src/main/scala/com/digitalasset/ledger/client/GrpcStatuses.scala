// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.{Status => StatusProto}
import com.daml.error.ErrorCode

import scala.util.Try

object GrpcStatuses {

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
    errorInfo.metadata
      .get(ErrorCode.DefiniteAnswerKey)
      .exists(value => java.lang.Boolean.valueOf(value))
}
