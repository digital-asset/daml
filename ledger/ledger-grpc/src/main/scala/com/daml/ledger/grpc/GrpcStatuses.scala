// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.grpc

import com.daml.ledger.offset.Offset
import com.google.protobuf.any.{Any => AnyProto}
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

  def completeWithOffset(incompleteStatus: StatusProto, completionOffset: Offset): StatusProto = {
    val (errorInfo, errorInfoIndex) =
      incompleteStatus.details.zipWithIndex
        .collectFirst {
          case (errorDetail, index) if errorDetail.is(ErrorInfo.messageCompanion) =>
            errorDetail.unpack(ErrorInfo.messageCompanion) -> index
        }
        .getOrElse(
          throw new IllegalArgumentException(
            s"No com.google.rpc.error_details.ErrorInfo found in details for $incompleteStatus"
          )
        )
    val extendedErrorInfoWithCompletionOffset =
      errorInfo
        .copy()
        .addMetadata(CompletionOffsetKey -> completionOffset.toHexString)
    val newDetails = incompleteStatus.details.updated(
      errorInfoIndex,
      AnyProto.pack(extendedErrorInfoWithCompletionOffset),
    )
    incompleteStatus.withDetails(newDetails)
  }
}
