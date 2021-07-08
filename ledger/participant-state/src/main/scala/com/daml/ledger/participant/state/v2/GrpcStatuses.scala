// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object GrpcStatuses {
  val DefiniteAnswerKey = "definite_answer"

  def isDefiniteAnswer(status: com.google.rpc.status.Status): Boolean =
    status.details.exists { any =>
      if (any.is(com.google.rpc.error_details.ErrorInfo.messageCompanion)) {
        Try(any.unpack(com.google.rpc.error_details.ErrorInfo.messageCompanion)).toOption
          .exists(isDefiniteAnswer)
      } else {
        false
      }
    }

  def isDefiniteAnswer(errorInfo: com.google.rpc.error_details.ErrorInfo): Boolean =
    errorInfo.metadata.getOrElse(DefiniteAnswerKey, "false") == "true"

  def completeWithOffset(
      incompleteStatus: com.google.rpc.status.Status,
      completionKey: String,
      completionOffset: Offset,
  ): com.google.rpc.status.Status = {
    val (errorInfo, errorInfoIndex): (com.google.rpc.error_details.ErrorInfo, Int) = {
      val iterator = incompleteStatus.details.iterator

      @tailrec def go(index: Int): Option[(com.google.rpc.error_details.ErrorInfo, Int)] =
        if (iterator.hasNext) {
          val next = iterator.next()
          if (next.is(com.google.rpc.error_details.ErrorInfo.messageCompanion)) {
            Try(next.unpack(com.google.rpc.error_details.ErrorInfo.messageCompanion)) match {
              case Success(errorInfo) => Some(errorInfo -> index)
              case _: Failure[_] => go(index + 1)
            }
          } else go(index + 1)
        } else None

      go(0).getOrElse(
        throw new IllegalArgumentException(
          s"No com.google.rpc.error_details.ErrorInfo found in details for $incompleteStatus"
        )
      )
    }

    val newErrorInfo =
      errorInfo
        .copy()
        .addMetadata(completionKey -> completionOffset.toHexString)
    val newDetails = incompleteStatus.details.updated(
      errorInfoIndex,
      com.google.protobuf.any.Any.pack(newErrorInfo),
    )
    incompleteStatus.withDetails(newDetails)
  }
}
