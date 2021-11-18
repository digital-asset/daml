// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils.testpackage.subpackage

import com.daml.error._
import com.daml.logging.LoggingContext

@Explanation("Groups mild errors together")
object MildErrors
    extends ErrorGroup()(parent = ErrorClass.root().extend(Grouping("Some grouping", None))) {

  @Explanation("Test: Things like this always happen.")
  @Resolution("Test: Why not ignore?")
  case object NotSoSeriousError
      extends ErrorCode(
        "TEST_ROUTINE_FAILURE_PLEASE_IGNORE",
        ErrorCategory.TransientServerFailure,
      ) {
    case class Error(someErrArg: String)(implicit val loggingContext: LoggingContext)
        extends BaseError {

      override def code: ErrorCode = NotSoSeriousError.code

      override def cause: String = "Some obscure cause"

      override def resources: Seq[(ErrorResource, String)] = Seq(
        (ErrorResource.LedgerId, LedgerIdResource)
      )
    }

    private[error] val LedgerIdResource = "some ledger id"
  }

}
