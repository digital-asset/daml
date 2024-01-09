// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error.testpackage.subpackage

import com.daml.error.*

@Explanation("Mild error parent explanation")
object MildErrorsParent extends ErrorGroup()(ErrorClass.root()) {

  @Explanation("Groups mild errors together")
  object MildErrors extends ErrorGroup() {

    @Explanation("Test: Things like this always happen.")
    @Resolution("Test: Why not ignore?")
    case object NotSoSeriousError
        extends ErrorCode(
          "TEST_ROUTINE_FAILURE_PLEASE_IGNORE",
          ErrorCategory.TransientServerFailure,
        ) {

      final case class Error(
          someErrArg: String,
          override val context: Map[String, String],
          override val definiteAnswerO: Option[Boolean] = Some(true),
      ) extends BaseError {

        override def code: ErrorCode = NotSoSeriousError.code

        override def cause: String = "Some obscure cause"

        override def resources: Seq[(ErrorResource, String)] = Seq(
          (ErrorResource.LedgerId, LedgerIdResource)
        )
      }

      private[error] val LedgerIdResource = "some ledger id"
    }

  }

}
