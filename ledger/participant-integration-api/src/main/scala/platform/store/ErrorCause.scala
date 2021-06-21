// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.lf.engine.{Error => LfError}

private[platform] sealed abstract class ErrorCause extends Product with Serializable {
  def explain: String
}

private[platform] object ErrorCause {

  final case class DamlLf(error: LfError) extends ErrorCause {

    override def explain: String =
      error match {
        case LfError.Internal(where, message, detailMsg) =>
          s"Internal error in Daml Engine: $message. Location: $where. Details : $detailMsg"
        case _ =>
          s"Command interpretation error in Daml Engine: ${error.message}."
      }
  }

  final case class LedgerTime(retries: Int) extends ErrorCause {
    override def explain: String = s"Could not find a suitable ledger time after $retries retries"
  }
}
