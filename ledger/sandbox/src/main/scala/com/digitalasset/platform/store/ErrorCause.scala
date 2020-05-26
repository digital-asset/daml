// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.lf.engine.{Error => LfError}

sealed abstract class ErrorCause extends Product with Serializable {
  def explain: String
}
object ErrorCause {

  final case class DamlLf(error: LfError) extends ErrorCause {
    override def explain: String = {
      val details =
        if (error.msg == error.detailMsg) "N/A"
        else error.detailMsg
      s"Command interpretation error in LF-DAMLe: ${error.msg}. Details: $details."
    }
  }

  final case class LedgerTime(retries: Int) extends ErrorCause {
    override def explain: String = s"Could not find a suitable ledger time after $retries retries"
  }
}
