// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.lf.engine.{Error => LfError}

private[platform] sealed abstract class ErrorCause extends Product with Serializable {
  def explain: String
}

private[platform] object ErrorCause {

  final case class DamlLf(error: LfError) extends ErrorCause {

    // TODO https://github.com/digital-asset/daml/issues/9974
    //  Review once LF errors are properly structured
    override def explain: String = {
      val details = {
        error match {
          case LfError.Interpretation(_, Some(detailMsg)) => detailMsg
          case _ => "N/A"
        }
      }
      s"Command interpretation error in LF-DAMLe: ${error.msg}. Details: $details."
    }
  }

  final case class LedgerTime(retries: Int) extends ErrorCause {
    override def explain: String = s"Could not find a suitable ledger time after $retries retries"
  }
}
