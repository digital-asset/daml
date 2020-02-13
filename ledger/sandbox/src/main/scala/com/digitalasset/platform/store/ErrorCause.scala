// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store

import com.digitalasset.daml.lf.engine.{Error => LfError}

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

  final case class Sequencer(errors: Set[SequencingError]) extends ErrorCause {
    override def explain: String =
      errors
        .map {
          case SequencingError.InactiveDependencyError(cid, _) =>
            s"Contract ${cid.coid} was not found in ACS"
          case SequencingError.TimeBeforeError(cid, time, let, _) =>
            s"Dependency contract ${cid.coid} has higher time ($time) than current let ($let)"
          case SequencingError.DuplicateKey(gk) =>
            s"Duplicate contract key ${gk.key} for template ${gk.templateId}"
          case SequencingError.InvalidLookup(gk, cid, currentCid) =>
            s"Lookup by key ${gk.key} uses contract $cid instead of the current $currentCid"
        }
        .mkString("Sequencing errors: [", ", ", "]")
  }
}
