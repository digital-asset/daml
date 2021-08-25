package com.daml.util

import com.daml.ledger.api.domain.LedgerId
import scalaz.syntax.tag._

object LedgerIdOps {

  @throws[IllegalArgumentException]
  def tryPick(preferred: Option[LedgerId], optFallback: Option[LedgerId]): String = {
    val res =
      (preferred, optFallback) match {
        case (None, None) =>
          throw new IllegalArgumentException("No fallback ledger id was provided for the call")
        case (None, Some(v)) => v
        case (Some(v), None) => v
        case (Some(v1), Some(v2)) =>
          assert(
            v1 == v2,
            "The ledger id passed to calls needs to be the equal to the ledger id provided by the class if existing",
          )
          v1
      }
    res.unwrap
  }
}
