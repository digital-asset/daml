package com.daml.http

import com.daml.http.domain.LedgerId

package object util {
  private[http] def toLedgerId(ledgerId: LedgerId): com.daml.ledger.api.domain.LedgerId = {
    import scalaz.syntax.tag._
    com.daml.ledger.api.domain.LedgerId(ledgerId.unwrap)
  }
}
