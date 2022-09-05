// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.http.domain.LedgerId

package object util {
  private[http] def toLedgerId(ledgerId: LedgerId): com.daml.ledger.api.domain.LedgerId = {
    import scalaz.syntax.tag._
    com.daml.ledger.api.domain.LedgerId(ledgerId.unwrap)
  }
}
