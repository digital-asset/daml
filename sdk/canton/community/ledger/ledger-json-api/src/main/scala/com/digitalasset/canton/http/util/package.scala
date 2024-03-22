// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.digitalasset.canton.http.domain.LedgerId

package object util {
 def toLedgerId(ledgerId: LedgerId): com.digitalasset.canton.ledger.api.domain.LedgerId = {
    import scalaz.syntax.tag._
    com.digitalasset.canton.ledger.api.domain.LedgerId(ledgerId.unwrap)
  }
}
