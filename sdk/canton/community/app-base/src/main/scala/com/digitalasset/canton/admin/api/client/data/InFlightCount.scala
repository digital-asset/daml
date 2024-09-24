// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

/** Represents in-flight command submissions and transactions. */
final case class InFlightCount(
    pendingSubmissions: NonNegativeInt,
    pendingTransactions: NonNegativeInt,
) extends PrettyPrinting {
  def exists: Boolean = pendingSubmissions.unwrap > 0 || pendingTransactions.unwrap > 0

  override def pretty: Pretty[InFlightCount] = prettyOfClass(
    param("pending submissions", _.pendingSubmissions),
    param("pending transactions", _.pendingTransactions),
  )
}
