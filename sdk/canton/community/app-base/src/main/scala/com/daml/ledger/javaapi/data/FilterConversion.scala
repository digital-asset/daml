// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.v1.transaction_filter.{TransactionFilter as ScalaTransactionFilter}

// TODO(i15321): hack to circumvent the package-private `toProto` method on `TransactionFilter`
object FilterConversion {

  def apply(filter: TransactionFilter): ScalaTransactionFilter =
    ScalaTransactionFilter.fromJavaProto(filter.toProto)

}
