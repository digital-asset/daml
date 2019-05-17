// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import com.digitalasset.ledger.api.v1.{value => rpcvalue}

final case class Contract[+T](
    contractId: Primitive.ContractId[T],
    value: T with Template[T],
    agreementText: Option[String]) {
  def arguments: rpcvalue.Record = value.arguments
}

object Contract {
  type OfAny = Contract[Any]
}
