// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import com.daml.ledger.api.v1.{value => rpcvalue}

/** A class representing a Daml contract of specific type (Daml template) with assigned contract ID and agreement text.
  *
  * @param contractId     Contract ID.
  * @param value          Contract instance as defined in Daml template (without `contractId`).
  * @param signatories    Signatories of the contract as defined in the Daml template
  * @param observers      Observers of the contract, both explicitly as defined in the Daml template and implicitly as
  *                       choice controllers.
  * @param key            The value of the key of this contract, if defined by the template.
  *
  * @tparam T             Contract template type parameter.
  */
final case class Contract[+T](
    contractId: Primitive.ContractId[T],
    value: T with Template[T],
    signatories: Seq[String],
    observers: Seq[String],
    key: Option[rpcvalue.Value],
) {
  def arguments: rpcvalue.Record = value.arguments
}

object Contract {
  type OfAny = Contract[Any]
}
