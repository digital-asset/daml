// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import com.digitalasset.ledger.api.v1.{value => rpcvalue}

/**
  * A class representing a DAML contract of specific type (DAML template) with assigned contract ID and agreement text.
  *
  * @param contractId     Contract ID.
  * @param value          Contract instance as defined in DAML template (without `contractId` and `agreementText`).
  * @param agreementText  Agreement text. `None` means that we did not receive the `agreementText` from the server.
  *                       `Some("")` is a valid case, this means that the contract has `agreementText` set to an empty string
  *                       or agreement was not defined in DAML, which defaults to an empty string.
  * @param signatories    Signatories of the contract as defined in the DAML template
  * @param observers      Observers of the contract, both explicitly as defined in the DAML template and implicitly as
  *                       choice controllers.
  *
  * @tparam T             Contract template type parameter.
  */
final case class Contract[+T](
    contractId: Primitive.ContractId[T],
    value: T with Template[T],
    agreementText: Option[String],
    signatories: Seq[String],
    observers: Seq[String]) {
  def arguments: rpcvalue.Record = value.arguments
}

object Contract {
  type OfAny = Contract[Any]
}
