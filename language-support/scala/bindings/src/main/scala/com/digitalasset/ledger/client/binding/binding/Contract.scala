// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import com.daml.ledger.api.v1.{value => rpcvalue}

/** A class representing a Daml contract of specific type (Daml template) with assigned contract ID and agreement text.
  *
  * @param contractId     Contract ID.
  * @param value          Contract instance as defined in Daml template (without `contractId` and `agreementText`).
  * @param agreementText  Agreement text. `None` means that we did not receive the `agreementText` from the server.
  *                       `Some("")` is a valid case, this means that the contract has `agreementText` set to an empty string
  *                       or agreement was not defined in Daml, which defaults to an empty string.
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
    agreementText: Option[String],
    signatories: Seq[String],
    observers: Seq[String],
    key: Option[rpcvalue.Value],
) {
  def arguments: rpcvalue.Record = value.arguments
}

object Contract {
  type OfAny = Contract[Any]
}
