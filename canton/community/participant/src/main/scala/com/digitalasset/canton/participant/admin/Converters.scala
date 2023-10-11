// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.ledger.client.binding.{Primitive as P}
import com.digitalasset.canton.topology.UniqueIdentifier

/** Conversions between types used by the admin-api */
object Converters {
  def toString(party: P.Party): String = P.Party.unwrap(party)
  def toParty(party: String): P.Party = P.Party(party)
  def toParty(party: UniqueIdentifier): P.Party = toParty(party.toProtoPrimitive)

  def toContractId[T](id: String): P.ContractId[T] = P.ContractId[T](id)
}
