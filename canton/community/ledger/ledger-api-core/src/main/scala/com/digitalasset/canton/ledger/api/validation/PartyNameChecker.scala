// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.lf.data.Ref.Party

trait PartyNameChecker {
  def isKnownParty(party: Party): Boolean
}

object PartyNameChecker {

  final case class AllowPartySet(partySet: Set[Party]) extends PartyNameChecker {
    override def isKnownParty(party: Party): Boolean = partySet.contains(party)
  }

  case object AllowAllParties extends PartyNameChecker {
    override def isKnownParty(party: Party): Boolean = true
  }
}
