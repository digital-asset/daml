// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.Generators
import com.digitalasset.canton.config.CantonRequireTypes.String185
import magnolify.scalacheck.auto.*
import org.scalacheck.Arbitrary

object GeneratorsTopology {
  import com.digitalasset.canton.config.GeneratorsConfig.*

  implicit val identifierArb: Arbitrary[Identifier] = Arbitrary(
    Generators.lengthLimitedStringGen(String185).map(s => Identifier.tryCreate(s.str))
  )

  implicit val domainMemberArb: Arbitrary[DomainMember] = genArbitrary
  implicit val authenticatedMemberArb: Arbitrary[AuthenticatedMember] = genArbitrary
  implicit val uniqueIdentifierArb: Arbitrary[UniqueIdentifier] = genArbitrary
  implicit val identityArb: Arbitrary[Identity] = genArbitrary
  implicit val keyOwnerArb: Arbitrary[KeyOwner] = genArbitrary
  implicit val domainIdArb: Arbitrary[DomainId] = genArbitrary
  implicit val mediatorIdArb: Arbitrary[MediatorId] = genArbitrary
  implicit val memberArb: Arbitrary[Member] = genArbitrary
  implicit val mediatorRefArb: Arbitrary[MediatorRef] = genArbitrary
  implicit val partyIdArb: Arbitrary[PartyId] = genArbitrary
}
