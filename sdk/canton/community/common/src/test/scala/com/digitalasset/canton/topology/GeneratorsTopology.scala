// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.crypto.Fingerprint
import magnolify.scalacheck.auto.*
import org.scalacheck.Arbitrary

object GeneratorsTopology {
  import com.digitalasset.canton.config.GeneratorsConfig.*

  implicit val domainMemberArb: Arbitrary[DomainMember] = genArbitrary
  implicit val authenticatedMemberArb: Arbitrary[AuthenticatedMember] = genArbitrary
  implicit val fingerprintArb: Arbitrary[Fingerprint] = Arbitrary(
    string68Arb.arbitrary.map(Fingerprint.tryCreate)
  )
  implicit val namespaceArb: Arbitrary[Namespace] = Arbitrary(
    fingerprintArb.arbitrary.map(Namespace(_))
  )
  implicit val uniqueIdentifierArb: Arbitrary[UniqueIdentifier] = Arbitrary(
    for {
      id <- string185Arb.arbitrary
      fp <- string68Arb.arbitrary
    } yield UniqueIdentifier.tryCreate(id.str, fp.str)
  )
  implicit val identityArb: Arbitrary[Identity] = genArbitrary
  implicit val domainIdArb: Arbitrary[DomainId] = genArbitrary
  implicit val mediatorIdArb: Arbitrary[MediatorId] = genArbitrary
  implicit val memberArb: Arbitrary[Member] = genArbitrary
  implicit val partyIdArb: Arbitrary[PartyId] = genArbitrary
}
