// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.Generators.*
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.topology.{GeneratorsTopology, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalacheck.Arbitrary

final class GeneratorsData(
    protocolVersion: ProtocolVersion,
    generatorsProtocol: GeneratorsProtocol,
    generatorsTopology: GeneratorsTopology,
) {
  import generatorsProtocol.*
  import generatorsTopology.*

  implicit val activeContractArb: Arbitrary[ActiveContractOld] =
    Arbitrary(
      for {
        synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
        contract <- serializableContractArb(canHaveEmptyKey = true).arbitrary
        reassignmentCounter <- reassignmentCounterGen
      } yield ActiveContractOld.create(synchronizerId, contract, reassignmentCounter)(
        protocolVersion
      )
    )

}
