// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.Generators.*
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.topology.GeneratorsTopology.*
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.version.ProtocolVersion
import org.scalacheck.Arbitrary

final class GeneratorsData(
    protocolVersion: ProtocolVersion,
    generatorsProtocol: GeneratorsProtocol,
) {
  import generatorsProtocol.*

  implicit val activeContractArb: Arbitrary[ActiveContract] =
    Arbitrary(for {
      synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
      contract <- serializableContractArb(canHaveEmptyKey = true).arbitrary
      reassignmentCounter <- reassignmentCounterGen
    } yield ActiveContract.create(synchronizerId, contract, reassignmentCounter)(protocolVersion))

}
