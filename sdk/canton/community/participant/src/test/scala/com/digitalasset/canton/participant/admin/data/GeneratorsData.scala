// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.Generators.*
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.GeneratorsTopology.*
import com.digitalasset.canton.version.ProtocolVersion
import org.scalacheck.Arbitrary

final class GeneratorsData(
    protocolVersion: ProtocolVersion,
    generatorsProtocol: GeneratorsProtocol,
) {
  import generatorsProtocol.*

  implicit val activeContractArb: Arbitrary[ActiveContract] = {

    Arbitrary(for {
      domainId <- Arbitrary.arbitrary[DomainId]
      contract <- serializableContractArb(canHaveEmptyKey = true).arbitrary
      transferCounter <- transferCounterGen
    } yield ActiveContract.create(domainId, contract, transferCounter)(protocolVersion))

  }

}
