// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.Generators.*
import com.digitalasset.canton.protocol.GeneratorsProtocol.serializableContractGen
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.GeneratorsTopology.*
import com.digitalasset.canton.version.GeneratorsVersion.*
import org.scalacheck.Arbitrary
import org.scalatest.EitherValues.*

object GeneratorsData {

  implicit val activeContractArb: Arbitrary[ActiveContract] = {

    Arbitrary(for {
      protocolVersion <- protocolVersionArb.arbitrary
      domainId <- Arbitrary.arbitrary[DomainId]
      contract <- serializableContractGen(protocolVersion)
      transferCounter <- transferCounterOGen(protocolVersion)

      ac = ActiveContract.create(domainId, contract, transferCounter)(protocolVersion)

    } yield ac.value)

  }

}
