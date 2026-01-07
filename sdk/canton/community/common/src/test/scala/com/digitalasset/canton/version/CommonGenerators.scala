// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.GeneratorsLf
import com.digitalasset.canton.data.{GeneratorsData, GeneratorsTrafficData}
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.protocol.messages.{
  GeneratorsLocalVerdict,
  GeneratorsMessages,
  GeneratorsVerdict,
}
import com.digitalasset.canton.sequencing.GeneratorsSequencing
import com.digitalasset.canton.sequencing.protocol.GeneratorsProtocol as GeneratorsProtocolSequencing
import com.digitalasset.canton.topology.GeneratorsTopology
import com.digitalasset.canton.topology.transaction.GeneratorsTransaction

final class CommonGenerators(protocolVersion: ProtocolVersion) {
  lazy val topology = new GeneratorsTopology(protocolVersion)
  lazy val generatorsSequencing = new GeneratorsSequencing(topology)
  lazy val lf = new GeneratorsLf(topology)
  lazy val protocol = new GeneratorsProtocol(protocolVersion, lf, topology)
  lazy val data = new GeneratorsData(protocolVersion, lf, protocol, topology)
  lazy val transaction =
    new GeneratorsTransaction(
      protocolVersion,
      lf,
      protocol,
      topology,
      generatorsSequencing,
    )
  lazy val localVerdict = GeneratorsLocalVerdict(protocolVersion, lf)
  lazy val verdict = GeneratorsVerdict(protocolVersion, localVerdict)
  lazy val trafficData = new GeneratorsTrafficData(
    protocolVersion,
    topology,
  )
  lazy val generatorsMessages = new GeneratorsMessages(
    protocolVersion,
    data,
    lf,
    protocol,
    localVerdict,
    verdict,
    topology,
    transaction,
    trafficData,
  )
  lazy val generatorsProtocolSeq = new GeneratorsProtocolSequencing(
    protocolVersion,
    generatorsMessages,
    topology,
  )

}
