// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlTransactionBlindingInfo
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlTransactionBlindingInfo.{
  DisclosureEntry,
  DivulgenceEntry
}
import com.daml.lf.crypto
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.IdString
import com.daml.lf.transaction.{BlindingInfo, NodeId}
import com.daml.lf.value.Value.ContractId
import org.scalatest.{Matchers, WordSpec}

class ConversionsSpec extends WordSpec with Matchers {
  "Conversions" should {
    "correctly encode Blindinginfo" in {
      val encoded = Conversions.encodeBlindingInfo(aBlindingInfo)
      encoded shouldBe anEncodedBlindingInfo
    }
  }

  private lazy val aParty: IdString.Party = Ref.Party.assertFromString("aParty")
  private lazy val aPartySet = Set(aParty)
  private lazy val aCid = ContractId.V1(crypto.Hash.hashPrivateKey("aCid"))
  private lazy val aNode: NodeId = NodeId(0)
  private lazy val aBlindingInfo = BlindingInfo(
    disclosure = Map(aNode -> aPartySet),
    divulgence = Map(aCid -> aPartySet)
  )

  private lazy val anEncodedBlindingInfo =
    DamlTransactionBlindingInfo.newBuilder
      .addDisclosure(
        DisclosureEntry.newBuilder.addDisclosedTo(aParty).setNodeId(aNode.index.toString)
      )
      .addDivulgence(
        DivulgenceEntry.newBuilder.addDivulgedTo(aParty).setContractId(aCid.coid)
      )
      .build
}
