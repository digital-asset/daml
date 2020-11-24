// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions.{
  decodeBlindingInfo,
  encodeBlindingInfo
}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlTransactionBlindingInfo
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlTransactionBlindingInfo.{
  DisclosureEntry,
  DivulgenceEntry
}
import com.daml.lf.crypto
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.{BlindingInfo, NodeId}
import com.daml.lf.value.Value.ContractId
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.immutable.{ListMap, ListSet}

class ConversionsSpec extends WordSpec with Matchers {
  "Conversions" should {
    "correctly and deterministically encode Blindinginfo" in {
      encodeBlindingInfo(wronglySortedBlindingInfo) shouldBe correctlySortedEncodedBlindingInfo
    }

    "correctly decode BlindingInfo" in {
      val decodedBlindingInfo = decodeBlindingInfo(correctlySortedEncodedBlindingInfo)
      decodedBlindingInfo.disclosure.toSet should contain theSameElementsAs wronglySortedBlindingInfo.disclosure.toSet
      decodedBlindingInfo.divulgence.toSet should contain theSameElementsAs wronglySortedBlindingInfo.divulgence.toSet
    }
  }

  private def newDisclosureEntry(node: NodeId, parties: List[String]) =
    DisclosureEntry.newBuilder
      .setNodeId(node.index.toString)
      .addAllDisclosedToLocalParties(parties.asJava)
      .build

  private def newDivulgenceEntry(contractId: String, parties: List[String]) =
    DivulgenceEntry.newBuilder
      .setContractId(contractId)
      .addAllDivulgedToLocalParties(parties.asJava)
      .build

  private lazy val party0: Party = Party.assertFromString("party0")
  private lazy val party1: Party = Party.assertFromString("party1")
  private lazy val contractId0: ContractId = ContractId.V1(wronglySortedHashes.tail.head)
  private lazy val contractId1: ContractId = ContractId.V1(wronglySortedHashes.head)
  private lazy val node0: NodeId = NodeId(0)
  private lazy val node1: NodeId = NodeId(1)
  private lazy val wronglySortedPartySet = ListSet(party1, party0)
  private lazy val wronglySortedHashes: List[Hash] =
    List(crypto.Hash.hashPrivateKey("hash0"), crypto.Hash.hashPrivateKey("hash1")).sorted.reverse
  private lazy val wronglySortedDisclosure: Relation[NodeId, Party] =
    ListMap(node1 -> wronglySortedPartySet, node0 -> wronglySortedPartySet)
  private lazy val wronglySortedDivulgence: Relation[ContractId, Party] =
    ListMap(contractId1 -> wronglySortedPartySet, contractId0 -> wronglySortedPartySet)
  private lazy val wronglySortedBlindingInfo = BlindingInfo(
    disclosure = wronglySortedDisclosure,
    divulgence = wronglySortedDivulgence,
  )
  private lazy val correctlySortedParties = List(party0, party1)
  private lazy val correctlySortedPartiesAsStrings =
    correctlySortedParties.asInstanceOf[List[String]]
  private lazy val correctlySortedEncodedBlindingInfo =
    DamlTransactionBlindingInfo.newBuilder
      .addAllDisclosures(
        List(
          newDisclosureEntry(node0, correctlySortedPartiesAsStrings),
          newDisclosureEntry(node1, correctlySortedPartiesAsStrings),
        ).asJava
      )
      .addAllDivulgences(
        List(
          newDivulgenceEntry(contractId0.coid, correctlySortedPartiesAsStrings),
          newDivulgenceEntry(contractId1.coid, correctlySortedPartiesAsStrings),
        ).asJava
      )
      .build
}
