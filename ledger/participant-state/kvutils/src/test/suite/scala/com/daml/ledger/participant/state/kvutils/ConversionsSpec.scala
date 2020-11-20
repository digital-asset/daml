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
      encodeBlindingInfo(unsortedBlindingInfo) shouldBe sortedEncodedBlindingInfo
    }

    "correctly decode BlindingInfo" in {
      decodeBlindingInfo(sortedEncodedBlindingInfo) shouldBe sortedBlindingInfo
    }
  }

  private lazy val party0: Party = Party.assertFromString("party0")
  private lazy val party1: Party = Party.assertFromString("party1")
  private lazy val unsortedPartySet = ListSet(party1, party0)
  private lazy val unsortedHashes: List[Hash] =
    List(crypto.Hash.hashPrivateKey("hash0"), crypto.Hash.hashPrivateKey("hash1")).sorted.reverse
  private lazy val contractId0: ContractId = ContractId.V1(unsortedHashes.tail.head)
  private lazy val contractId1: ContractId = ContractId.V1(unsortedHashes.head)
  private lazy val node0: NodeId = NodeId(0)
  private lazy val node1: NodeId = NodeId(1)
  private lazy val unsortedDisclosure: Relation[NodeId, Party] =
    Map(node1 -> unsortedPartySet, node0 -> unsortedPartySet)
  private lazy val unsortedDivulgence: Relation[ContractId, Party] =
    Map(contractId1 -> unsortedPartySet, contractId0 -> unsortedPartySet)
  private lazy val unsortedBlindingInfo = BlindingInfo(
    disclosure = unsortedDisclosure,
    divulgence = unsortedDivulgence,
  )
  private lazy val sortedPartySet: Set[Party] = ListSet(party0, party1)
  private lazy val sortedDisclosure: Relation[NodeId, Party] =
    ListMap(node0 -> sortedPartySet, node1 -> sortedPartySet)
  private lazy val sortedDivulgence: Map[ContractId, Set[Party]] =
    ListMap(contractId0 -> sortedPartySet, contractId1 -> sortedPartySet)
  private lazy val sortedBlindingInfo = BlindingInfo(
    disclosure = sortedDisclosure,
    divulgence = sortedDivulgence,
  )
  private lazy val sortedParties = List(party0, party1)
  private lazy val sortedPartiesAsStrings =
    sortedParties.asInstanceOf[List[String]]
  private lazy val sortedEncodedBlindingInfo =
    DamlTransactionBlindingInfo.newBuilder
      .addAllDisclosures(
        List(
          newDisclosureEntry(node0, sortedPartiesAsStrings),
          newDisclosureEntry(node1, sortedPartiesAsStrings),
        ).asJava
      )
      .addAllDivulgences(
        List(
          newDivulgenceEntry(contractId0.coid, sortedPartiesAsStrings),
          newDivulgenceEntry(contractId1.coid, sortedPartiesAsStrings),
        ).asJava
      )
      .build

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
}
