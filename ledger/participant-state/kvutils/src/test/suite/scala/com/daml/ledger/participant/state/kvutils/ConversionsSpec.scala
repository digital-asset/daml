// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlTransactionBlindingInfo
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlTransactionBlindingInfo.{
  DisclosureEntry,
  DivulgenceEntry
}
import com.daml.lf.crypto
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.IdString
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.{BlindingInfo, NodeId}
import com.daml.lf.value.Value.ContractId
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.immutable.ListSet

class ConversionsSpec extends WordSpec with Matchers {
  "Conversions" should {
    "correctly and deterministically encode Blindinginfo" in {
      val encoded = Conversions.encodeBlindingInfo(blindingInfoNotInOrder)
      encoded shouldBe orderedEncodedBlindingInfo
    }
  }

  private lazy val party0: IdString.Party = Ref.Party.assertFromString("party0")
  private lazy val party1: IdString.Party = Ref.Party.assertFromString("party1")
  private lazy val partySetNotInOrder = ListSet(party1, party0)
  private lazy val hashesNotInOrder: List[Hash] =
    List(crypto.Hash.hashPrivateKey("hash0"), crypto.Hash.hashPrivateKey("hash1")).sorted.reverse
  private lazy val contractId0 = ContractId.V1(hashesNotInOrder.tail.head)
  private lazy val contractId1 = ContractId.V1(hashesNotInOrder.head)
  private lazy val node0: NodeId = NodeId(0)
  private lazy val node1: NodeId = NodeId(1)
  private lazy val disclosureNotInOrder: Relation[NodeId, Ref.Party] =
    Map(node1 -> partySetNotInOrder, node0 -> partySetNotInOrder)
  private lazy val divulgenceNotInOrder: Relation[ContractId, Ref.Party] =
    Map(contractId1 -> partySetNotInOrder, contractId0 -> partySetNotInOrder)
  private lazy val blindingInfoNotInOrder = BlindingInfo(
    disclosure = disclosureNotInOrder,
    divulgence = divulgenceNotInOrder,
  )

  private lazy val partiesInOrder = List(party0, party1)

  private lazy val orderedEncodedBlindingInfo =
    DamlTransactionBlindingInfo.newBuilder
      .addAllDisclosures(
        List(
          DisclosureEntry.newBuilder
            .setNodeId(node0.index.toString)
            .addAllDisclosedToLocalParties(partiesInOrder.asInstanceOf[List[String]].asJava)
            .build,
          DisclosureEntry.newBuilder
            .setNodeId(node1.index.toString)
            .addAllDisclosedToLocalParties(partiesInOrder.asInstanceOf[List[String]].asJava)
            .build,
        ).asJava
      )
      .addAllDivulgences(
        List(
          DivulgenceEntry.newBuilder
            .setContractId(contractId0.coid)
            .addAllDivulgedToLocalParties(partiesInOrder.asInstanceOf[List[String]].asJava)
            .build,
          DivulgenceEntry.newBuilder
            .setContractId(contractId1.coid)
            .addAllDivulgedToLocalParties(partiesInOrder.asInstanceOf[List[String]].asJava)
            .build,
        ).asJava
      )
      .build
}
