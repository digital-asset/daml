// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.MerkleSeq.{Branch, MerkleSeqElement, Singleton}
import com.digitalasset.canton.data.MerkleTree.{
  BlindSubtree,
  BlindingCommand,
  RevealIfNeedBe,
  RevealSubtree,
}
import com.digitalasset.canton.data.MerkleTreeTest.{AbstractLeaf, Leaf1}
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.protocol.RootHash
import com.google.protobuf.ByteString
import org.scalatest.prop.TableFor4
import org.scalatest.wordspec.AnyWordSpec

class MerkleSeqTest extends AnyWordSpec with BaseTest {

  import com.digitalasset.canton.protocol.ExampleTransactionFactory.*

  private val hashOps: HashOps = MerkleTreeTest.hashOps

  private def leaf(index: Int): Leaf1 =
    Leaf1(index)(AbstractLeaf.protocolVersionRepresentativeFor(testedProtocolVersion))

  private def singleton(index: Int): Singleton[Leaf1] =
    Singleton(leaf(index), testedProtocolVersion)(hashOps)

  private def branch(
      first: MerkleTree[MerkleSeqElement[Leaf1]],
      second: MerkleTree[MerkleSeqElement[Leaf1]],
  ): Branch[Leaf1] =
    Branch(first, second, testedProtocolVersion)(hashOps)

  private val Empty: MerkleSeq[Nothing] = MerkleSeq(None, testedProtocolVersion)(hashOps)

  private val OneUnblindedElement: MerkleSeq[Leaf1] =
    MerkleSeq(Some(singleton(0)), testedProtocolVersion)(hashOps)

  private val OneBlindedElement: MerkleSeq[Leaf1] =
    MerkleSeq(
      Some(
        Singleton(blinded(leaf(0)), testedProtocolVersion)(hashOps)
      ),
      testedProtocolVersion,
    )(hashOps)

  private val OneElementFullyBlinded: MerkleSeq[Leaf1] =
    MerkleSeq(Some(blinded(singleton(0))), testedProtocolVersion)(hashOps)

  private val TwoUnblindedElements: MerkleSeq[Leaf1] =
    MerkleSeq(Some(branch(singleton(0), singleton(1))), testedProtocolVersion)(hashOps)

  private val TwoBlindedElements: MerkleSeq[Leaf1] =
    MerkleSeq(Some(branch(blinded(singleton(0)), blinded(singleton(1)))), testedProtocolVersion)(
      hashOps
    )

  private val OneBlindedOneUnblinded: MerkleSeq[Leaf1] =
    MerkleSeq(Some(branch(blinded(singleton(0)), singleton(1))), testedProtocolVersion)(hashOps)

  private val TwoElementsRootHash: RootHash =
    TwoUnblindedElements.rootOrEmpty
      .getOrElse(throw new IllegalStateException("Missing root element"))
      .rootHash

  private val TwoElementsFullyBlinded: MerkleSeq[Leaf1] =
    MerkleSeq(Some(BlindedNode(TwoElementsRootHash)), testedProtocolVersion)(hashOps)

  private val SevenElementsLeft: Branch[Leaf1] =
    branch(branch(singleton(0), singleton(1)), branch(singleton(2), singleton(3)))
  private val SevenElementsRight: Branch[Leaf1] =
    branch(branch(singleton(4), singleton(5)), singleton(6))
  private val SevenElements: MerkleSeq[Leaf1] =
    MerkleSeq(Some(branch(SevenElementsLeft, SevenElementsRight)), testedProtocolVersion)(hashOps)
  private val SevenElementsRootUnblinded: MerkleSeq[Leaf1] =
    MerkleSeq(
      Some(branch(blinded(SevenElementsLeft), blinded(SevenElementsRight))),
      testedProtocolVersion,
    )(hashOps)

  private val testCases
      : TableFor4[String, Seq[MerkleTree[Leaf1]], MerkleSeq[Leaf1], MerkleSeq[Leaf1]] =
    Table[String, Seq[MerkleTree[Leaf1]], MerkleSeq[Leaf1], MerkleSeq[Leaf1]](
      ("name", "elements", "Merkle seq", "Merkle seq with root unblinded"),
      ("no elements", Seq.empty, Empty, Empty),
      ("one unblinded element", Seq(leaf(0)), OneUnblindedElement, OneBlindedElement),
      (
        "one blinded element",
        Seq(blinded(leaf(0))),
        OneElementFullyBlinded,
        OneElementFullyBlinded,
      ),
      ("two unblinded elements", Seq(leaf(0), leaf(1)), TwoUnblindedElements, TwoBlindedElements),
      (
        "one blinded and one unblinded element",
        Seq(blinded(leaf(0)), leaf(1)),
        OneBlindedOneUnblinded,
        TwoBlindedElements,
      ),
      (
        "two blinded elements",
        Seq(blinded(leaf(0)), blinded(leaf(1))),
        TwoElementsFullyBlinded,
        TwoElementsFullyBlinded,
      ),
      ("seven elements", (0 until 7).map(leaf), SevenElements, SevenElementsRootUnblinded),
    )

  testCases.forEvery { (name, elements, merkleSeq, merkleSeqWithRootUnblinded) =>
    s"A MerkleSeq with $name" can {
      "be constructed" in {
        MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(elements) shouldEqual merkleSeq
      }

      "be serialized" in {
        val merkleSeqP = merkleSeq.toByteString
        val merkleSeqDeserialized =
          MerkleSeq
            .fromByteString(
              (
                hashOps,
                (bytes: ByteString) => AbstractLeaf.fromByteString(testedProtocolVersion, bytes),
              ),
              testedProtocolVersion,
            )(merkleSeqP)
            .value

        merkleSeqDeserialized shouldEqual merkleSeq
      }

      val rootHash = merkleSeq.rootOrEmpty.map(_.rootHash)

      "blind the root" in {
        val policy = rootHash.toList.map(_ -> BlindSubtree).toMap
        val expectedBlindedSeq =
          MerkleSeq(rootHash.map(BlindedNode(_)), testedProtocolVersion)(hashOps)

        merkleSeq.doBlind(policy) shouldEqual expectedBlindedSeq
      }

      "blind all except the root" in {
        val policy: PartialFunction[RootHash, BlindingCommand] = {
          case hash if rootHash.contains(hash) => RevealIfNeedBe
          case _ => BlindSubtree
        }

        merkleSeq.doBlind(policy) shouldEqual merkleSeqWithRootUnblinded
      }

      "blind nothing" in {
        val policy = rootHash.toList.map(_ -> RevealSubtree).toMap

        merkleSeq.doBlind(policy) shouldEqual merkleSeq
      }

      "compute the right indices" in {
        val merkleSeq = MerkleSeq.fromSeq(hashOps, testedProtocolVersion)(elements)
        val indices: Seq[MerklePathElement] = MerkleSeq.indicesFromSeq(elements.size)

        assert(indices.sizeIs == elements.size)
        assert(indices.distinct == indices, "indices are distinct")
        val encodedIndices = indices.map(_.encodeDeterministically)
        assert(encodedIndices.distinct == encodedIndices, "encoded indices are distinct")

        val indexToElement = indices.zip(elements).toMap

        merkleSeq.unblindedElementsWithIndex.foreach { case (unblinded, index) =>
          val assigned = indexToElement(index)
          assert(assigned == unblinded, s"at index $index")
        }
      }

      "remain unchanged when mapped over with the identity function" in {
        merkleSeq.mapM(identity) shouldBe merkleSeq
      }
    }
  }

  "Mapping changes the tree as expected" in {
    val inc: Leaf1 => Leaf1 = { case Leaf1(i) =>
      Leaf1(i + 1)(AbstractLeaf.protocolVersionRepresentativeFor(testedProtocolVersion))
    }
    OneBlindedElement.mapM(inc) shouldBe OneBlindedElement
    OneElementFullyBlinded.mapM(inc) shouldBe OneElementFullyBlinded
    OneUnblindedElement.mapM(inc) shouldBe MerkleSeq(Some(singleton(1)), testedProtocolVersion)(
      hashOps
    )
    TwoUnblindedElements
      .mapM(inc) shouldBe MerkleSeq(
      Some(branch(singleton(1), singleton(2))),
      testedProtocolVersion,
    )(hashOps)
    SevenElements.mapM(inc.compose(inc)) shouldBe SevenElements.mapM(inc).mapM(inc)
  }
}
