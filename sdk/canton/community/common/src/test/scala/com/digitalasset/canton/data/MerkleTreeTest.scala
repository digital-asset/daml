// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.MerkleTree.{
  BlindSubtree,
  RevealIfNeedBe,
  RevealSubtree,
  VersionedMerkleTree,
}
import com.digitalasset.canton.data.MerkleTreeTest.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.RootHash
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  DeterministicEncoding,
  HasCryptographicEvidence,
}
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.{BaseTest, ProtoDeserializationError, data}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class MerkleTreeTest extends AnyWordSpec with BaseTest {

  val fullyBlindedTreeHash: RootHash = RootHash(TestHash.digest("test"))
  val fullyBlindedTree: BlindedNode[Nothing] = BlindedNode(fullyBlindedTreeHash)

  val singletonLeaf1: Leaf1 =
    Leaf1(1)(AbstractLeaf.protocolVersionRepresentativeFor(testedProtocolVersion))
  val singletonLeaf2: Leaf2 =
    Leaf2(2)(AbstractLeaf.protocolVersionRepresentativeFor(testedProtocolVersion))
  val singletonLeaf3: Leaf3 =
    Leaf3(3)(AbstractLeaf.protocolVersionRepresentativeFor(testedProtocolVersion))

  def singletonLeafHash(index: Int): RootHash = RootHash {
    val salt = TestSalt.generateSalt(index)
    val data = DeterministicEncoding.encodeInt(index)
    val hashBuilder = TestHash.build
    hashBuilder
      .add(salt.forHashing)
      .add(data)
      .finish()
  }

  val singletonInnerNode: InnerNode1 = InnerNode1()
  val singletonInnerNodeHash: RootHash = RootHash {
    val hashBuilder = TestHash.build(HashPurpose.MerkleTreeInnerNode)
    hashBuilder.add(0).finish()
  }

  val innerNodeWithSingleChild: InnerNode1 = InnerNode1(singletonLeaf1)
  val innerNodeWithSingleChildHash: RootHash = RootHash {
    val hashBuilder = TestHash.build(HashPurpose.MerkleTreeInnerNode)
    hashBuilder
      .add(1)
      .addWithoutLengthPrefix(singletonLeafHash(1).getCryptographicEvidence)
      .finish()
  }

  val innerNodeWithTwoChildren: InnerNode2 = InnerNode2(singletonLeaf2, singletonLeaf3)
  val innerNodeWithTwoChildrenHash: RootHash = RootHash {
    val hashBuilder = TestHash.build(HashPurpose.MerkleTreeInnerNode)
    hashBuilder
      .add(2)
      .addWithoutLengthPrefix(singletonLeafHash(2).getCryptographicEvidence)
      .addWithoutLengthPrefix(singletonLeafHash(3).getCryptographicEvidence)
      .finish()
  }

  val threeLevelTree: InnerNode1 = InnerNode1(singletonLeaf1, innerNodeWithTwoChildren)
  val threeLevelTreeHash: RootHash = RootHash {
    val hashBuilder = TestHash.build(HashPurpose.MerkleTreeInnerNode)
    hashBuilder
      .add(2)
      .addWithoutLengthPrefix(singletonLeafHash(1).getCryptographicEvidence)
      .addWithoutLengthPrefix(innerNodeWithTwoChildrenHash.getCryptographicEvidence)
      .finish()
  }

  val threeLevelTreePartiallyBlinded: InnerNode1 =
    InnerNode1(BlindedNode(singletonLeafHash(1)), innerNodeWithTwoChildren)

  "Every Merkle tree" must {

    val testCases =
      Table[MerkleTree[_], RootHash, Boolean, Boolean](
        ("Merkle tree", "Expected root hash", "Is fully unblinded", "Has all leaves blinded"),
        (fullyBlindedTree, fullyBlindedTreeHash, false, false),
        (singletonLeaf1, singletonLeafHash(1), true, true),
        (singletonInnerNode, singletonInnerNodeHash, true, true),
        (innerNodeWithSingleChild, innerNodeWithSingleChildHash, true, false),
        (innerNodeWithTwoChildren, innerNodeWithTwoChildrenHash, true, false),
        (threeLevelTree, threeLevelTreeHash, true, false),
        (threeLevelTreePartiallyBlinded, threeLevelTreeHash, false, true),
      )

    "have the expected root hash" in {
      forEvery(testCases)((merkleTree, expectedRootHash, _, _) =>
        merkleTree.rootHash should equal(expectedRootHash)
      )
    }

    "wrap something (unless it is blinded)" in {
      forEvery(testCases)((merkleTree, _, _, _) => {
        merkleTree match {
          case BlindedNode(rootHash) =>
            merkleTree.unwrap should equal(Left(rootHash))
            an[UnsupportedOperationException] should be thrownBy merkleTree.tryUnwrap
          case _ =>
            merkleTree.unwrap should equal(Right(merkleTree))
            merkleTree.tryUnwrap should equal(merkleTree)
        }
      })
    }

    "correctly say whether it is fully unblinded" in {
      forEvery(testCases) { case (merkleTree, _, fullyUnblinded, _) =>
        merkleTree.isFullyUnblinded shouldEqual fullyUnblinded
      }
    }

    "correctly say whether all leaves are blinded" in {
      forEvery(testCases) { case (merkleTree, _, _, leavesBlinded) =>
        merkleTree.hasAllLeavesBlinded shouldEqual leavesBlinded
      }
    }

    "be blindable" in {
      forEvery(testCases)((merkleTree, _, _, _) => {
        val blindedTree = BlindedNode(merkleTree.rootHash)
        merkleTree.blind({ case t if t eq merkleTree => BlindSubtree }) should equal(blindedTree)
        merkleTree.blind({ case _ => RevealIfNeedBe }) should equal(blindedTree)
      })
    }

    "remain unchanged on Reveal" in {
      forEvery(testCases)((merkleTree, _, _, _) => {
        merkleTree.blind({ case t if t eq merkleTree => RevealSubtree }) should equal(merkleTree)
      })
    }

    "escalate a malformed blinding policy" in {
      forEvery(testCases)((merkleTree, _, _, _) => {
        merkleTree match {
          case BlindedNode(_) => // skip test, as the blinding policy cannot be malformed
          case _ =>
            an[IllegalArgumentException] should be thrownBy merkleTree.blind(PartialFunction.empty)
        }
      })
    }
  }

  "A deep Merkle tree" can {
    "be partially blinded" in {
      // Blind threeLevelTree to threeLevelTreePartiallyBlinded.
      threeLevelTree.blind({
        case InnerNode1(_*) => RevealIfNeedBe
        case Leaf1(_) => BlindSubtree
        case InnerNode2(_*) => RevealSubtree
      }) should equal(threeLevelTreePartiallyBlinded)

      // Use RevealIfNeedBe while blinding one child and revealing the other child.
      innerNodeWithTwoChildren.blind({
        case InnerNode2(_*) => RevealIfNeedBe
        case Leaf2(_) => BlindSubtree
        case Leaf3(_) => RevealSubtree
      }) should equal(InnerNode2(BlindedNode(singletonLeafHash(2)), singletonLeaf3))

      // Use RevealIfNeedBe while blinding all children.
      innerNodeWithTwoChildren.blind({
        case InnerNode2(_*) => RevealIfNeedBe
        case Leaf2(_) => BlindSubtree
        case Leaf3(_) => BlindSubtree
      }) should equal(BlindedNode(innerNodeWithTwoChildrenHash))

      // Use RevealIfNeedBe while blinding all children.
      // Make sure that grandchildren are blinded, even though the policy assigns Reveal to them.
      threeLevelTree.blind({
        case InnerNode1(_*) => RevealIfNeedBe
        case Leaf1(_) => BlindSubtree
        case InnerNode2(_*) => BlindSubtree
        case _ => RevealSubtree
      }) should equal(BlindedNode(threeLevelTreeHash))
    }
  }
}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object MerkleTreeTest {
  type VersionedAbstractLeaf = AbstractLeaf[_ <: VersionedMerkleTree[_]]
  val hashOps = new SymbolicPureCrypto

  object AbstractLeaf extends HasProtocolVersionedCompanion[VersionedAbstractLeaf] {
    override def name: String = "AbstractLeaf"
    override def supportedProtoVersions: data.MerkleTreeTest.AbstractLeaf.SupportedProtoVersions =
      SupportedProtoVersions(
        ProtoVersion(0) -> LegacyProtoConverter.raw(
          ProtocolVersion.v3,
          fromProto(0),
          _.getCryptographicEvidence,
        ),
        ProtoVersion(1) -> VersionedProtoConverter.raw(
          ProtocolVersion.smallestStable,
          fromProto(1),
          _.getCryptographicEvidence,
        ),
      )

    def fromProto(protoVersion: Int)(bytes: ByteString): ParsingResult[Leaf1] =
      for {
        rpv <- protocolVersionRepresentativeFor(ProtoVersion(protoVersion))
        leaf <- leafFromByteString(i => Leaf1(i)(rpv))(bytes).leftMap(e =>
          ProtoDeserializationError.OtherError(e.message)
        )
      } yield leaf

  }

  abstract class AbstractLeaf[A <: MerkleTree[
    _
  ] with HasCryptographicEvidence with HasProtocolVersionedWrapper[_]](
      val index: Int
  ) extends MerkleTreeLeaf[A](MerkleTreeTest.hashOps)
      with HasCryptographicEvidence
      with HasProtocolVersionedWrapper[VersionedAbstractLeaf] {
    this: A =>

    override def salt: Salt = TestSalt.generateSalt(index)

    override def getCryptographicEvidence: ByteString = DeterministicEncoding.encodeInt(index)

    override val hashPurpose: HashPurpose = TestHash.testHashPurpose

    override def pretty: Pretty[AbstractLeaf[A]] = prettyOfClass(unnamedParam(_.index))
  }

  def leafFromByteString[L <: AbstractLeaf[_]](
      mkLeaf: Int => L
  )(bytes: ByteString): Either[DeserializationError, L] =
    for {
      indexAndRemainder <- DeterministicEncoding.decodeInt(bytes)
      (index, remainder) = indexAndRemainder
      _ <- Either.cond(
        remainder.isEmpty,
        (),
        DefaultDeserializationError(
          "Unable to deserialize Int from ByteString. Remaining bytes: "
        ),
      )
    } yield mkLeaf(index)

  final case class Leaf1(override val index: Int)(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[AbstractLeaf.type]
  ) extends AbstractLeaf[Leaf1](index) {
    override protected lazy val companionObj: AbstractLeaf.type =
      AbstractLeaf
  }

  final case class Leaf2(override val index: Int)(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[AbstractLeaf.type]
  ) extends AbstractLeaf[Leaf2](index) {
    override protected lazy val companionObj: AbstractLeaf.type =
      AbstractLeaf
  }

  final case class Leaf3(override val index: Int)(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[AbstractLeaf.type]
  ) extends AbstractLeaf[Leaf3](index) {
    override protected lazy val companionObj: AbstractLeaf.type =
      AbstractLeaf
  }

  abstract class AbstractInnerNode[A](
      val create: Seq[MerkleTree[_]] => MerkleTree[A],
      val subtrees: MerkleTree[_]*
  ) extends MerkleTreeInnerNode[A](MerkleTreeTest.hashOps) {
    this: A =>

    override private[data] def withBlindedSubtrees(
        blindingCommandPerNode: PartialFunction[RootHash, MerkleTree.BlindingCommand]
    ): MerkleTree[A] =
      create(subtrees.map(_.doBlind(blindingCommandPerNode)))

    override def pretty: Pretty[AbstractInnerNode[A]] = prettyOfClass(
      param("subtrees", _.subtrees)
    )
  }

  final case class InnerNode1(override val subtrees: MerkleTree[_]*)
      extends AbstractInnerNode[InnerNode1](InnerNode1.apply, subtrees: _*) {}

  final case class InnerNode2(override val subtrees: MerkleTree[_]*)
      extends AbstractInnerNode[InnerNode2](InnerNode2.apply, subtrees: _*) {}
}
