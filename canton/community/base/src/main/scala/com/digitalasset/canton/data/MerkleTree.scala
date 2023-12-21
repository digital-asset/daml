// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.implicits.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.MerkleSeq.MerkleSeqElement
import com.digitalasset.canton.data.MerkleTree.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{RootHash, v1}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.HasProtocolVersionedWrapper
import com.digitalasset.canton.{DiscardOps, ProtoDeserializationError}
import com.google.protobuf.ByteString
import monocle.Lens

import scala.collection.mutable

/** Encapsulate a Merkle tree.
  * Every node has an arbitrary number of children.
  * Every node has a `rootHash`.
  *
  * Every node may be blinded, i.e., the `rootHash` remains, but the children are removed. The `rootHash` does not
  * change if some children are blinded.
  *
  * @tparam A the runtime type of the class that actually implements this instance of `MerkleTree`.
  *           I.e., a proper implementation of this trait must be declared like
  *           `class MyMerkleTree extends MerkleTree[MyMerkleTree]`.
  */
trait MerkleTree[+A] extends Product with Serializable with PrettyPrinting {
  def subtrees: Seq[MerkleTree[_]]

  def rootHash: RootHash

  /** @return `Left(hash)`, if this is a blinded tree and `Right(tree)` otherwise.
    */
  def unwrap: Either[RootHash, A]

  /** Yields this instance with type `A`
    *
    * @throws java.lang.UnsupportedOperationException if this is blinded
    */
  def tryUnwrap: A = unwrap match {
    case Right(r) => r
    case Left(_) =>
      throw new UnsupportedOperationException(
        s"Unable to unwrap object of type ${getClass.getSimpleName}"
      )
  }

  lazy val blindFully: MerkleTree[A] = BlindedNode[A](rootHash)

  /** Blinds this Merkle tree according to the commands given by `blindingStatus`.
    * Traverses this tree in pre-order.
    *
    * @param blindingPolicy assigns blinding commands to subtrees
    * @throws java.lang.IllegalArgumentException if `blindingPolicy` does not assign a blinding command to some subtree
    *                                            and all ancestors of subtree have the blinding command `RevealIfNeedBe`
    */
  final def blind(
      blindingPolicy: PartialFunction[MerkleTree[_], BlindingCommand]
  ): MerkleTree[A] = {

    val optimizedBlindingPolicy = mutable.Map[RootHash, BlindingCommand]()
    optimizeBlindingPolicy(this).discard

    // Optimizes the blinding policy by replacing RevealIfNeedBe with RevealSubtree or BlindSubtree
    // whenever possible.
    // Consequently, if the optimized policy associates a node with RevealIfNeedBe, then:
    // - the node effectively needs to be revealed, (otherwise it would be "BlindSubtree")
    // - the node effectively needs to be copied (because it has a blinded descendant).
    //
    // Returns (allRevealed, allBlinded) indicating whether all nodes in tree are revealed/blinded.
    def optimizeBlindingPolicy(tree: MerkleTree[_]): (Boolean, Boolean) = {
      completeBlindingPolicy(tree) match {
        case BlindSubtree =>
          optimizedBlindingPolicy += tree.rootHash -> BlindSubtree
          (false, true)
        case RevealSubtree =>
          optimizedBlindingPolicy += tree.rootHash -> RevealSubtree
          (true, false)
        case RevealIfNeedBe =>
          val (allRevealed, allBlinded) = tree.subtrees
            .map(optimizeBlindingPolicy)
            .foldLeft((true, true)) { case ((r1, b1), (r2, b2)) =>
              (r1 && r2, b1 && b2)
            }
          val command =
            if (allBlinded) BlindSubtree else if (allRevealed) RevealSubtree else RevealIfNeedBe
          optimizedBlindingPolicy += tree.rootHash -> command
          (allRevealed && !allBlinded, allBlinded)
      }
    }

    def completeBlindingPolicy(tree: MerkleTree[_]): BlindingCommand =
      blindingPolicy.applyOrElse[MerkleTree[_], BlindingCommand](
        tree,
        {
          case _: BlindedNode[_] => BlindSubtree
          case _: MerkleSeqElement[_] => RevealIfNeedBe
          case _ =>
            throw new IllegalArgumentException(
              s"No blinding command specified for subtree of type ${tree.getClass}"
            )
        },
      )

    doBlind(optimizedBlindingPolicy)
  }

  /** Internal function that effectively performs the blinding. */
  private[data] final def doBlind(
      optimizedBlindingPolicy: PartialFunction[RootHash, BlindingCommand]
  ): MerkleTree[A] =
    optimizedBlindingPolicy(rootHash) match {
      case BlindSubtree => BlindedNode[A](rootHash)
      case RevealSubtree => this
      case RevealIfNeedBe => withBlindedSubtrees(optimizedBlindingPolicy)
    }

  /** Yields a copy of this that results from applying `doBlind(optimizedBlindingPolicy)` to all children. */
  private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, BlindingCommand]
  ): MerkleTree[A]

  def isBlinded: Boolean = unwrap.isLeft

  lazy val isFullyUnblinded: Boolean = unwrap.isRight && subtrees.forall(_.isFullyUnblinded)

  lazy val hasAllLeavesBlinded: Boolean = unwrap.isRight && subtrees.collectFirst {
    case l: MerkleTreeLeaf[_] => l
  }.isEmpty
}

/** An inner node of a Merkle tree.
  * Has no data, no salt, and an arbitrary number of subtrees.
  * An inner node is considered unblinded.
  */
abstract class MerkleTreeInnerNode[+A](val hashOps: HashOps) extends MerkleTree[A] {
  this: A =>

  override lazy val rootHash: RootHash = {
    val hashBuilder = hashOps.build(HashPurpose.MerkleTreeInnerNode).add(subtrees.length)
    subtrees.foreach { subtree =>
      // All hashes within a Merkle tree use the same hash algorithm and are therefore of fixed length,
      // so no length prefix is needed.
      hashBuilder.addWithoutLengthPrefix(subtree.rootHash.getCryptographicEvidence)
    }
    RootHash(hashBuilder.finish())
  }

  override def unwrap: Either[RootHash, A] = Right(this)
}

/** A leaf of a Merkle tree.
  * Has data, a salt, and no children.
  * A leaf is considered unblinded.
  */
abstract class MerkleTreeLeaf[+A <: HasCryptographicEvidence](val hashOps: HashOps)
    extends MerkleTree[A] {
  this: A =>

  /** The `HashPurpose` to be used for computing the root hash.
    * Must uniquely identify the type of this instance.
    * Must be different from `HashPurpose.MerkleTreeInnerNode`.
    *
    * @see [[com.digitalasset.canton.crypto.HashBuilder]]
    */
  def hashPurpose: HashPurpose

  def salt: Salt

  def subtrees: Seq[MerkleTree[_]] = Seq.empty

  override lazy val rootHash: RootHash = {
    if (hashPurpose == HashPurpose.MerkleTreeInnerNode) {
      throw new IllegalStateException(
        s"HashPurpose must not be a ${HashPurpose.description(hashPurpose)}"
      )
    }
    val hash = hashOps
      .build(hashPurpose)
      .add(salt.forHashing)
      .add(tryUnwrap.getCryptographicEvidence)
      .finish()
    RootHash(hash)
  }

  override def unwrap = Right(this)

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, BlindingCommand]
  ): A with MerkleTree[A] = this
}

/** A blinded node of a Merkle tree.
  * Has no subtrees, as they are all blinded.
  */
final case class BlindedNode[+A](rootHash: RootHash) extends MerkleTree[A] {
  override def subtrees: Seq[MerkleTree[_]] = Seq.empty

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, BlindingCommand]
  ): MerkleTree[A] = this

  override def unwrap: Either[RootHash, A] = Left(rootHash)

  override def pretty: Pretty[BlindedNode.this.type] = prettyOfClass(unnamedParam(_.rootHash))
}

object MerkleTree {
  type VersionedMerkleTree[A] = MerkleTree[A] with HasProtocolVersionedWrapper[_]

  /** Command indicating whether and how to blind a Merkle tree. */
  sealed trait BlindingCommand extends Product with Serializable

  case object BlindSubtree extends BlindingCommand

  case object RevealSubtree extends BlindingCommand

  /** Reveal the node if at least one descendant is revealed as well */
  case object RevealIfNeedBe extends BlindingCommand

  def toBlindableNodeV1(node: MerkleTree[HasProtocolVersionedWrapper[_]]): v1.BlindableNode =
    v1.BlindableNode(blindedOrNot = node.unwrap match {
      case Left(h) => v1.BlindableNode.BlindedOrNot.BlindedHash(h.toProtoPrimitive)
      case Right(n) =>
        v1.BlindableNode.BlindedOrNot.Unblinded(
          n.toByteString
        )
    })

  def fromProtoOptionV1[NodeType](
      protoNode: Option[v1.BlindableNode],
      f: ByteString => ParsingResult[MerkleTree[NodeType]],
  ): ParsingResult[MerkleTree[NodeType]] = {
    import v1.BlindableNode.BlindedOrNot as BON
    protoNode.map(_.blindedOrNot) match {
      case Some(BON.BlindedHash(hashBytes)) =>
        RootHash
          .fromProtoPrimitive(hashBytes)
          .bimap(
            e => ProtoDeserializationError.OtherError(s"Failed to deserialize root hash: $e"),
            hash => BlindedNode.apply[NodeType](hash),
          )
      case Some(BON.Unblinded(unblindedNode)) => f(unblindedNode)
      case Some(BON.Empty) | None =>
        Left(ProtoDeserializationError.OtherError(s"Missing blindedOrNot specification"))
    }
  }

  def tryUnwrap[A <: MerkleTree[A]]: Lens[MerkleTree[A], A] =
    Lens[MerkleTree[A], A](_.tryUnwrap)(unwrapped => _ => unwrapped)
}
