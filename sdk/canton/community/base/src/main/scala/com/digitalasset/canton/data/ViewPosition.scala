// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.Order
import cats.instances.list.*
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex.Direction
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.google.protobuf.ByteString

/** A position encodes the path from a view in a transaction tree to its root.
  * The encoding must not depend on the hashes of the nodes.
  *
  * @param position The path from the view to the root as a singly-linked list.
  *                 The path starts at the view rather than the root so that paths to the root can
  *                 be shared.
  */
final case class ViewPosition(position: List[MerklePathElement]) extends PrettyPrinting {

  /** Adds a [[ViewPosition.MerklePathElement]] at the start of the path. */
  def +:(index: MerklePathElement): ViewPosition = new ViewPosition(index :: position)

  def encodeDeterministically: ByteString =
    DeterministicEncoding.encodeSeqWith(position)(_.encodeDeterministically)

  /** Reverse the position, as well as all contained MerkleSeqIndex path elements */
  def reverse: ViewPositionFromRoot = ViewPositionFromRoot(position.reverse.map(_.reverse))

  def toProtoV30: v30.ViewPosition = v30.ViewPosition(position = position.map(_.toProtoV30))

  override def pretty: Pretty[ViewPosition] = prettyOfClass(unnamedParam(_.position.mkShow()))
}

/** Same as [[ViewPosition]], with the position directed from the root to the leaf */
final case class ViewPositionFromRoot(position: List[MerklePathElement]) extends AnyVal {
  def isTopLevel: Boolean = position.size == 1
  def isEmpty: Boolean = position.isEmpty
}

object ViewPosition {

  /** The root [[ViewPosition]] has an empty path. */
  val root: ViewPosition = new ViewPosition(List.empty[MerklePathElement])

  implicit def prettyViewPosition: Pretty[ViewPosition] = {
    import com.digitalasset.canton.logging.pretty.Pretty.*
    prettyOfClass(unnamedParam(_.position))
  }

  private[canton] val orderViewPosition: Order[ViewPosition] =
    Order.by((_: ViewPosition).position.reverse)(
      catsKernelStdOrderForList(MerklePathElement.orderMerklePathElement)
    )

  def fromProtoV30(viewPositionP: v30.ViewPosition): ViewPosition = {
    val v30.ViewPosition(positionP) = viewPositionP
    val position = positionP.map(MerkleSeqIndex.fromProtoV30).toList
    ViewPosition(position)
  }

  /** A single element on a path through a Merkle tree. */
  sealed trait MerklePathElement extends Product with Serializable with PrettyPrinting {
    def encodeDeterministically: ByteString
    def reverse: MerklePathElement

    def toProtoV30: v30.MerkleSeqIndex
  }

  /** A leaf position in a [[MerkleSeq]], encodes as a path of directions from the leaf to the root.
    * The path is directed from the leaf to the root such that common subpaths can be shared.
    */
  final case class MerkleSeqIndex(index: List[Direction]) extends MerklePathElement {
    override def encodeDeterministically: ByteString =
      DeterministicEncoding.encodeSeqWith(index)(_.encodeDeterministically)

    override def pretty: Pretty[MerkleSeqIndex] =
      prettyOfString(_ => index.reverse.map(_.show).mkString(""))

    override lazy val reverse: MerkleSeqIndexFromRoot = MerkleSeqIndexFromRoot(index.reverse)

    override def toProtoV30: v30.MerkleSeqIndex =
      v30.MerkleSeqIndex(isRight = index.map(_ == Direction.Right))
  }

  /** Same as [[MerkleSeqIndex]], with the position directed from the root to the leaf */
  final case class MerkleSeqIndexFromRoot(index: List[Direction]) extends MerklePathElement {
    override def encodeDeterministically: ByteString =
      throw new UnsupportedOperationException(
        "MerkleSeqIndexFromRoot is for internal use only and should not be encoded"
      )

    override def pretty: Pretty[MerkleSeqIndexFromRoot] =
      prettyOfString(_ => index.map(_.show).mkString(""))

    override lazy val reverse: MerkleSeqIndex = MerkleSeqIndex(index.reverse)

    def toProtoV30: v30.MerkleSeqIndex = throw new UnsupportedOperationException(
      "MerkleSeqIndexFromRoot is for internal use only and should not be serialized"
    )
  }

  object MerklePathElement {
    private[data] val orderMerklePathElement: Order[MerklePathElement] = Order.from {
      case (MerkleSeqIndexFromRoot(index1), MerkleSeqIndexFromRoot(index2)) =>
        implicitly[Order[List[Direction]]].compare(index1, index2)

      case (MerkleSeqIndex(index1), element2) =>
        orderMerklePathElement.compare(MerkleSeqIndexFromRoot(index1.reverse), element2)

      case (element1, MerkleSeqIndex(index2)) =>
        orderMerklePathElement.compare(element1, MerkleSeqIndexFromRoot(index2.reverse))
    }
  }

  object MerkleSeqIndex {
    sealed trait Direction extends Product with Serializable with PrettyPrinting {
      def encodeDeterministically: ByteString
    }
    object Direction {

      implicit val orderDirection: Order[Direction] = Order.by {
        case Left => 0
        case Right => 1
      }

      case object Left extends Direction {
        override def encodeDeterministically: ByteString = DeterministicEncoding.encodeByte(0)

        override def pretty: Pretty[Left.type] = prettyOfString(_ => "L")
      }

      case object Right extends Direction {
        override def encodeDeterministically: ByteString = DeterministicEncoding.encodeByte(1)

        override def pretty: Pretty[Right.type] = prettyOfString(_ => "R")
      }
    }

    def fromProtoV30(merkleSeqIndexP: v30.MerkleSeqIndex): MerkleSeqIndex = {
      val v30.MerkleSeqIndex(isRightP) = merkleSeqIndexP
      MerkleSeqIndex(isRightP.map(if (_) Direction.Right else Direction.Left).toList)
    }
  }

  def isDescendant(descendant: ViewPosition, ancestor: ViewPosition): Boolean = {
    descendant.position.size >= ancestor.position.size &&
    descendant.position.drop(descendant.position.size - ancestor.position.size) == ancestor.position
  }
}
