// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.MerkleSeq.MerkleSeqElement
import com.digitalasset.canton.data.MerkleTree.*
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex.Direction
import com.digitalasset.canton.data.ViewPosition.{
  MerklePathElement,
  MerkleSeqIndex,
  MerkleSeqIndexFromRoot,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{RootHash, v30}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import scala.annotation.tailrec

/** Wraps a sequence that is also a [[MerkleTree]].
  * Elements are arranged in a balanced binary tree. As a result, if all except one element are blinded, the
  * resulting MerkleSeq has size logarithmic in the size of the fully unblinded MerkleSeq.
  *
  * @param rootOrEmpty the root element or `None` if the sequence is empty
  * @tparam M the type of elements
  */
final case class MerkleSeq[+M <: VersionedMerkleTree[?]](
    rootOrEmpty: Option[MerkleTree[MerkleSeqElement[M]]]
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[MerkleSeq.type],
    hashOps: HashOps,
) extends PrettyPrinting
    with HasProtocolVersionedWrapper[
      MerkleSeq[VersionedMerkleTree[?]]
    ] {

  /** Obtain a representative protocol version for a [[MerkleSeqElement]] by casting ours.
    *
    * This is possible because currently there is a close connection between the versioning of these two structures.
    * Only use this in edge cases, where obtaining a representative for [[MerkleSeqElement]] is not possible without
    * making unsafe assumptions.
    *
    * WARNING: /!\ This will blow up if (when?) the versioning of the two structures diverges. /!\
    */
  private[data] lazy val tryMerkleSeqElementRepresentativeProtocolVersion
      : RepresentativeProtocolVersion[MerkleSeqElement.type] = {
    castRepresentativeProtocolVersion[MerkleSeqElement.type](MerkleSeqElement)
      .valueOr(e => throw new IllegalArgumentException(e))
  }

  lazy val unblindedElementsWithIndex: Seq[(M, MerklePathElement)] = rootOrEmpty match {
    case Some(root) =>
      root.unwrap match {
        case Right(unblindedRoot) => unblindedRoot.unblindedElements
        case Left(_) => Seq.empty
      }
    case None => Seq.empty
  }

  lazy val unblindedElements: Seq[M] = unblindedElementsWithIndex.map(_._1)

  lazy val blindedElements: Seq[RootHash] = rootOrEmpty match {
    case Some(root) => root.unwrap.fold(Seq(_), _.blindedElements)
    case None => Seq.empty
  }

  /** Converts this to a Seq.
    * The resulting seq may be shorter than the underlying fully unblinded seq,
    * because neighbouring blinded elements may be blinded into a single node.
    */
  lazy val toSeq: Seq[MerkleTree[M]] = rootOrEmpty match {
    case Some(t) => MerkleSeqElement.seqOf(t)
    case None => Seq.empty
  }

  def blindFully: MerkleSeq[M] = rootOrEmpty.fold(this)(root =>
    MerkleSeq(Some(root.blindFully))(representativeProtocolVersion, hashOps)
  )

  def isFullyBlinded: Boolean = rootOrEmpty.fold(true)(_.unwrap.isLeft)

  lazy val rootHashO: Option[RootHash] = rootOrEmpty.map(_.rootHash)

  private[data] def doBlind(
      optimizedBlindingPolicy: PartialFunction[RootHash, BlindingCommand]
  ): MerkleSeq[M] =
    rootOrEmpty match {
      case Some(root) =>
        optimizedBlindingPolicy(root.rootHash) match {
          case BlindSubtree =>
            MerkleSeq(Some(BlindedNode[MerkleSeqElement[M]](root.rootHash)))(
              representativeProtocolVersion,
              hashOps,
            )
          case RevealSubtree => this
          case RevealIfNeedBe =>
            val blindedRoot = root.withBlindedSubtrees(optimizedBlindingPolicy)
            MerkleSeq(Some(blindedRoot))(representativeProtocolVersion, hashOps)
        }
      case None => this
    }

  /** Blind everything in this MerkleSeq, except the leaf identified by the given path.
    * To ensure the path is valid, it should be obtained beforehand with a traversal
    * method such as [[unblindedElementsWithIndex]] and reversed with [[ViewPosition.reverse]].
    *
    * @param path the path from root to leaf
    * @param actionOnLeaf an action to transform the leaf once it is found
    * @throws java.lang.UnsupportedOperationException if the path does not lead to an unblinded leaf
    */
  def tryBlindAllButLeaf[A <: VersionedMerkleTree[A]](
      path: MerkleSeqIndexFromRoot,
      actionOnLeaf: M => A,
      // Ideally, we would have `actionOnLeaf: M => M`, as there is no need to change the type of the
      // leaf when blinding. Unfortunately, this becomes harder in practice: since M is covariant,
      // the type checker does not know the actual type at runtime and could still mishandle it.
  ): MerkleSeq[A] = {
    rootOrEmpty match {
      case Some(root) =>
        MerkleSeq(Some(root.tryUnwrap.tryBlindAllButLeaf(path, actionOnLeaf)))(
          representativeProtocolVersion,
          hashOps,
        )
      case None => throw new UnsupportedOperationException("Empty MerkleSeq")
    }
  }

  def toProtoV30: v30.MerkleSeq =
    v30.MerkleSeq(rootOrEmpty = rootOrEmpty.map(MerkleTree.toBlindableNodeV30))

  override def pretty: Pretty[MerkleSeq.this.type] = prettyOfClass(
    param("root hash", _.rootHashO, _.rootOrEmpty.exists(!_.isBlinded)),
    unnamedParamIfDefined(_.rootOrEmpty),
  )

  def mapM[A <: VersionedMerkleTree[A]](f: M => A): MerkleSeq[A] = {
    this.copy(rootOrEmpty = rootOrEmpty.map(_.unwrap.fold(BlindedNode(_), seq => seq.mapM(f))))(
      representativeProtocolVersion,
      hashOps,
    )
  }

  @transient override protected lazy val companionObj: MerkleSeq.type = MerkleSeq
}

object MerkleSeq
    extends HasProtocolVersionedWithContextCompanion[
      MerkleSeq[VersionedMerkleTree[?]],
      (
          HashOps,
          // This function is the deserializer for unblinded nodes
          ByteString => ParsingResult[MerkleTree[VersionedMerkleTree[?]]],
      ),
    ] {

  override def name: String = "MerkleSeq"

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.MerkleSeq)(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30.toByteString,
      )
    )

  private type Path = List[Direction]
  private def emptyPath: Path = List.empty[Direction]

  sealed trait MerkleSeqElement[+M <: VersionedMerkleTree[?]]
      extends MerkleTree[MerkleSeqElement[M]]
      with HasProtocolVersionedWrapper[MerkleSeqElement[VersionedMerkleTree[?]]]
      with Product
      with Serializable {
    @transient override protected lazy val companionObj: MerkleSeqElement.type = MerkleSeqElement

    lazy val unblindedElements: Seq[(M, MerklePathElement)] = computeUnblindedElements()

    // Doing this in a separate method, as Idea would otherwise complain about a covariant type parameter
    // being used in a contravariant position.
    private def computeUnblindedElements(): Seq[(M, MerklePathElement)] = {
      val builder = Seq.newBuilder[(M, MerklePathElement)]
      foreachUnblindedElement(emptyPath)((x, path) => builder.+=(x -> MerkleSeqIndex(path)))
      builder.result()
    }

    private[MerkleSeq] def foreachUnblindedElement(path: Path)(body: (M, Path) => Unit): Unit

    def blindedElements: Seq[RootHash] = {
      val builder = scala.collection.mutable.Seq.newBuilder[RootHash]
      foreachBlindedElement(builder.+=(_))
      builder.result()
    }.toSeq

    def toSeq: Seq[MerkleTree[M]]

    private[MerkleSeq] def foreachBlindedElement(body: RootHash => Unit): Unit

    // We repeat this here to enforce a more specific return type.
    override private[data] def withBlindedSubtrees(
        optimizedBlindingPolicy: PartialFunction[RootHash, BlindingCommand]
    ): MerkleSeqElement[M]

    def tryBlindAllButLeaf[A <: VersionedMerkleTree[A]](
        path: MerkleSeqIndexFromRoot,
        actionOnLeaf: M => A,
    ): MerkleSeqElement[A]

    def mapM[A <: VersionedMerkleTree[A]](f: M => A): MerkleSeqElement[A]

    def toProtoV30: v30.MerkleSeqElement
  }

  object Branch {
    def apply[M <: VersionedMerkleTree[_]](
        first: MerkleTree[MerkleSeqElement[M]],
        second: MerkleTree[MerkleSeqElement[M]],
        protocolVersion: ProtocolVersion,
    )(
        hashOps: HashOps
    ): Branch[M] = {
      new Branch[M](
        first,
        second,
        MerkleSeqElement.protocolVersionRepresentativeFor(protocolVersion),
      )(hashOps)
    }
  }

  private[data] final case class Branch[+M <: VersionedMerkleTree[_]](
      first: MerkleTree[MerkleSeqElement[M]],
      second: MerkleTree[MerkleSeqElement[M]],
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        MerkleSeqElement.type
      ],
  )(
      hashOps: HashOps
  ) extends MerkleTreeInnerNode[Branch[M]](hashOps)
      with MerkleSeqElement[M] {

    override def subtrees: Seq[MerkleTree[?]] = Seq(first, second)

    override def toSeq: Seq[MerkleTree[M]] =
      MerkleSeqElement.seqOf(first) ++ MerkleSeqElement.seqOf(second)

    override private[data] def withBlindedSubtrees(
        optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
    ): Branch[M] =
      Branch(
        first.doBlind(optimizedBlindingPolicy),
        second.doBlind(optimizedBlindingPolicy),
        representativeProtocolVersion,
      )(
        hashOps
      )

    override def tryBlindAllButLeaf[A <: VersionedMerkleTree[A]](
        path: MerkleSeqIndexFromRoot,
        actionOnLeaf: M => A,
    ): MerkleSeqElement[A] = {
      path.index match {
        case Direction.Left :: tailIndex =>
          Branch[A](
            first.tryUnwrap.tryBlindAllButLeaf(MerkleSeqIndexFromRoot(tailIndex), actionOnLeaf),
            BlindedNode[MerkleSeqElement[A]](second.rootHash),
            representativeProtocolVersion,
          )(hashOps)

        case Direction.Right :: tailIndex =>
          Branch[A](
            BlindedNode[MerkleSeqElement[A]](first.rootHash),
            second.tryUnwrap.tryBlindAllButLeaf(MerkleSeqIndexFromRoot(tailIndex), actionOnLeaf),
            representativeProtocolVersion,
          )(hashOps)

        case Nil =>
          throw new UnsupportedOperationException(
            "The path is invalid: path exhausted but leaf not reached"
          )
      }
    }

    override private[MerkleSeq] def foreachUnblindedElement(
        path: Path
    )(body: (M, Path) => Unit): Unit = {
      first.unwrap.foreach(_.foreachUnblindedElement(Direction.Left :: path)(body))
      second.unwrap.foreach(_.foreachUnblindedElement(Direction.Right :: path)(body))
    }

    override private[MerkleSeq] def foreachBlindedElement(body: RootHash => Unit): Unit = {
      first.unwrap.fold(body, _.foreachBlindedElement(body))
      second.unwrap.fold(body, _.foreachBlindedElement(body))
    }

    def toProtoV30: v30.MerkleSeqElement =
      v30.MerkleSeqElement(
        first = Some(MerkleTree.toBlindableNodeV30(first)),
        second = Some(MerkleTree.toBlindableNodeV30(second)),
        data = None,
      )

    override def pretty: Pretty[Branch.this.type] = prettyOfClass(
      param("first", _.first),
      param("second", _.second),
    )

    override def mapM[A <: VersionedMerkleTree[A]](
        f: M => A
    ): MerkleSeqElement[A] = {
      val newFirst: MerkleTree[MerkleSeqElement[A]] =
        first.unwrap.fold(h => BlindedNode(h), _.mapM(f))
      val newSecond = second.unwrap.fold(h => BlindedNode(h), _.mapM(f))
      Branch(newFirst, newSecond, representativeProtocolVersion)(hashOps)
    }
  }

  object Singleton {
    private[data] def apply[M <: VersionedMerkleTree[?]](
        data: MerkleTree[M],
        protocolVersion: ProtocolVersion,
    )(
        hashOps: HashOps
    ): Singleton[M] = {
      Singleton(data, MerkleSeqElement.protocolVersionRepresentativeFor(protocolVersion))(hashOps)
    }
  }

  private[data] final case class Singleton[+M <: VersionedMerkleTree[?]](
      data: MerkleTree[M],
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        MerkleSeqElement.type
      ],
  )(
      hashOps: HashOps
  ) extends MerkleTreeInnerNode[Singleton[M]](hashOps)
      with MerkleSeqElement[M] {
    // Singleton is a subtype of MerkleTree[_], because otherwise we would leak the size of the MerkleSeq in some cases
    // (e.g., if there is exactly one element).
    //
    // data is of type MerkleTree[_], because otherwise we would have to come up with a "surprising" implementation
    // of "withBlindedSubtrees" (i.e., blind the Singleton if the data is blinded).

    override def subtrees: Seq[MerkleTree[_]] = Seq(data)

    override def toSeq: Seq[MerkleTree[M]] = Seq(data)

    override private[data] def withBlindedSubtrees(
        optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
    ): Singleton[M] =
      Singleton[M](data.doBlind(optimizedBlindingPolicy), representativeProtocolVersion)(hashOps)

    override def tryBlindAllButLeaf[A <: VersionedMerkleTree[A]](
        path: MerkleSeqIndexFromRoot,
        actionOnLeaf: M => A,
    ): MerkleSeqElement[A] = {
      path.index match {
        case List() =>
          Singleton(
            actionOnLeaf(data.tryUnwrap),
            representativeProtocolVersion,
          )(hashOps)
        case other =>
          throw new UnsupportedOperationException(
            s"The path is invalid: reached a leaf but the path contains more steps ($other)"
          )
      }
    }

    override private[MerkleSeq] def foreachUnblindedElement(path: Path)(
        body: (M, Path) => Unit
    ): Unit =
      data.unwrap.foreach(body(_, path))

    override private[MerkleSeq] def foreachBlindedElement(body: RootHash => Unit): Unit =
      data.unwrap.fold(body, _ => ())

    def toProtoV30: v30.MerkleSeqElement =
      v30.MerkleSeqElement(
        first = None,
        second = None,
        data = Some(MerkleTree.toBlindableNodeV30(data)),
      )

    override def pretty: Pretty[Singleton.this.type] = prettyOfClass(unnamedParam(_.data))

    override def mapM[A <: VersionedMerkleTree[A]](
        f: M => A
    ): MerkleSeqElement[A] = {
      val newData: MerkleTree[A] = data.unwrap.fold(h => BlindedNode(h), f(_))
      Singleton(newData, representativeProtocolVersion)(hashOps)
    }
  }

  object MerkleSeqElement
      extends HasProtocolVersionedWithContextCompanion[
        MerkleSeqElement[VersionedMerkleTree[?]],
        // The function in the second part of the context is the deserializer for unblinded nodes
        (HashOps, ByteString => ParsingResult[MerkleTree[VersionedMerkleTree[?]]]),
      ] {
    override val name: String = "MerkleSeqElement"

    def seqOf[M <: VersionedMerkleTree[?]](
        elementTree: MerkleTree[MerkleSeqElement[M]]
    ): Seq[MerkleTree[M]] = elementTree.unwrap match {
      case Right(element) => element.toSeq
      case Left(rootHash) => Seq(BlindedNode(rootHash))
    }

    override def supportedProtoVersions: SupportedProtoVersions =
      SupportedProtoVersions(
        ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.MerkleSeqElement)(
          supportedProtoVersion(_)(fromProtoV30),
          _.toProtoV30.toByteString,
        )
      )

    private[MerkleSeq] def fromByteStringV30[M <: VersionedMerkleTree[?]](
        hashOps: HashOps,
        dataFromByteString: ByteString => ParsingResult[
          MerkleTree[M & HasProtocolVersionedWrapper[?]]
        ],
    )(bytes: ByteString): ParsingResult[MerkleSeqElement[M]] = {
      for {
        proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
        unwrapped <- proto.wrapper.data.toRight(
          ProtoDeserializationError.FieldNotSet(s"MerkleSeqElement: data")
        )
        merkleSeqElementP <- ProtoConverter.protoParser(v30.MerkleSeqElement.parseFrom)(unwrapped)
        merkleSeqElement <- fromProtoV30(
          (hashOps, dataFromByteString),
          merkleSeqElementP,
        )
      } yield merkleSeqElement
    }

    private[MerkleSeq] def fromProtoV30[M <: VersionedMerkleTree[?]](
        context: (
            HashOps,
            ByteString => ParsingResult[
              MerkleTree[M & HasProtocolVersionedWrapper[?]]
            ],
        ),
        merkleSeqElementP: v30.MerkleSeqElement,
    ): ParsingResult[MerkleSeqElement[M]] = {
      val (hashOps, dataFromByteString) = context
      val v30.MerkleSeqElement(maybeFirstP, maybeSecondP, maybeDataP) = merkleSeqElementP

      def branchChildFromMaybeProtoBlindableNode(
          maybeNodeP: Option[v30.BlindableNode]
      ): ParsingResult[Option[MerkleTree[MerkleSeqElement[M]]]] =
        maybeNodeP.traverse(nodeP =>
          MerkleTree.fromProtoOptionV30(Some(nodeP), fromByteStringV30(hashOps, dataFromByteString))
        )

      def singletonDataFromMaybeProtoBlindableNode(
          maybeDataP: Option[v30.BlindableNode]
      ): ParsingResult[Option[MerkleTree[M & HasProtocolVersionedWrapper[?]]]] =
        maybeDataP.traverse(dataP => MerkleTree.fromProtoOptionV30(Some(dataP), dataFromByteString))

      val rpv: RepresentativeProtocolVersion[MerkleSeqElement.type] =
        protocolVersionRepresentativeFor(ProtoVersion(1))

      for {
        maybeFirst <- branchChildFromMaybeProtoBlindableNode(maybeFirstP)
        maybeSecond <- branchChildFromMaybeProtoBlindableNode(maybeSecondP)
        maybeData <- singletonDataFromMaybeProtoBlindableNode(maybeDataP)

        merkleSeqElement <- (maybeFirst, maybeSecond, maybeData) match {
          case (Some(first), Some(second), None) =>
            Right(Branch(first, second, rpv)(hashOps))
          case (None, None, Some(data)) =>
            Right(Singleton[M](data, rpv)(hashOps))
          case (None, None, None) =>
            ProtoDeserializationError
              .OtherError(s"Unable to create MerkleSeqElement, as all fields are undefined.")
              .asLeft
          case (Some(_), Some(_), Some(_)) =>
            ProtoDeserializationError
              .OtherError(
                s"Unable to create MerkleSeqElement, as both the fields for a Branch and a Singleton are defined."
              )
              .asLeft
          case (_, _, _) =>
            // maybeFirst.isDefined != maybeSecond.isDefined
            def mkState: Option[_] => String = _.fold("undefined")(_ => "defined")

            ProtoDeserializationError
              .OtherError(
                s"Unable to create MerkleSeqElement, as first is ${mkState(maybeFirst)} and second is ${mkState(maybeSecond)}."
              )
              .asLeft
        }

      } yield merkleSeqElement
    }
  }

  def fromProtoV30[M <: VersionedMerkleTree[?]](
      context: (
          HashOps,
          ByteString => ParsingResult[
            MerkleTree[M & HasProtocolVersionedWrapper[?]]
          ],
      ),
      merkleSeqP: v30.MerkleSeq,
  ): ParsingResult[MerkleSeq[M]] = {
    val (hashOps, dataFromByteString) = context
    val v30.MerkleSeq(maybeRootP) = merkleSeqP
    val representativeProtocolVersion = protocolVersionRepresentativeFor(ProtoVersion(1))
    for {
      rootOrEmpty <- maybeRootP.traverse(_ =>
        MerkleTree.fromProtoOptionV30(
          maybeRootP,
          MerkleSeqElement.fromByteStringV30[M](hashOps, dataFromByteString),
        )
      )
    } yield MerkleSeq(rootOrEmpty)(representativeProtocolVersion, hashOps)
  }

  def fromSeq[M <: VersionedMerkleTree[?]](
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  )(elements: Seq[MerkleTree[M]]): MerkleSeq[M] = {
    val representativeProtocolVersion = protocolVersionRepresentativeFor(protocolVersion)
    val elemRepresentativeProtocolVersion =
      MerkleSeqElement.protocolVersionRepresentativeFor(protocolVersion)
    fromSeq(hashOps, representativeProtocolVersion, elemRepresentativeProtocolVersion)(elements)
  }

  def fromSeq[M <: VersionedMerkleTree[?]](
      hashOps: HashOps,
      representativeProtocolVersion: RepresentativeProtocolVersion[MerkleSeq.type],
      elemRepresentativeProtocolVersion: RepresentativeProtocolVersion[MerkleSeqElement.type],
  )(elements: Seq[MerkleTree[M]]): MerkleSeq[M] = {
    if (elements.isEmpty) {
      MerkleSeq.empty(representativeProtocolVersion, hashOps)
    } else {
      // elements is non-empty

      // Arrange elements in a balanced binary tree
      val merkleSeqElements = elements.iterator
        .map(
          Singleton(_, elemRepresentativeProtocolVersion)(hashOps)
        ) // Wrap elements in singletons
        .map { // Blind singletons, if the enclosed element is blinded
          case singleton @ Singleton(BlindedNode(_), _) => BlindedNode(singleton.rootHash)
          case singleton => singleton
        }

      val root = mkTree[MerkleTree[MerkleSeqElement[M]]](merkleSeqElements, elements.size) {
        (first, second) =>
          val branch = Branch(first, second, elemRepresentativeProtocolVersion)(hashOps)
          if (first.isBlinded && second.isBlinded) BlindedNode(branch.rootHash) else branch
      }

      MerkleSeq(Some(root))(representativeProtocolVersion, hashOps)
    }
  }

  def apply[M <: VersionedMerkleTree[?]](
      rootOrEmpty: Option[MerkleTree[MerkleSeqElement[M]]],
      protocolVersion: ProtocolVersion,
  )(hashOps: HashOps): MerkleSeq[M] = {
    MerkleSeq(rootOrEmpty)(protocolVersionRepresentativeFor(protocolVersion), hashOps)
  }

  /** Create an empty MerkleSeq */
  def empty[M <: VersionedMerkleTree[?]](
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  ): MerkleSeq[M] =
    empty(protocolVersionRepresentativeFor(protocolVersion), hashOps)

  def empty[M <: VersionedMerkleTree[?]](
      representativeProtocolVersion: RepresentativeProtocolVersion[MerkleSeq.type],
      hashOps: HashOps,
  ): MerkleSeq[M] =
    MerkleSeq(None)(representativeProtocolVersion, hashOps)

  /** Arranges a non-empty sequence of `elements` in a balanced binary tree.
    *
    * @param size The [[scala.collection.Iterator.length]] of `elements`.
    *             We take the size as a separate parameter because the implementation of the method relies on
    *             [[scala.collection.Iterator.grouped]] and computing the size of an iterator takes linear time.
    * @param combine The function to construct an inner node of the binary tree from two subtrees.
    */
  @tailrec
  private def mkTree[E](elements: Iterator[E], size: Int)(combine: (E, E) => E): E = {
    require(size > 0, "This method must be called with a positive size.")

    if (size == 1) {
      elements.next()
    } else {
      // size > 1

      val newElements = elements.grouped(2).map { // Create the next layer of the tree
        case Seq(first, second) => combine(first, second)
        case Seq(single) => single
        case group => throw new IllegalStateException(s"Unexpected group size: ${group.size}")
      }

      val newSize = size / 2 + size % 2 // half of size, rounded upwards
      mkTree(newElements, newSize)(combine)
    }
  }

  /** Computes the [[ViewPosition.MerkleSeqIndex]]es for all leaves in a [[MerkleSeq]] of the given size.
    * The returned indices are in sequence.
    */
  // takes O(size) runtime and memory due to sharing albeit there are O(size * log(size)) directions
  def indicesFromSeq(size: Int): Seq[MerkleSeqIndex] = {
    require(size >= 0, "A sequence cannot have negative size")

    if (size == 0) Seq.empty[MerkleSeqIndex]
    else {
      val tree = mkTree[Node](Iterator.fill[Node](size)(Leaf), size)(Inner)
      // enumerate all paths in the tree from left to right
      tree.addTo(emptyPath, List.empty[MerkleSeqIndex])
    }
  }

  // Helper classes for indicesFromSeq
  private trait Node extends Product with Serializable {

    /** Prefixes `subsequentIndices` with all paths from the leaves of this subtree to the root
      *
      * @param pathFromRoot The path from this node to the root.
      */
    def addTo(pathFromRoot: Path, subsequentIndices: List[MerkleSeqIndex]): List[MerkleSeqIndex]
  }
  private case object Leaf extends Node {
    override def addTo(
        pathFromRoot: Path,
        subsequentPaths: List[MerkleSeqIndex],
    ): List[MerkleSeqIndex] =
      MerkleSeqIndex(pathFromRoot) :: subsequentPaths
  }
  private final case class Inner(left: Node, right: Node) extends Node {
    override def addTo(
        pathFromRoot: Path,
        subsequentPaths: List[MerkleSeqIndex],
    ): List[MerkleSeqIndex] =
      left.addTo(
        Direction.Left :: pathFromRoot,
        right.addTo(Direction.Right :: pathFromRoot, subsequentPaths),
      )
  }
}
