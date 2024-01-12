// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import monocle.Lens

/** Encapsulates a [[GenTransactionTree]] that is also an informee tree.
  */
// private constructor, because object invariants are checked by factory methods
final case class InformeeTree private (tree: GenTransactionTree)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[InformeeTree.type]
) extends HasProtocolVersionedWrapper[InformeeTree] {

  def validated: Either[String, this.type] = for {
    _ <- InformeeTree.checkGlobalMetadata(tree)
    _ <- InformeeTree.checkViews(tree.rootViews, assertFull = false)
  } yield this

  @transient override protected lazy val companionObj: InformeeTree.type = InformeeTree

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(tree.rootHash)

  lazy val informeesByViewHash: Map[ViewHash, Set[Informee]] =
    InformeeTree.viewCommonDataByViewHash(tree).map { case (hash, viewCommonData) =>
      hash -> viewCommonData.informees
    }

  private lazy val commonMetadata = checked(tree.commonMetadata.tryUnwrap)

  def domainId: DomainId = commonMetadata.domainId

  def mediator: MediatorRef = commonMetadata.mediator

  def toProtoV0: v0.InformeeTree = v0.InformeeTree(tree = Some(tree.toProtoV0))

  def toProtoV1: v1.InformeeTree = v1.InformeeTree(tree = Some(tree.toProtoV1))
}

object InformeeTree
    extends HasProtocolVersionedWithContextAndValidationCompanion[InformeeTree, HashOps] {
  override val name: String = "InformeeTree"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.InformeeTree)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.InformeeTree)(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  /** Creates an informee tree from a [[GenTransactionTree]].
    * @throws InformeeTree$.InvalidInformeeTree if `tree` is not a valid informee tree (i.e. the wrong nodes are blinded)
    */
  def tryCreate(
      tree: GenTransactionTree,
      protocolVersion: ProtocolVersion,
  ): InformeeTree = create(tree, protocolVersionRepresentativeFor(protocolVersion)).valueOr(err =>
    throw InvalidInformeeTree(err)
  )

  /** Creates an [[InformeeTree]] from a [[GenTransactionTree]].
    * Yields `Left(...)` if `tree` is not a valid informee tree (i.e. the wrong nodes are blinded)
    */
  private[data] def create(
      tree: GenTransactionTree,
      representativeProtocolVersion: RepresentativeProtocolVersion[InformeeTree.type],
  ): Either[String, InformeeTree] = InformeeTree(tree)(representativeProtocolVersion).validated

  /** Creates an [[InformeeTree]] from a [[GenTransactionTree]].
    * Yields `Left(...)` if `tree` is not a valid informee tree (i.e. the wrong nodes are blinded)
    */
  private[data] def create(
      tree: GenTransactionTree,
      protocolVersion: ProtocolVersion,
  ): Either[String, InformeeTree] = create(tree, protocolVersionRepresentativeFor(protocolVersion))

  private[data] def checkGlobalMetadata(tree: GenTransactionTree): Either[String, Unit] = {
    val errors = Seq.newBuilder[String]

    if (tree.submitterMetadata.unwrap.isRight)
      errors += "The submitter metadata of an informee tree must be blinded."
    if (tree.commonMetadata.unwrap.isLeft)
      errors += "The common metadata of an informee tree must be unblinded."
    if (tree.participantMetadata.unwrap.isRight)
      errors += "The participant metadata of an informee tree must be blinded."

    val message = errors.result().mkString(" ")
    EitherUtil.condUnitE(message.isEmpty, message)
  }

  private[data] def checkViews(
      rootViews: MerkleSeq[TransactionView],
      assertFull: Boolean,
  ): Either[String, Unit] = {

    val errors = Seq.newBuilder[String]

    def checkIsEmpty(blinded: Seq[RootHash]): Unit =
      if (assertFull && blinded.nonEmpty) {
        val hashes = blinded.map(_.toString).mkString(", ")
        errors += s"All views in a full informee tree must be unblinded. Found $hashes"
      }

    checkIsEmpty(rootViews.blindedElements)

    def go(wrappedViews: Seq[TransactionView]): Unit =
      wrappedViews.foreach { view =>
        checkIsEmpty(view.subviews.blindedElements)

        if (assertFull && view.viewCommonData.unwrap.isLeft)
          errors += s"The view common data in a full informee tree must be unblinded. Found ${view.viewCommonData}."

        if (view.viewParticipantData.unwrap.isRight)
          errors += s"The view participant data in an informee tree must be blinded. Found ${view.viewParticipantData}."

        go(view.subviews.unblindedElements)
      }

    go(rootViews.unblindedElements)

    val message = errors.result().mkString("\n")
    EitherUtil.condUnitE(message.isEmpty, message)
  }

  /** Lens for modifying the [[GenTransactionTree]] inside of an informee tree.
    * It does not check if the new `tree` actually constitutes a valid informee tree, therefore:
    * DO NOT USE IN PRODUCTION.
    */
  @VisibleForTesting
  def genTransactionTreeUnsafe: Lens[InformeeTree, GenTransactionTree] =
    Lens[InformeeTree, GenTransactionTree](_.tree)(newTree =>
      oldInformeeTree => InformeeTree(newTree)(oldInformeeTree.representativeProtocolVersion)
    )

  private[data] def viewCommonDataByViewHash(
      tree: GenTransactionTree
  ): Map[ViewHash, ViewCommonData] =
    tree.rootViews.unblindedElements
      .flatMap(_.flatten)
      .map(view => view.viewHash -> view.viewCommonData.unwrap)
      .collect { case (hash, Right(viewCommonData)) => hash -> viewCommonData }
      .toMap

  private[data] def viewCommonDataByViewPosition(
      tree: GenTransactionTree
  ): Map[ViewPosition, ViewCommonData] =
    tree.rootViews.unblindedElementsWithIndex
      .flatMap { case (view, index) =>
        view.allSubviewsWithPosition(ViewPosition(List(index))).map { case (subview, position) =>
          position -> subview.viewCommonData.unwrap
        }
      }
      .collect { case (position, Right(viewCommonData)) => position -> viewCommonData }
      .toMap

  /** Indicates an attempt to create an invalid [[InformeeTree]] or [[FullInformeeTree]]. */
  final case class InvalidInformeeTree(message: String) extends RuntimeException(message) {}

  def fromProtoV0(
      context: (HashOps, ProtocolVersion),
      protoInformeeTree: v0.InformeeTree,
  ): ParsingResult[InformeeTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoInformeeTree.tree)
      tree <- GenTransactionTree.fromProtoV0(context, protoTree)
      informeeTree <- InformeeTree
        .create(tree, protocolVersionRepresentativeFor(ProtoVersion(0)))
        .leftMap(e => ProtoDeserializationError.OtherError(s"Unable to create informee tree: $e"))
    } yield informeeTree

  def fromProtoV1(
      context: (HashOps, ProtocolVersion),
      protoInformeeTree: v1.InformeeTree,
  ): ParsingResult[InformeeTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoInformeeTree.tree)
      tree <- GenTransactionTree.fromProtoV1(context, protoTree)
      informeeTree <- InformeeTree
        .create(tree, protocolVersionRepresentativeFor(ProtoVersion(1)))
        .leftMap(e => ProtoDeserializationError.OtherError(s"Unable to create informee tree: $e"))
    } yield informeeTree
}
