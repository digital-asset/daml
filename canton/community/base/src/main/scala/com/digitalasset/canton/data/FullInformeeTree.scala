// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.sequencing.protocol.MediatorsOfDomain
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import monocle.Lens

import java.util.UUID

/** Wraps a [[GenTransactionTree]] that is also a full informee tree.
  */
// private constructor, because object invariants are checked by factory methods
final case class FullInformeeTree private (tree: GenTransactionTree)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[FullInformeeTree.type]
) extends HasProtocolVersionedWrapper[FullInformeeTree]
    with PrettyPrinting {

  def validated: Either[String, this.type] = for {
    _ <- FullInformeeTree.checkGlobalMetadata(tree)
    _ <- FullInformeeTree.checkViews(tree.rootViews)
  } yield this

  @transient override protected lazy val companionObj: FullInformeeTree.type = FullInformeeTree

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(tree.rootHash)

  private lazy val commonMetadata: CommonMetadata = checked(tree.commonMetadata.tryUnwrap)
  lazy val domainId: DomainId = commonMetadata.domainId
  lazy val mediator: MediatorsOfDomain = commonMetadata.mediator

  lazy val informeesAndThresholdByViewPosition: Map[ViewPosition, (Set[Informee], NonNegativeInt)] =
    FullInformeeTree.viewCommonDataByViewPosition(tree).map { case (position, viewCommonData) =>
      position -> ((viewCommonData.informees, viewCommonData.threshold))
    }

  lazy val allInformees: Set[LfPartyId] = FullInformeeTree
    .viewCommonDataByViewPosition(tree)
    .flatMap { case (_, viewCommonData) => viewCommonData.informees }
    .map(_.party)
    .toSet

  lazy val transactionUuid: UUID = checked(tree.commonMetadata.tryUnwrap).uuid

  lazy val confirmationPolicy: ConfirmationPolicy = checked(
    tree.commonMetadata.tryUnwrap
  ).confirmationPolicy

  lazy val submittingParticipant: ParticipantId =
    tree.submitterMetadata.tryUnwrap.submittingParticipant

  def toProtoV30: v30.FullInformeeTree =
    v30.FullInformeeTree(tree = Some(tree.toProtoV30))

  override def pretty: Pretty[FullInformeeTree] = prettyOfParam(_.tree)
}

object FullInformeeTree
    extends HasProtocolVersionedWithContextAndValidationCompanion[FullInformeeTree, HashOps] {
  override val name: String = "FullInformeeTree"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.FullInformeeTree)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  /** Creates a full informee tree from a [[GenTransactionTree]].
    * @throws FullInformeeTree$.InvalidInformeeTree if `tree` is not a valid full informee tree (i.e. the wrong nodes are blinded)
    */
  def tryCreate(tree: GenTransactionTree, protocolVersion: ProtocolVersion): FullInformeeTree =
    create(tree, protocolVersionRepresentativeFor(protocolVersion)).valueOr(err =>
      throw InvalidInformeeTree(err)
    )

  private[data] def create(
      tree: GenTransactionTree,
      representativeProtocolVersion: RepresentativeProtocolVersion[FullInformeeTree.type],
  ): Either[String, FullInformeeTree] =
    FullInformeeTree(tree)(representativeProtocolVersion).validated

  private[data] def create(
      tree: GenTransactionTree,
      protocolVersion: ProtocolVersion,
  ): Either[String, FullInformeeTree] =
    create(tree, protocolVersionRepresentativeFor(protocolVersion))

  private[data] def checkGlobalMetadata(tree: GenTransactionTree): Either[String, Unit] = {
    val errors = Seq.newBuilder[String]

    if (tree.submitterMetadata.unwrap.isLeft)
      errors += "The submitter metadata of a full informee tree must be unblinded."
    if (tree.commonMetadata.unwrap.isLeft)
      errors += "The common metadata of an informee tree must be unblinded."
    if (tree.participantMetadata.unwrap.isRight)
      errors += "The participant metadata of an informee tree must be blinded."

    val message = errors.result().mkString(" ")
    EitherUtil.condUnitE(message.isEmpty, message)
  }

  private[data] def checkViews(
      rootViews: MerkleSeq[TransactionView]
  ): Either[String, Unit] = {

    val errors = Seq.newBuilder[String]

    def checkIsEmpty(blinded: Seq[RootHash]): Unit =
      if (blinded.nonEmpty) {
        val hashes = blinded.map(_.toString).mkString(", ")
        errors += s"All views in a full informee tree must be unblinded. Found $hashes"
      }

    checkIsEmpty(rootViews.blindedElements)

    def go(wrappedViews: Seq[TransactionView]): Unit =
      wrappedViews.foreach { view =>
        checkIsEmpty(view.subviews.blindedElements)

        if (view.viewCommonData.unwrap.isLeft)
          errors += s"The view common data in a full informee tree must be unblinded. Found ${view.viewCommonData}."

        if (view.viewParticipantData.unwrap.isRight)
          errors += s"The view participant data in an informee tree must be blinded. Found ${view.viewParticipantData}."

        go(view.subviews.unblindedElements)
      }

    go(rootViews.unblindedElements)

    val message = errors.result().mkString("\n")
    EitherUtil.condUnitE(message.isEmpty, message)
  }

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

  /** Indicates an attempt to create an invalid [[FullInformeeTree]]. */
  final case class InvalidInformeeTree(message: String) extends RuntimeException(message) {}

  /** Lens for modifying the [[GenTransactionTree]] inside of a full informee tree.
    * It does not check if the new `tree` actually constitutes a valid full informee tree, therefore:
    * DO NOT USE IN PRODUCTION.
    */
  @VisibleForTesting
  lazy val genTransactionTreeUnsafe: Lens[FullInformeeTree, GenTransactionTree] =
    Lens[FullInformeeTree, GenTransactionTree](_.tree)(newTree =>
      fullInformeeTree => FullInformeeTree(newTree)(fullInformeeTree.representativeProtocolVersion)
    )

  def fromProtoV30(
      context: (HashOps, ProtocolVersion),
      protoInformeeTree: v30.FullInformeeTree,
  ): ParsingResult[FullInformeeTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoInformeeTree.tree)
      tree <- GenTransactionTree.fromProtoV30(context, protoTree)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      fullInformeeTree <- FullInformeeTree
        .create(tree, rpv)
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create full informee tree: $e")
        )
    } yield fullInformeeTree
}
