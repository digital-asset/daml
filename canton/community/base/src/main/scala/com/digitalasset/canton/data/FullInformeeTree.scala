// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.InformeeTree.InvalidInformeeTree
import com.digitalasset.canton.data.MerkleTree.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
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
    _ <- InformeeTree.checkGlobalMetadata(tree)
    _ <- InformeeTree.checkViews(tree.rootViews, assertFull = true)
  } yield this

  @transient override protected lazy val companionObj: FullInformeeTree.type = FullInformeeTree

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(tree.rootHash)

  private lazy val commonMetadata: CommonMetadata = checked(tree.commonMetadata.tryUnwrap)
  lazy val domainId: DomainId = commonMetadata.domainId
  lazy val mediator: MediatorRef = commonMetadata.mediator

  /** Yields the informee tree unblinded for a defined set of parties.
    * If a view common data is already blinded, then it remains blinded even if one of the given parties is a stakeholder.
    */
  def informeeTreeUnblindedFor(
      parties: collection.Set[LfPartyId],
      protocolVersion: ProtocolVersion,
  ): InformeeTree = {
    val rawResult = tree
      .blind({
        case _: GenTransactionTree => RevealIfNeedBe
        case _: SubmitterMetadata => BlindSubtree
        case _: CommonMetadata => RevealSubtree
        case _: ParticipantMetadata => BlindSubtree
        case _: TransactionView => RevealIfNeedBe
        case v: ViewCommonData =>
          if (v.informees.map(_.party).intersect(parties).nonEmpty)
            RevealSubtree
          else
            BlindSubtree
        case _: ViewParticipantData => BlindSubtree
      })
      .tryUnwrap
    InformeeTree.tryCreate(rawResult, protocolVersion)
  }

  lazy val informeesAndThresholdByViewHash: Map[ViewHash, (Set[Informee], NonNegativeInt)] =
    InformeeTree.viewCommonDataByViewHash(tree).map { case (hash, viewCommonData) =>
      hash -> ((viewCommonData.informees, viewCommonData.threshold))
    }

  lazy val informeesAndThresholdByViewPosition: Map[ViewPosition, (Set[Informee], NonNegativeInt)] =
    InformeeTree.viewCommonDataByViewPosition(tree).map { case (position, viewCommonData) =>
      position -> ((viewCommonData.informees, viewCommonData.threshold))
    }

  lazy val allInformees: Set[LfPartyId] = InformeeTree
    .viewCommonDataByViewPosition(tree)
    .flatMap { case (_, viewCommonData) => viewCommonData.informees }
    .map(_.party)
    .toSet

  lazy val transactionUuid: UUID = checked(tree.commonMetadata.tryUnwrap).uuid

  lazy val confirmationPolicy: ConfirmationPolicy = checked(
    tree.commonMetadata.tryUnwrap
  ).confirmationPolicy

  def toProtoV0: v0.FullInformeeTree =
    v0.FullInformeeTree(tree = Some(tree.toProtoV0))

  def toProtoV1: v1.FullInformeeTree =
    v1.FullInformeeTree(tree = Some(tree.toProtoV1))

  override def pretty: Pretty[FullInformeeTree] = prettyOfParam(_.tree)
}

object FullInformeeTree
    extends HasProtocolVersionedWithContextCompanion[FullInformeeTree, HashOps] {
  override val name: String = "FullInformeeTree"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.FullInformeeTree)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.FullInformeeTree)(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  /** Creates a full informee tree from a [[GenTransactionTree]].
    * @throws InformeeTree$.InvalidInformeeTree if `tree` is not a valid informee tree (i.e. the wrong nodes are blinded)
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

  /** Lens for modifying the [[GenTransactionTree]] inside of a full informee tree.
    * It does not check if the new `tree` actually constitutes a valid full informee tree, therefore:
    * DO NOT USE IN PRODUCTION.
    */
  @VisibleForTesting
  lazy val genTransactionTreeUnsafe: Lens[FullInformeeTree, GenTransactionTree] =
    Lens[FullInformeeTree, GenTransactionTree](_.tree)(newTree =>
      fullInformeeTree => FullInformeeTree(newTree)(fullInformeeTree.representativeProtocolVersion)
    )

  def fromProtoV0(
      hashOps: HashOps,
      protoInformeeTree: v0.FullInformeeTree,
  ): ParsingResult[FullInformeeTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoInformeeTree.tree)
      tree <- GenTransactionTree.fromProtoV0(hashOps, protoTree)
      fullInformeeTree <- FullInformeeTree
        .create(tree, protocolVersionRepresentativeFor(ProtoVersion(0)))
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create full informee tree: $e")
        )
    } yield fullInformeeTree

  def fromProtoV1(
      hashOps: HashOps,
      protoInformeeTree: v1.FullInformeeTree,
  ): ParsingResult[FullInformeeTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoInformeeTree.tree)
      tree <- GenTransactionTree.fromProtoV1(hashOps, protoTree)
      fullInformeeTree <- FullInformeeTree
        .create(tree, protocolVersionRepresentativeFor(ProtoVersion(1)))
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create full informee tree: $e")
        )
    } yield fullInformeeTree
}
