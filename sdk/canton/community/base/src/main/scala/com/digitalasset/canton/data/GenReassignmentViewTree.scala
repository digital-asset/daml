// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.{HashOps, Signature}
import com.digitalasset.canton.data.MerkleTree.{BlindSubtree, RevealIfNeedBe, RevealSubtree}
import com.digitalasset.canton.protocol.{ViewHash, v30}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}
import com.google.protobuf.ByteString

/** A reassignment request tree has two children:
  * The `commonData` for the mediator and the involved participants
  * and the `view` only for the involved participants.
  */
abstract class GenReassignmentViewTree[
    CommonData <: HasProtocolVersionedWrapper[CommonData] & HasCryptographicEvidence,
    View <: HasProtocolVersionedWrapper[View],
    Tree,
    MediatorMessage,
] protected (commonData: MerkleTreeLeaf[CommonData], participantData: MerkleTree[View])(
    hashOps: HashOps
) extends MerkleTreeInnerNode[Tree](hashOps) { this: Tree =>

  val viewPosition: ViewPosition =
    ViewPosition.root // Use a dummy value, as there is only one view.

  override def subtrees: Seq[MerkleTree[_]] = Seq(commonData, participantData)

  /*
  This method is visible because we need the non-deterministic serialization only when we encrypt the tree,
  but the message to the mediator is sent unencrypted.

  The versioning does not play well with this parametrized class so we define the serialization
  method explicitly.
   */
  private def toProtoVersioned(version: ProtocolVersion): VersionedMessage[ReassignmentViewTree] =
    VersionedMessage(toProtoV30.toByteString, 1)

  def toByteString(version: ProtocolVersion): ByteString = toProtoVersioned(version).toByteString

  // If you add new versions, take `version` into account in `toProtoVersioned` above
  def toProtoV30: v30.ReassignmentViewTree =
    v30.ReassignmentViewTree(
      commonData = commonData.tryUnwrap.toByteString,
      participantData = Some(MerkleTree.toBlindableNodeV30(participantData)),
    )

  def viewHash: ViewHash = ViewHash.fromRootHash(rootHash)

  /** Blinds the reassignment view tree such that the `view` is blinded and the `commonData` remains revealed. */
  def mediatorMessage(
      submittingParticipantSignature: Signature
  ): MediatorMessage = {
    val blinded = blind {
      case root if root eq this => RevealIfNeedBe
      case `commonData` => RevealSubtree
      case `participantData` => BlindSubtree
    }
    createMediatorMessage(blinded.tryUnwrap, submittingParticipantSignature)
  }

  /** Creates the mediator message from an appropriately blinded reassignment view tree. */
  protected[this] def createMediatorMessage(
      blindedTree: Tree,
      submittingParticipantSignature: Signature,
  ): MediatorMessage
}

object GenReassignmentViewTree {
  private[data] def fromProtoV30[CommonData, View, Tree](
      deserializeCommonData: ByteString => ParsingResult[CommonData],
      deserializeView: ByteString => ParsingResult[MerkleTree[View]],
  )(
      createTree: (CommonData, MerkleTree[View]) => Tree
  )(treeP: v30.ReassignmentViewTree): ParsingResult[Tree] = {
    val v30.ReassignmentViewTree(commonDataP, viewP) = treeP
    for {
      commonData <- deserializeCommonData(commonDataP)
        .leftMap(error => OtherError(s"reassignmentCommonData: $error"))
      view <- MerkleTree
        .fromProtoOptionV30(viewP, deserializeView(_))
        .leftMap(error => OtherError(s"reassignmentView: $error"))
    } yield createTree(commonData, view)
  }
}
