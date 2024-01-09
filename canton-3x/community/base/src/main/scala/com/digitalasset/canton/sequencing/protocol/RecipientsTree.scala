// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.reducible.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member

/** A tree representation of the recipients for a batch.
  * Each member receiving the batch should see only subtrees of recipients from a node containing
  * the member. If a member is present in a subtree A and a sub-subtree of A then it should only see
  * the top-level subtree A.
  */
final case class RecipientsTree(
    recipientGroup: NonEmpty[Set[Recipient]],
    children: Seq[RecipientsTree],
) extends PrettyPrinting {

  override def pretty: Pretty[RecipientsTree] =
    prettyOfClass(
      param("recipient group", _.recipientGroup.toList),
      paramIfNonEmpty("children", _.children),
    )

  lazy val allRecipients: NonEmpty[Set[Recipient]] = {
    val tail: Set[Recipient] = children.flatMap(t => t.allRecipients).toSet
    recipientGroup ++ tail
  }

  def allPaths: NonEmpty[Seq[NonEmpty[Seq[NonEmpty[Set[Recipient]]]]]] =
    NonEmpty.from(children) match {
      case Some(childrenNE) =>
        childrenNE.flatMap { child =>
          child.allPaths.map(p => recipientGroup +: p)
        }
      case None => NonEmpty(Seq, NonEmpty(Seq, recipientGroup))
    }

  def forMember(
      member: Member,
      groupRecipients: Set[GroupRecipient],
  ): Seq[RecipientsTree] =
    if (
      recipientGroup
        .exists {
          case MemberRecipient(m) => member == m
          case g: GroupRecipient =>
            groupRecipients.contains(g)
        }
    ) {
      Seq(this)
    } else {
      children.flatMap(c => c.forMember(member, groupRecipients))
    }

  lazy val leafRecipients: NonEmpty[Set[Recipient]] = children match {
    case NonEmpty(cs) => cs.toNEF.reduceLeftTo(_.leafRecipients)(_ ++ _.leafRecipients)
    case _ => recipientGroup.map(m => m: Recipient)
  }

  def toProtoV0: v0.RecipientsTree = {
    val recipientsP = recipientGroup.toSeq.map(_.toProtoPrimitive).sorted
    val childrenP = children.map(_.toProtoV0)
    new v0.RecipientsTree(recipientsP, childrenP)
  }
}

object RecipientsTree {
  def ofRecipients(
      recipientGroup: NonEmpty[Set[Recipient]],
      children: Seq[RecipientsTree],
  ): RecipientsTree = RecipientsTree(recipientGroup, children)

  def ofMembers(
      recipientGroup: NonEmpty[Set[Member]],
      children: Seq[RecipientsTree],
  ): RecipientsTree = RecipientsTree(recipientGroup.map(MemberRecipient), children)

  def leaf(group: NonEmpty[Set[Member]]): RecipientsTree =
    RecipientsTree(group.map(MemberRecipient), Seq.empty)

  def recipientsLeaf(group: NonEmpty[Set[Recipient]]): RecipientsTree =
    RecipientsTree(group, Seq.empty)

  def fromProtoV0(
      treeProto: v0.RecipientsTree,
      supportGroupAddressing: Boolean,
  ): ParsingResult[RecipientsTree] = {
    for {
      members <- treeProto.recipients.traverse(str =>
        if (supportGroupAddressing)
          Recipient.fromProtoPrimitive(str, "RecipientsTreeProto.recipients")
        else Member.fromProtoPrimitive(str, "RecipientsTreeProto.recipients").map(MemberRecipient)
      )
      recipientsNonEmpty <- NonEmpty
        .from(members)
        .toRight(
          ProtoDeserializationError.ValueConversionError(
            "RecipientsTree.recipients",
            s"RecipientsTree.recipients must be non-empty",
          )
        )
      children = treeProto.children
      childTrees <- children.toList.traverse(fromProtoV0(_, supportGroupAddressing))
    } yield RecipientsTree(
      recipientsNonEmpty.toSet,
      childTrees,
    )
  }

}
