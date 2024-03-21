// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.topology.*
import slick.jdbc.SetParameter

/** Unique path of a topology transaction
  *
  * The unique path allows us to distinguish any topology transaction by a unique code.
  * In addition, the code is chosen such that we can organise all transactions in a
  * binary tree structure such that changes to that tree are mostly local.
  *
  * The path is defined by
  *   namespace :: Rest
  * and Rest can be
  *   "nsd" :: content :: topologyElementId (for namespace delegations)
  *   "uid" :: uid.identifier :: "code" :: element-id
  *
  * (type, namespace, optional[identifier], optional[element-id])
  *
  * Therefore, if we onboard a new participant with many parties, all the changes
  * to the topology will be within the "subpath" defined by the namespace.
  */
trait UniquePath {
  def dbType: DomainTopologyTransactionType
  def namespace: Namespace
  def maybeUid: Option[UniqueIdentifier]
  def maybeElementId: Option[TopologyElementId]
  def toProtoPrimitive: String
}

final case class UniquePathNamespaceDelegation(namespace: Namespace, elementId: TopologyElementId)
    extends UniquePath {

  override def dbType: DomainTopologyTransactionType =
    DomainTopologyTransactionType.NamespaceDelegation
  override def maybeUid: Option[UniqueIdentifier] = None
  override def maybeElementId: Option[TopologyElementId] = Some(elementId)
  override lazy val toProtoPrimitive: String =
    Seq(namespace.fingerprint.unwrap, dbType.code, elementId.unwrap)
      .mkString(SafeSimpleString.delimiter)

}

final case class UniquePathSignedTopologyTransaction(
    uid: UniqueIdentifier,
    dbType: DomainTopologyTransactionType,
    elementId: TopologyElementId,
) extends UniquePath {

  override lazy val toProtoPrimitive: String =
    Seq(
      uid.namespace.fingerprint.unwrap,
      UniquePath.uniqueIdentifierCode,
      uid.id.unwrap,
      dbType.code,
      elementId.unwrap,
    )
      .mkString(SafeSimpleString.delimiter)

  override def maybeUid: Option[UniqueIdentifier] = Some(uid)
  override def maybeElementId: Option[TopologyElementId] = Some(elementId)

  override def namespace: Namespace = uid.namespace

}

object UniquePathSignedTopologyTransaction {

  def queryForUid(uniqueIdentifier: UniqueIdentifier): String =
    Seq(
      uniqueIdentifier.namespace.toProtoPrimitive,
      UniquePath.uniqueIdentifierCode,
      uniqueIdentifier.id.unwrap,
    )
      .mkString(SafeSimpleString.delimiter)

  def forUid(
      uid: UniqueIdentifier,
      typ: DomainTopologyTransactionType,
      elementId: TopologyElementId,
  ): UniquePathSignedTopologyTransaction =
    UniquePathSignedTopologyTransaction(uid, typ, elementId)

}

final case class UniquePathSignedDomainGovernanceTransaction(
    uid: UniqueIdentifier,
    dbType: DomainTopologyTransactionType,
) extends UniquePath {

  override lazy val toProtoPrimitive: String =
    Seq(
      uid.namespace.fingerprint.unwrap,
      UniquePath.uniqueIdentifierCode,
      uid.id.unwrap,
      dbType.code,
    )
      .mkString(SafeSimpleString.delimiter)

  override def maybeUid: Option[UniqueIdentifier] = Some(uid)
  override def maybeElementId: Option[TopologyElementId] = None

  override def namespace: Namespace = uid.namespace
}

object UniquePath {

  private[topology] val uniqueIdentifierCode = "uid"

  def queryForNamespace(namespace: Namespace): String = namespace.toProtoPrimitive

}

sealed case class DomainTopologyTransactionType private (dbInt: Int, code: String)

object DomainTopologyTransactionType {

  object ParticipantState extends DomainTopologyTransactionType(1, "pas")
  object NamespaceDelegation extends DomainTopologyTransactionType(2, "nsd")
  object IdentifierDelegation extends DomainTopologyTransactionType(3, "idd")
  object OwnerToKeyMapping extends DomainTopologyTransactionType(4, "okm")
  object PartyToParticipant extends DomainTopologyTransactionType(5, "ptp")
  object SignedLegalIdentityClaim extends DomainTopologyTransactionType(6, "lic")
  object PackageUse extends DomainTopologyTransactionType(7, "pau")
  object DomainParameters extends DomainTopologyTransactionType(8, "dmp")
  object MediatorDomainState extends DomainTopologyTransactionType(9, "mds")

  lazy val all = Seq(
    NamespaceDelegation,
    IdentifierDelegation,
    OwnerToKeyMapping,
    PartyToParticipant,
    SignedLegalIdentityClaim,
    PackageUse,
    ParticipantState,
    DomainParameters,
    MediatorDomainState,
  )

  implicit val setParameterDomainTopologyTransactionType
      : SetParameter[DomainTopologyTransactionType] = (v, pp) => pp.setInt(v.dbInt)

}
