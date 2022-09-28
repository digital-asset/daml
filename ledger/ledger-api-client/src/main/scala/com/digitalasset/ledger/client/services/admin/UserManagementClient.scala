// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.admin

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{ObjectMeta, User, UserRight}
import com.daml.ledger.api.v1.admin.user_management_service.UserManagementServiceGrpc.UserManagementServiceStub
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.ledger.api.v1.{admin => admin_proto}
import com.daml.ledger.client.LedgerClient
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Party, UserId}

import scala.concurrent.{ExecutionContext, Future}

final class UserManagementClient(service: UserManagementServiceStub)(implicit
    ec: ExecutionContext
) {
  import UserManagementClient._

  def createUser(
      user: User,
      initialRights: Seq[UserRight] = List.empty,
      token: Option[String] = None,
  ): Future[User] = {
    val request = proto.CreateUserRequest(
      Some(UserManagementClient.toProtoUser(user)),
      initialRights.view.map(toProtoRight).toList,
    )
    LedgerClient
      .stub(service, token)
      .createUser(request)
      .map(res => fromProtoUser(res.user.get))
  }

  def getUser(userId: UserId, token: Option[String] = None): Future[User] =
    LedgerClient
      .stub(service, token)
      .getUser(proto.GetUserRequest(userId.toString))
      .map(res => fromProtoUser(res.user.get))

  /** Retrieve the User information for the user authenticated by the token(s) on the call . */
  def getAuthenticatedUser(token: Option[String] = None): Future[User] =
    LedgerClient
      .stub(service, token)
      .getUser(proto.GetUserRequest())
      .map(res => fromProtoUser(res.user.get))

  def deleteUser(userId: UserId, token: Option[String] = None): Future[Unit] =
    LedgerClient
      .stub(service, token)
      .deleteUser(proto.DeleteUserRequest(userId.toString))
      .map(_ => ())

  def listUsers(
      token: Option[String] = None,
      pageToken: String,
      pageSize: Int,
  ): Future[(Seq[User], String)] =
    LedgerClient
      .stub(service, token)
      .listUsers(proto.ListUsersRequest(pageToken = pageToken, pageSize = pageSize))
      .map(res => res.users.view.map(fromProtoUser).toSeq -> res.nextPageToken)

  def grantUserRights(
      userId: UserId,
      rights: Seq[UserRight],
      token: Option[String] = None,
  ): Future[Seq[UserRight]] =
    LedgerClient
      .stub(service, token)
      .grantUserRights(proto.GrantUserRightsRequest(userId.toString, rights.map(toProtoRight)))
      .map(_.newlyGrantedRights.view.collect(fromProtoRight.unlift).toSeq)

  def revokeUserRights(
      userId: UserId,
      rights: Seq[UserRight],
      token: Option[String] = None,
  ): Future[Seq[UserRight]] =
    LedgerClient
      .stub(service, token)
      .revokeUserRights(proto.RevokeUserRightsRequest(userId.toString, rights.map(toProtoRight)))
      .map(_.newlyRevokedRights.view.collect(fromProtoRight.unlift).toSeq)

  /** List the rights of the given user.
    * Unknown rights are ignored.
    */
  def listUserRights(userId: UserId, token: Option[String] = None): Future[Seq[UserRight]] =
    LedgerClient
      .stub(service, token)
      .listUserRights(proto.ListUserRightsRequest(userId.toString))
      .map(_.rights.view.collect(fromProtoRight.unlift).toSeq)

  /** Retrieve the rights of the user authenticated by the token(s) on the call .
    * Unknown rights are ignored.
    */
  def listAuthenticatedUserRights(token: Option[String] = None): Future[Seq[UserRight]] =
    LedgerClient
      .stub(service, token)
      .listUserRights(proto.ListUserRightsRequest())
      .map(_.rights.view.collect(fromProtoRight.unlift).toSeq)
}

object UserManagementClient {
  private def fromProtoUser(user: proto.User): User =
    User(
      id = Ref.UserId.assertFromString(user.id),
      primaryParty =
        Option.unless(user.primaryParty.isEmpty)(Party.assertFromString(user.primaryParty)),
      isDeactivated = user.isDeactivated,
      metadata = user.metadata.fold(domain.ObjectMeta.empty)(fromProtoMetadata),
    )

  private def fromProtoMetadata(
      metadata: com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
  ): domain.ObjectMeta = {
    domain.ObjectMeta(
      // TODO um-for-hub: Ledger-api-client shouldn't need to know how to parse resourceVersion. (ledger-api-domain probably shouldn't be used here)
      resourceVersionO =
        Option.when(metadata.resourceVersion.nonEmpty)(metadata.resourceVersion).map(_.toLong),
      annotations = metadata.annotations,
    )
  }

  private def toProtoUser(user: User): proto.User =
    proto.User(
      id = user.id.toString,
      primaryParty = user.primaryParty.fold("")(_.toString),
      isDeactivated = user.isDeactivated,
      metadata = Some(toProtoObjectMeta(user.metadata)),
    )

  private def toProtoObjectMeta(meta: ObjectMeta): admin_proto.object_meta.ObjectMeta =
    admin_proto.object_meta.ObjectMeta(
      // TODO um-for-hub: Ledger-api-client shouldn't need to know how to serialize resourceVersion. (ledger-api-domain probably shouldn't be used here)
      resourceVersion = meta.resourceVersionO.map(_.toString).getOrElse(""),
      annotations = meta.annotations,
    )

  private val toProtoRight: domain.UserRight => proto.Right = {
    case domain.UserRight.ParticipantAdmin =>
      proto.Right(proto.Right.Kind.ParticipantAdmin(proto.Right.ParticipantAdmin()))
    case domain.UserRight.CanActAs(party) =>
      proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs(party)))
    case domain.UserRight.CanReadAs(party) =>
      proto.Right(proto.Right.Kind.CanReadAs(proto.Right.CanReadAs(party)))
  }

  private val fromProtoRight: proto.Right => Option[domain.UserRight] = {
    case proto.Right(_: proto.Right.Kind.ParticipantAdmin) =>
      Some(domain.UserRight.ParticipantAdmin)
    case proto.Right(proto.Right.Kind.CanActAs(x)) =>
      // Note: assertFromString is OK here, as the server should deliver valid party identifiers.
      Some(domain.UserRight.CanActAs(Ref.Party.assertFromString(x.party)))
    case proto.Right(proto.Right.Kind.CanReadAs(x)) =>
      Some(domain.UserRight.CanReadAs(Ref.Party.assertFromString(x.party)))
    case proto.Right(proto.Right.Kind.Empty) =>
      None // The server sent a right of a kind that this client doesn't know about.
  }
}
