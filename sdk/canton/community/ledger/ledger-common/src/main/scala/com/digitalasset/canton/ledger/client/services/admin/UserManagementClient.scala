// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.admin

import com.daml.ledger.api.v2.admin.user_management_service.UserManagementServiceGrpc.UserManagementServiceStub
import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.ledger.api.v2.admin as admin_proto
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Party, UserId}
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.{ObjectMeta, User, UserRight}
import com.digitalasset.canton.ledger.client.LedgerClient

import scala.concurrent.{ExecutionContext, Future}

final class UserManagementClient(service: UserManagementServiceStub)(implicit
    ec: ExecutionContext
) {
  import UserManagementClient.*

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
      .flatMap(res => fromOptionalProtoUser(res.user))
  }

  def getUser(userId: UserId, token: Option[String] = None): Future[User] =
    LedgerClient
      .stub(service, token)
      .getUser(proto.GetUserRequest(userId.toString))
      .flatMap(res => fromOptionalProtoUser(res.user))

  /** Retrieve the User information for the user authenticated by the token(s) on the call . */
  def getAuthenticatedUser(token: Option[String] = None): Future[User] =
    LedgerClient
      .stub(service, token)
      .getUser(proto.GetUserRequest())
      .flatMap(res => fromOptionalProtoUser(res.user))

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

  private def fromOptionalProtoUser(userO: Option[proto.User]): Future[User] =
    userO.fold(Future.failed[User](new IllegalStateException("empty user")))(u =>
      Future.successful(fromProtoUser(u))
    )

  private def fromProtoUser(user: proto.User): User =
    User(
      id = Ref.UserId.assertFromString(user.id),
      primaryParty =
        Option.unless(user.primaryParty.isEmpty)(Party.assertFromString(user.primaryParty)),
      isDeactivated = user.isDeactivated,
      metadata = user.metadata.fold(domain.ObjectMeta.empty)(fromProtoMetadata),
    )

  private def fromProtoMetadata(
      metadata: com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
  ): domain.ObjectMeta = {
    domain.ObjectMeta(
      // It's unfortunate that a client is using the server-side domain ObjectMeta and has to know how to parse the resource version
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
      // It's unfortunate that a client is using the server-side domain ObjectMeta and has to know how to parse the resource version
      resourceVersion = meta.resourceVersionO.map(_.toString).getOrElse(""),
      annotations = meta.annotations,
    )

  private val toProtoRight: domain.UserRight => proto.Right = {
    case domain.UserRight.ParticipantAdmin =>
      proto.Right(proto.Right.Kind.ParticipantAdmin(proto.Right.ParticipantAdmin()))
    case domain.UserRight.IdentityProviderAdmin =>
      proto.Right(proto.Right.Kind.IdentityProviderAdmin(proto.Right.IdentityProviderAdmin()))
    case domain.UserRight.CanActAs(party) =>
      proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs(party)))
    case domain.UserRight.CanReadAs(party) =>
      proto.Right(proto.Right.Kind.CanReadAs(proto.Right.CanReadAs(party)))
    case domain.UserRight.CanReadAsAnyParty =>
      proto.Right(proto.Right.Kind.CanReadAsAnyParty(proto.Right.CanReadAsAnyParty()))
  }

  private val fromProtoRight: proto.Right => Option[domain.UserRight] = {
    case proto.Right(_: proto.Right.Kind.ParticipantAdmin) =>
      Some(domain.UserRight.ParticipantAdmin)
    case proto.Right(_: proto.Right.Kind.IdentityProviderAdmin) =>
      Some(domain.UserRight.IdentityProviderAdmin)
    case proto.Right(proto.Right.Kind.CanActAs(x)) =>
      // Note: assertFromString is OK here, as the server should deliver valid party identifiers.
      Some(domain.UserRight.CanActAs(Ref.Party.assertFromString(x.party)))
    case proto.Right(proto.Right.Kind.CanReadAs(x)) =>
      Some(domain.UserRight.CanReadAs(Ref.Party.assertFromString(x.party)))
    case proto.Right(proto.Right.Kind.CanReadAsAnyParty(_)) =>
      Some(domain.UserRight.CanReadAsAnyParty)
    case proto.Right(proto.Right.Kind.Empty) =>
      None // The server sent a right of a kind that this client doesn't know about.
  }
}
