// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.ledger.client

import com.daml.ledger.api.v2.{admin => admin_proto}
import com.daml.ledger.api.v2.admin.{user_management_service => proto}
import com.daml.ledger.api.v2.admin.user_management_service.UserManagementServiceGrpc.UserManagementServiceStub
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{Party, UserId}

import scala.concurrent.{ExecutionContext, Future}

final class UserManagementClient(
    service: UserManagementServiceStub,
    getDefaultToken: () => Option[String] = () => None,
)(implicit
    ec: ExecutionContext
) {
  import UserManagementClient._

  def createUser(
      user: User,
      initialRights: Seq[UserRight] = List.empty,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[User] = {
    val request = proto.CreateUserRequest(
      Some(toProtoUser(user)),
      initialRights.view.map(toProtoRight).toList,
    )
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .createUser(request)
      .flatMap(res => fromOptionalProtoUser(res.user))
  }

  def getUser(userId: UserId, token: Option[String] = None, identityProviderId: String = "")(
      implicit traceContext: TraceContext
  ): Future[User] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .getUser(proto.GetUserRequest(userId, identityProviderId))
      .flatMap(res => fromOptionalProtoUser(res.user))

  def deleteUser(userId: UserId, token: Option[String] = None, identityProviderId: String = "")(
      implicit traceContext: TraceContext
  ): Future[Unit] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .deleteUser(proto.DeleteUserRequest(userId, identityProviderId))
      .map(_ => ())

  def listUsers(
      token: Option[String] = None,
      pageToken: String,
      pageSize: Int,
      identityProviderId: String = "",
  )(implicit traceContext: TraceContext): Future[(Seq[User], String)] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .listUsers(
        proto.ListUsersRequest(
          pageToken = pageToken,
          pageSize = pageSize,
          identityProviderId = identityProviderId,
        )
      )
      .map(res => res.users.view.map(fromProtoUser).toSeq -> res.nextPageToken)

  def grantUserRights(
      userId: UserId,
      rights: Seq[UserRight],
      token: Option[String] = None,
      identityProviderId: String = "",
  )(implicit traceContext: TraceContext): Future[Seq[UserRight]] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .grantUserRights(
        proto.GrantUserRightsRequest(userId, rights.map(toProtoRight), identityProviderId)
      )
      .map(_.newlyGrantedRights.view.collect(fromProtoRight.unlift).toSeq)

  def revokeUserRights(
      userId: UserId,
      rights: Seq[UserRight],
      token: Option[String] = None,
      identityProviderId: String = "",
  )(implicit traceContext: TraceContext): Future[Seq[UserRight]] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .revokeUserRights(
        proto.RevokeUserRightsRequest(userId, rights.map(toProtoRight), identityProviderId)
      )
      .map(_.newlyRevokedRights.view.collect(fromProtoRight.unlift).toSeq)

  /** List the rights of the given user. Unknown rights are ignored. */
  def listUserRights(userId: UserId, token: Option[String] = None, identityProviderId: String = "")(
      implicit traceContext: TraceContext
  ): Future[Seq[UserRight]] =
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .listUserRights(proto.ListUserRightsRequest(userId, identityProviderId))
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
      identityProviderId = IdentityProviderId(user.identityProviderId),
      metadata = user.metadata.fold(ObjectMeta.empty)(fromProtoMetadata),
      primaryPartyAuthentication = user.primaryPartyAuthentication,
    )

  private def fromProtoMetadata(
      metadata: com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
  ): ObjectMeta =
    ObjectMeta(
      resourceVersionO =
        Option.when(metadata.resourceVersion.nonEmpty)(metadata.resourceVersion).map(_.toLong),
      annotations = metadata.annotations,
    )

  private def toProtoUser(user: User): proto.User =
    proto.User(
      id = user.id,
      primaryParty = user.primaryParty.getOrElse(""),
      isDeactivated = user.isDeactivated,
      identityProviderId = user.identityProviderId.toRequestString,
      metadata = Some(toProtoObjectMeta(user.metadata)),
      primaryPartyAuthentication = user.primaryPartyAuthentication,
    )

  private def toProtoObjectMeta(meta: ObjectMeta): admin_proto.object_meta.ObjectMeta =
    admin_proto.object_meta.ObjectMeta(
      resourceVersion = meta.resourceVersionO.map(_.toString).getOrElse(""),
      annotations = meta.annotations,
    )

  private val toProtoRight: UserRight => proto.Right = {
    case UserRight.ParticipantAdmin =>
      proto.Right(proto.Right.Kind.ParticipantAdmin(proto.Right.ParticipantAdmin()))
    case UserRight.IdentityProviderAdmin =>
      proto.Right(proto.Right.Kind.IdentityProviderAdmin(proto.Right.IdentityProviderAdmin()))
    case UserRight.CanActAs(party) =>
      proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs(party)))
    case UserRight.CanReadAs(party) =>
      proto.Right(proto.Right.Kind.CanReadAs(proto.Right.CanReadAs(party)))
    case UserRight.CanReadAsAnyParty =>
      proto.Right(proto.Right.Kind.CanReadAsAnyParty(proto.Right.CanReadAsAnyParty()))
    case UserRight.CanExecuteAs(party) =>
      proto.Right(proto.Right.Kind.CanExecuteAs(proto.Right.CanExecuteAs(party)))
    case UserRight.CanExecuteAsAnyParty =>
      proto.Right(proto.Right.Kind.CanExecuteAsAnyParty(proto.Right.CanExecuteAsAnyParty()))
  }

  private val fromProtoRight: proto.Right => Option[UserRight] = {
    case proto.Right(_: proto.Right.Kind.ParticipantAdmin) =>
      Some(UserRight.ParticipantAdmin)
    case proto.Right(_: proto.Right.Kind.IdentityProviderAdmin) =>
      Some(UserRight.IdentityProviderAdmin)
    case proto.Right(proto.Right.Kind.CanActAs(x)) =>
      Some(UserRight.CanActAs(Ref.Party.assertFromString(x.party)))
    case proto.Right(proto.Right.Kind.CanReadAs(x)) =>
      Some(UserRight.CanReadAs(Ref.Party.assertFromString(x.party)))
    case proto.Right(proto.Right.Kind.CanReadAsAnyParty(_)) =>
      Some(UserRight.CanReadAsAnyParty)
    case proto.Right(proto.Right.Kind.CanExecuteAs(x)) =>
      Some(UserRight.CanExecuteAs(Ref.Party.assertFromString(x.party)))
    case proto.Right(proto.Right.Kind.CanExecuteAsAnyParty(_)) =>
      Some(UserRight.CanExecuteAsAnyParty)
    case proto.Right(proto.Right.Kind.Empty) =>
      None // The server sent a right of a kind that this client doesn't know about.
  }
}
