package com.daml.ledger.client.services.admin

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{UserId, User, UserRight}
import com.daml.ledger.api.v1.admin.user_management_service.UserManagementServiceGrpc.UserManagementServiceStub
import com.daml.ledger.api.v1.admin.user_management_service.{GetUserRequest, Right}
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.ledger.client.LedgerClient
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party

import scala.concurrent.{ExecutionContext, Future}

final class UserManagementClient
  (service: UserManagementServiceStub)
  (implicit ec: ExecutionContext)
{
  import UserManagementClient._

  def createUser(user: User, initialRights: Seq[UserRight] = List.empty, token: Option[String] = None): Future[User] = {
    val request = proto.CreateUserRequest(
      Some(UserManagementClient.toProtoUser(user)),
      initialRights.view.map(toProtoRight).toList
    )
    LedgerClient
      .stub(service, token)
      .createUser(request)
      .map(fromProtoUser)
  }

  def getUser(userId: UserId, token: Option[String] = None): Future[User] =
    LedgerClient
      .stub(service, token)
      .getUser(GetUserRequest(userId.toString))
      .map(fromProtoUser)

  /** Retrieve the User information for the user authenticated by the token(s) on the call . */
  def getAuthenticatedUser(token: Option[String] = None): Future[User] =
    LedgerClient
      .stub(service, token)
      .getUser(GetUserRequest())
      .map(fromProtoUser)

  def deleteUser(userId: UserId, token: Option[String] = None): Future[Unit] =
    LedgerClient
      .stub(service, token)
      .deleteUser(proto.DeleteUserRequest(userId.toString))
      .map(_ => ())

  def listUsers(token: Option[String] = None): Future[Vector[User]] =
    LedgerClient
      .stub(service, token)
      .listUsers(proto.ListUsersRequest())
      .map(_.users.view.map(fromProtoUser).toVector)

  def grantUserRights(userId: UserId, rights: Seq[UserRight], token: Option[String] = None): Future[Vector[UserRight]] =
    LedgerClient
      .stub(service, token)
      .grantUserRights(proto.GrantUserRightsRequest(userId.toString, rights.map(toProtoRight)))
      .map(_.newlyGrantedRights.view.collect(fromProtoRight.unlift).toVector)

  def revokeUserRights(userId: UserId, rights: Seq[UserRight], token: Option[String] = None): Future[Vector[UserRight]] =
    LedgerClient
      .stub(service, token)
      .revokeUserRights(proto.RevokeUserRightsRequest(userId.toString, rights.map(toProtoRight)))
      .map(_.newlyRevokedRights.view.collect(fromProtoRight.unlift).toVector)

  /** List the rights of the given user.
   * Unknown rights are ignored.
   */
  def listUserRights(userId: UserId, token: Option[String] = None): Future[Vector[UserRight]] =
    LedgerClient
      .stub(service, token)
      .listUserRights(proto.ListUserRightsRequest(userId.toString))
      .map(_.rights.view.collect(fromProtoRight.unlift).toVector)

  /** Retrieve the rights of the user authenticated by the token(s) on the call .
   * Unknown rights are ignored.
   */
  def listAuthenticatedUserRights(token: Option[String] = None): Future[Vector[UserRight]] =
    LedgerClient
      .stub(service, token)
      .listUserRights(proto.ListUserRightsRequest())
      .map(_.rights.view.collect(fromProtoRight.unlift).toVector)
}

object UserManagementClient {
  private def fromProtoUser(user: proto.User): User =
    User(
      UserId(Ref.UserId.assertFromString(user.id)),
      Option.unless(user.primaryParty.isEmpty)(Party.assertFromString(user.primaryParty))
    )

  private def toProtoUser(user: User): proto.User =
    proto.User(user.id.toString, user.primaryParty.fold("")(_.toString))

  private val toProtoRight: domain.UserRight => Right = {
    case domain.UserRight.ParticipantAdmin =>
      Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))
    case domain.UserRight.CanActAs(party) =>
      Right(Right.Kind.CanActAs(Right.CanActAs(party)))
    case domain.UserRight.CanReadAs(party) =>
      Right(Right.Kind.CanReadAs(Right.CanReadAs(party)))
  }

  private val fromProtoRight: Right => Option[domain.UserRight] = {
    case Right(_: Right.Kind.ParticipantAdmin) => Some(domain.UserRight.ParticipantAdmin)
    case Right(Right.Kind.CanActAs(x)) =>
      // Note: assertFromString is OK here, as the server should deliver valid party identifiers.
      Some(domain.UserRight.CanActAs(Ref.Party.assertFromString(x.party)))
    case Right(Right.Kind.CanReadAs(x)) =>
      Some(domain.UserRight.CanReadAs(Ref.Party.assertFromString(x.party)))
    case Right(Right.Kind.Empty) =>
      None // The server sent a right of a kind that this client doesn't know about.
  }
}
