package com.daml.ledger.client.services.admin

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{ApplicationId, User, UserRight}
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.ledger.api.v1.admin.user_management_service.{GetUserRequest, Right}
import com.daml.ledger.api.v1.admin.user_management_service.UserManagementServiceGrpc.UserManagementServiceStub
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.services.admin.UserManagementClient.{fromApiRight, fromProtoUser, toApiRight}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{LedgerString, Party}

import scala.concurrent.{ExecutionContext, Future}

final class UserManagementClient
  (service: UserManagementServiceStub)
  (implicit ec: ExecutionContext)
{
  def createUser(user: User, initialRights: Seq[UserRight] = List.empty, token: Option[String] = None) = {
    val request = proto.CreateUserRequest(
      Some(UserManagementClient.toProtoUser(user)),
      initialRights.view.map(UserManagementClient.toApiRight).toList
    )
    LedgerClient
      .stub(service, token)
      .createUser(request)
      .map(UserManagementClient.fromProtoUser)
  }

  def getUser(userId: ApplicationId, token: Option[String] = None): Future[User] =
    LedgerClient
      .stub(service, token)
      .getUser(GetUserRequest(userId.toString))
      .map(UserManagementClient.fromProtoUser)

  /** Retrieve the User information for the user authenticated by the token(s) on the call . */
  def getAuthenticatedUser(token: Option[String] = None): Future[User] =
    LedgerClient
      .stub(service, token)
      .getUser(GetUserRequest())
      .map(UserManagementClient.fromProtoUser)

  def deleteUser(userId: ApplicationId, token: Option[String] = None): Future[Unit] =
    LedgerClient
      .stub(service, token)
      .deleteUser(proto.DeleteUserRequest(userId.toString))
      .map(_ => ())

  def listUsers(token: Option[String] = None): Future[Vector[User]] =
    LedgerClient
      .stub(service, token)
      .listUsers(proto.ListUsersRequest())
      .map(_.users.view.map(fromProtoUser).toVector)

  def grantUserRights(userId: ApplicationId, rights: Seq[UserRight], token: Option[String] = None): Future[Vector[UserRight]] =
    LedgerClient
      .stub(service, token)
      .grantUserRights(proto.GrantUserRightsRequest(userId.toString, rights.map(toApiRight)))
      .map(_.newlyGrantedRights.view.map(fromApiRight).toVector)

  def revokeUserRights(userId: ApplicationId, rights: Seq[UserRight], token: Option[String] = None): Future[Vector[UserRight]] =
    LedgerClient
      .stub(service, token)
      .revokeUserRights(proto.RevokeUserRightsRequest(userId.toString, rights.map(toApiRight)))
      .map(_.newlyRevokedRights.view.map(fromApiRight).toVector)

  def listUserRights(userId: ApplicationId, token: Option[String] = None): Future[Vector[UserRight]] =
    LedgerClient
      .stub(service, token)
      .listUserRights(proto.ListUserRightsRequest(userId.toString))
      .map(_.rights.view.map(fromApiRight).toVector)

  /** Retrieve the rights of the user authenticated by the token(s) on the call . */
  def listAuthenticatedUserRights(token: Option[String] = None): Future[Vector[UserRight]] =
    LedgerClient
      .stub(service, token)
      .listUserRights(proto.ListUserRightsRequest())
      .map(_.rights.view.map(fromApiRight).toVector)
}

object UserManagementClient {
  private def fromProtoUser(user: proto.User): User =
    User(
      ApplicationId(LedgerString.assertFromString(user.id)),
      Option.unless(user.primaryParty.isEmpty)(Party.assertFromString(user.primaryParty))
    )

  private def toProtoUser(user: User): proto.User =
    proto.User(user.id.toString, user.primaryParty.fold("")(_.toString))

  // FIXME: move to the right place in the codebase; and cleanup imports; and dedup with the code in 'ApiUserManagementService'
  val toApiRight: domain.UserRight => Right = {
    case domain.UserRight.ParticipantAdmin =>
      Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))
    case domain.UserRight.CanActAs(party) =>
      Right(Right.Kind.CanActAs(Right.CanActAs(party)))
    case domain.UserRight.CanReadAs(party) =>
      Right(Right.Kind.CanReadAs(Right.CanReadAs(party)))
  }

  // FIXME: move to the right place in the codebase; and cleanup imports; and dedup with the code in 'ApiUserManagementService'
  val fromApiRight: Right => domain.UserRight = {
    case Right(_: Right.Kind.ParticipantAdmin) => domain.UserRight.ParticipantAdmin
    case Right(Right.Kind.CanActAs(x)) =>
      domain.UserRight.CanActAs(Ref.Party.assertFromString(x.party))
    case Right(Right.Kind.CanReadAs(x)) =>
      domain.UserRight.CanReadAs(Ref.Party.assertFromString(x.party))
    case _ => throw new Exception // TODO FIXME validation
  }

}