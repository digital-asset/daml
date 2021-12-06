package com.daml.platform.sandbox.auth
import com.daml.ledger.api.v1.admin.user_management_service.{GetUserRequest, User, UserManagementServiceGrpc}
import org.scalatest.Assertion

import scala.concurrent.Future

/** Tests covering the special behaviour of GetUser when not specifying a user-id. */
class GetUserWithNoUserIdIT extends ServiceCallAuthTests {
  override def serviceCallName: String = "UserManagementService#getUser(<no-user-id>)"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(UserManagementServiceGrpc.stub(channel), token).getUser(GetUserRequest())

  protected def expectUser(token: Option[String], expectedUser: User): Future[Assertion] =
    serviceCallWithToken(token).map(assertResult(expectedUser)(_))

  behavior of serviceCallName

  it should "deny unauthenticated access" in {
    expectUnauthenticated(serviceCallWithToken(None))
  }

  it should "deny access for a standard token referring to an unknown user" in {
    expectUnauthenticated(serviceCallWithToken(canReadAsUnknownUserStandardJWT))
  }

  it should "return the 'participant_admin' user when using its standard token" in {
    expectUser(canReadAsAdminStandardJWT, User("participant_admin", ""))
  }

  it should "return the user record corresponding to the decoded custom token with actAs rights" in {
    expectUser(canActAsRandomParty, User(randomUserId, randomParty))
  }

  it should "return the user record corresponding to the decoded custom token with readAs rights" in {
    expectUser(canReadAsRandomParty, User(randomUserId, randomParty))
  }
}
