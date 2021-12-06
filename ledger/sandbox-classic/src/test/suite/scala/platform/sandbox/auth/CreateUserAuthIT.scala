package com.daml.platform.sandbox.auth

import java.util.UUID

import com.daml.ledger.api.v1.admin.user_management_service._

import scala.concurrent.Future

final class CreateUserAuthIT extends AdminServiceCallAuthTests {

  override def serviceCallName: String = "UserManagementService#CreateUser"

  override def serviceCallWithToken(token: Option[String]): Future[Any] = {
    val userId = "fresh-user-" + UUID.randomUUID().toString
    val req = CreateUserRequest(Some(User(userId)))
    stub(UserManagementServiceGrpc.stub(channel), token).createUser(req)
  }

}
