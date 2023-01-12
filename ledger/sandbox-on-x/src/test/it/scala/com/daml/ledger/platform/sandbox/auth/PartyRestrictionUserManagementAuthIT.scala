// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.error.ErrorsAssertions
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.user_management_service.Right
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.ledger.api.v1.admin.{user_management_service => proto}

import java.util.UUID
import scala.concurrent.Future

final class PartyRestrictionUserManagementAuthIT
    extends ServiceCallAuthTests
    with IdentityProviderConfigAuth
    with ErrorsAssertions {

  private val UserManagementCacheExpiryInSeconds = 1

  override def config: Config = super.config.copy(
    participants = singleParticipant(
      ApiServerConfig.copy(
        userManagement = ApiServerConfig.userManagement
          .copy(
            cacheExpiryAfterWriteInSeconds = UserManagementCacheExpiryInSeconds
          )
      )
    )
  )

  override def serviceCallName: String = ""

  override protected def serviceCall(context: ServiceCallContext): Future[Any] = ???

  def createUser(
      userId: String,
      serviceCallContext: ServiceCallContext,
      rights: Vector[proto.Right] = Vector.empty,
      primaryParty: String = "",
  ): Future[proto.CreateUserResponse] = {
    val user = proto.User(
      id = userId,
      metadata = Some(ObjectMeta()),
      identityProviderId = serviceCallContext.identityProviderId,
      primaryParty = primaryParty,
    )
    val req = proto.CreateUserRequest(Some(user), rights)
    stub(proto.UserManagementServiceGrpc.stub(channel), serviceCallContext.token)
      .createUser(req)
  }

  it should "lalala" in {

    for {
      idpConfig <- createConfig(canReadAsAdmin)
      identityProviderConfig = idpConfig.identityProviderConfig.getOrElse(
        sys.error("Unable to create IdentityProviderConfig")
      )
      (_, alice) <- createUserByAdmin(
        userId = UUID.randomUUID().toString + "-alice-1",
        tokenIssuer = Some(identityProviderConfig.issuer),
        identityProviderId = identityProviderConfig.identityProviderId,
        rights = Vector(
          Right(Right.Kind.IdentityProviderAdmin(Right.IdentityProviderAdmin())),
          Right(Right.Kind.CanReadAs(Right.CanReadAs("some-party-2"))),
        ),
        primaryParty = "some-party-1",
      )

      _ <- createUser(
        UUID.randomUUID().toString + "-alice-2",
        alice,
        Vector(),
        // Vector(
        //  Right(Right.Kind.CanReadAs(Right.CanReadAs("some-party-2")))
        // ),
        "",
      )

    } yield {
      assert(true)
    }

  }

}
