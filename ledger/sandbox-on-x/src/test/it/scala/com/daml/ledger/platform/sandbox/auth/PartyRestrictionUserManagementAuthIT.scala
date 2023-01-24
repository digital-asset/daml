// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.error.ErrorsAssertions
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.ledger.api.v1.admin.{user_management_service => uproto}
import com.daml.ledger.api.v1.admin.{party_management_service => pproto}
import com.daml.platform.sandbox.TestJwtVerifierLoader

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

  private def createUser(
      userId: String,
      serviceCallContext: ServiceCallContext,
      rights: Vector[uproto.Right] = Vector.empty,
  ): Future[uproto.CreateUserResponse] = {
    val user = uproto.User(
      id = userId,
      metadata = Some(ObjectMeta()),
      identityProviderId = serviceCallContext.identityProviderId,
    )
    val req = uproto.CreateUserRequest(Some(user), rights)
    stub(uproto.UserManagementServiceGrpc.stub(channel), serviceCallContext.token)
      .createUser(req)
  }

  private def allocateParty(serviceCallContext: ServiceCallContext, party: String) =
    stub(pproto.PartyManagementServiceGrpc.stub(channel), serviceCallContext.token)
      .allocateParty(
        pproto.AllocatePartyRequest(
          partyIdHint = party,
          identityProviderId = serviceCallContext.identityProviderId,
        )
      )

  it should "allow to grant permissions to parties which are allocated in the IDP" in {

    for {
      idpConfig <- createConfig(canReadAsAdmin)
      identityProviderConfig = idpConfig.identityProviderConfig.getOrElse(
        sys.error("Unable to create IdentityProviderConfig")
      )
      (_, idpAdmin) <- createUserByAdmin(
        userId = UUID.randomUUID().toString + "-alice-1",
        tokenIssuer = Some(identityProviderConfig.issuer),
        identityProviderId = identityProviderConfig.identityProviderId,
        rights = Vector(
          uproto.Right(
            uproto.Right.Kind.IdentityProviderAdmin(uproto.Right.IdentityProviderAdmin())
          ),
          uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs("some-party-2"))),
        ),
        primaryParty = "some-party-1",
        secret = Some(TestJwtVerifierLoader.secret1)
      )
      _ <- createUser(
        UUID.randomUUID().toString + "-alice-2",
        idpAdmin,
      )

      _ <- expectInvalidArgument(
        createUser(
          UUID.randomUUID().toString + "-alice-3",
          idpAdmin,
          Vector(
            uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs("some-party-1"))),
            uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs("some-party-2"))),
          ),
        )
      )

      _ <- allocateParty(idpAdmin, "some-party-1")
      _ <- allocateParty(idpAdmin, "some-party-2")

      _ <- createUser(
        UUID.randomUUID().toString + "-alice-4",
        idpAdmin,
        Vector(
          uproto.Right(uproto.Right.Kind.CanReadAs(uproto.Right.CanReadAs("some-party-1"))),
          uproto.Right(uproto.Right.Kind.CanActAs(uproto.Right.CanActAs("some-party-2"))),
        ),
      )
    } yield {
      assert(true)
    }

  }

}
