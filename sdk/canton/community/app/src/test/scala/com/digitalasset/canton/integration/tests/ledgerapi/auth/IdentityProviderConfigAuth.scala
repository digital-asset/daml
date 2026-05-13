// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import cats.syntax.functor.*
import com.daml.ledger.api.v2.admin.identity_provider_config_service.{
  CreateIdentityProviderConfigRequest,
  DeleteIdentityProviderConfigRequest,
  GetIdentityProviderConfigRequest,
  IdentityProviderConfig,
  IdentityProviderConfigServiceGrpc,
}
import com.daml.ledger.api.v2.admin.party_management_service.{
  PartyManagementServiceGrpc,
  UpdatePartyIdentityProviderIdRequest,
}
import com.daml.ledger.api.v2.admin.user_management_service.{
  UpdateUserIdentityProviderIdRequest,
  UserManagementServiceGrpc,
}
import com.digitalasset.canton.integration.plugins.UseJWKSServer

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait IdentityProviderConfigAuth extends KeyPairs {
  this: ServiceCallAuthTests =>

  val jwksServer = new UseJWKSServer(jwks, altJwks)
  registerPlugin(jwksServer)
  lazy val jwksUrl: String = jwksServer.endpoint
  lazy val altJwksUrl: String = jwksServer.altEndpoint

  def idpStub(
      context: ServiceCallContext
  ): IdentityProviderConfigServiceGrpc.IdentityProviderConfigServiceStub =
    stub(IdentityProviderConfigServiceGrpc.stub(channel), context.token)

  def createConfig(
      context: ServiceCallContext,
      idpId: Option[String] = None,
      audience: Option[String] = None,
      jwksUrlOverride: Option[String] = None,
      issuer: Option[String] = None,
  )(implicit ec: ExecutionContext): Future[IdentityProviderConfig] = {
    val suffix = UUID.randomUUID().toString
    val config =
      IdentityProviderConfig(
        identityProviderId = idpId.getOrElse("idp-id-" + suffix),
        isDeactivated = false,
        issuer = issuer.getOrElse("issuer-" + suffix),
        // token must be signed with one of the private keys corresponding to the jwks provided by this url
        jwksUrl = jwksUrlOverride.getOrElse(jwksUrl),
        audience = audience.getOrElse(""),
      )
    createConfig(context, config)
  }

  private def createConfig(
      context: ServiceCallContext,
      config: IdentityProviderConfig,
  )(implicit ec: ExecutionContext): Future[IdentityProviderConfig] =
    idpStub(context)
      .createIdentityProviderConfig(CreateIdentityProviderConfigRequest(Some(config)))
      .map(_.identityProviderConfig)
      .flatMap {
        case Some(idp) => Future.successful(idp)
        case None => Future.failed(new RuntimeException("Failed to create IDP Config"))
      }

  def deleteConfig(
      context: ServiceCallContext,
      identityProviderId: String,
  )(implicit ec: ExecutionContext): Future[Unit] =
    idpStub(context)
      .deleteIdentityProviderConfig(
        DeleteIdentityProviderConfigRequest(identityProviderId = identityProviderId)
      )
      .void

  def getConfig(
      context: ServiceCallContext,
      identityProviderId: String,
  )(implicit ec: ExecutionContext): Future[IdentityProviderConfig] =
    idpStub(context)
      .getIdentityProviderConfig(GetIdentityProviderConfigRequest(identityProviderId))
      .map(_.identityProviderConfig)
      .flatMap {
        case Some(idp) => Future.successful(idp)
        case None => Future.failed(new RuntimeException("Failed to get IDP Config"))
      }

  def parkParties(
      context: ServiceCallContext,
      parties: Seq[String],
      identityProviderId: String,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val pmsvc = stub(PartyManagementServiceGrpc.stub(channel), context.token)
    Future
      .sequence(
        parties.map(party =>
          pmsvc.updatePartyIdentityProviderId(
            UpdatePartyIdentityProviderIdRequest(
              party = party,
              sourceIdentityProviderId = identityProviderId,
              targetIdentityProviderId = "",
            )
          )
        )
      )
      .void
  }

  def parkUsers(
      context: ServiceCallContext,
      users: Seq[String],
      identityProviderId: String,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val umsvc = stub(UserManagementServiceGrpc.stub(channel), context.token)
    Future
      .sequence(
        users.map(user =>
          umsvc.updateUserIdentityProviderId(
            UpdateUserIdentityProviderIdRequest(
              userId = user,
              sourceIdentityProviderId = identityProviderId,
              targetIdentityProviderId = "",
            )
          )
        )
      )
      .void
  }

}
