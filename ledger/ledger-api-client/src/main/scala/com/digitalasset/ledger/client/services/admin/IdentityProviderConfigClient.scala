// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.admin

import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId, JwksUrl}
import com.daml.ledger.api.v1.admin.identity_provider_config_service.IdentityProviderConfigServiceGrpc.IdentityProviderConfigServiceStub
import com.daml.ledger.api.v1.admin.{identity_provider_config_service => proto}
import com.daml.ledger.client.LedgerClient
import com.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask

import scala.concurrent.{ExecutionContext, Future}

final class IdentityProviderConfigClient(service: IdentityProviderConfigServiceStub)(implicit
    ec: ExecutionContext
) {

  import IdentityProviderConfigClient._
  def createIdentityProviderConfig(
      config: IdentityProviderConfig,
      token: Option[String],
  ): Future[IdentityProviderConfig] = {
    val request = proto.CreateIdentityProviderConfigRequest(
      Some(IdentityProviderConfigClient.toProtoConfig(config))
    )
    LedgerClient
      .stub(service, token)
      .createIdentityProviderConfig(request)
      .map(res => fromProtoConfig(res.getIdentityProviderConfig))
  }

  def getIdentityProviderConfig(
      identityProviderId: IdentityProviderId.Id,
      token: Option[String],
  ): Future[IdentityProviderConfig] = {
    val request = proto.GetIdentityProviderConfigRequest(identityProviderId.toRequestString)
    LedgerClient
      .stub(service, token)
      .getIdentityProviderConfig(request)
      .map(res => fromProtoConfig(res.getIdentityProviderConfig))
  }

  def updateIdentityProviderConfig(
      config: IdentityProviderConfig,
      updateMask: FieldMask,
      token: Option[String],
  ): Future[IdentityProviderConfig] = {
    val request = proto.UpdateIdentityProviderConfigRequest(
      Some(IdentityProviderConfigClient.toProtoConfig(config)),
      Some(updateMask),
    )
    LedgerClient
      .stub(service, token)
      .updateIdentityProviderConfig(request)
      .map(res => fromProtoConfig(res.getIdentityProviderConfig))
  }

  def listIdentityProviderConfigs(token: Option[String]): Future[Seq[IdentityProviderConfig]] = {
    val request = proto.ListIdentityProviderConfigsRequest()
    LedgerClient
      .stub(service, token)
      .listIdentityProviderConfigs(request)
      .map(res => res.identityProviderConfigs.map(fromProtoConfig))
  }

  def deleteIdentityProviderConfig(
      identityProviderId: IdentityProviderId.Id,
      token: Option[String],
  ): Future[Unit] = {
    val request = proto.DeleteIdentityProviderConfigRequest(identityProviderId.toRequestString)
    LedgerClient
      .stub(service, token)
      .deleteIdentityProviderConfig(request)
      .map(_ => ())
  }
}

object IdentityProviderConfigClient {
  private def toProtoConfig(config: IdentityProviderConfig): proto.IdentityProviderConfig =
    proto.IdentityProviderConfig(
      config.identityProviderId.toRequestString,
      config.isDeactivated,
      config.issuer,
      config.jwksUrl.value,
      config.audience.getOrElse(""),
    )

  private def fromProtoConfig(config: proto.IdentityProviderConfig) =
    IdentityProviderConfig(
      IdentityProviderId.Id(Ref.LedgerString.assertFromString(config.identityProviderId)),
      config.isDeactivated,
      JwksUrl.assertFromString(config.jwksUrl),
      config.issuer,
      Option(config.audience).filter(_.trim.nonEmpty),
    )
}
