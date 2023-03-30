// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.admin

import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId, JwksUrl}
import com.daml.ledger.api.v1.admin.identity_provider_config_service.IdentityProviderConfigServiceGrpc.IdentityProviderConfigServiceStub
import com.daml.ledger.api.v1.admin.{identity_provider_config_service => proto}
import com.daml.ledger.client.LedgerClient
import com.daml.lf.data.Ref

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
      .map(res => fromProtoConfig(res.identityProviderConfig.get))
  }
}

object IdentityProviderConfigClient {
  private def toProtoConfig(config: IdentityProviderConfig) =
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
