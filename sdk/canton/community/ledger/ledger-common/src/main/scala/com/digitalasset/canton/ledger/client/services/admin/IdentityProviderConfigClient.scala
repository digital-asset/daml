// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.admin

import com.daml.jwt.JwksUrl
import com.daml.ledger.api.v2.admin.identity_provider_config_service as proto
import com.daml.ledger.api.v2.admin.identity_provider_config_service.IdentityProviderConfigServiceGrpc.IdentityProviderConfigServiceStub
import com.digitalasset.canton.ledger.api.{IdentityProviderConfig, IdentityProviderId}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask

import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
final class IdentityProviderConfigClient(
    service: IdentityProviderConfigServiceStub,
    getDefaultToken: () => Option[String] = () => None,
)(implicit
    ec: ExecutionContext
) {

  import IdentityProviderConfigClient.*
  def createIdentityProviderConfig(
      config: IdentityProviderConfig,
      token: Option[String],
  )(implicit traceContext: TraceContext): Future[IdentityProviderConfig] = {
    val request = proto.CreateIdentityProviderConfigRequest(
      Some(IdentityProviderConfigClient.toProtoConfig(config))
    )
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .createIdentityProviderConfig(request)
      .map(res => fromProtoConfig(res.getIdentityProviderConfig))
  }

  def getIdentityProviderConfig(
      identityProviderId: IdentityProviderId.Id,
      token: Option[String],
  )(implicit traceContext: TraceContext): Future[IdentityProviderConfig] = {
    val request = proto.GetIdentityProviderConfigRequest(identityProviderId.toRequestString)
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .getIdentityProviderConfig(request)
      .map(res => fromProtoConfig(res.getIdentityProviderConfig))
  }

  def updateIdentityProviderConfig(
      config: IdentityProviderConfig,
      updateMask: FieldMask,
      token: Option[String],
  )(implicit traceContext: TraceContext): Future[IdentityProviderConfig] = {
    val request = proto.UpdateIdentityProviderConfigRequest(
      Some(IdentityProviderConfigClient.toProtoConfig(config)),
      Some(updateMask),
    )
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .updateIdentityProviderConfig(request)
      .map(res => fromProtoConfig(res.getIdentityProviderConfig))
  }

  def listIdentityProviderConfigs(
      token: Option[String]
  )(implicit traceContext: TraceContext): Future[Seq[IdentityProviderConfig]] = {
    val request = proto.ListIdentityProviderConfigsRequest()
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .listIdentityProviderConfigs(request)
      .map(res => res.identityProviderConfigs.map(fromProtoConfig))
  }

  def deleteIdentityProviderConfig(
      identityProviderId: IdentityProviderId.Id,
      token: Option[String],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val request = proto.DeleteIdentityProviderConfigRequest(identityProviderId.toRequestString)
    LedgerClient
      .stubWithTracing(service, token.orElse(getDefaultToken()))
      .deleteIdentityProviderConfig(request)
      .map(_ => ())
  }

  def serviceStub(token: Option[String] = None)(implicit
      traceContext: TraceContext
  ): IdentityProviderConfigServiceStub =
    LedgerClient.stubWithTracing(service, token.orElse(getDefaultToken()))

}

object IdentityProviderConfigClient {
  def toProtoConfig(config: IdentityProviderConfig): proto.IdentityProviderConfig =
    proto.IdentityProviderConfig(
      config.identityProviderId.toRequestString,
      config.isDeactivated,
      config.issuer,
      config.jwksUrl.value,
      config.audience.getOrElse(""),
    )

  def fromProtoConfig(config: proto.IdentityProviderConfig): IdentityProviderConfig =
    IdentityProviderConfig(
      IdentityProviderId.Id(Ref.LedgerString.assertFromString(config.identityProviderId)),
      config.isDeactivated,
      JwksUrl.assertFromString(config.jwksUrl),
      config.issuer,
      Option(config.audience).filter(_.trim.nonEmpty),
    )
}
