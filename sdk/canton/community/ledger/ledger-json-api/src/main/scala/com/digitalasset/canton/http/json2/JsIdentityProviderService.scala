// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json2

import com.daml.ledger.api.v2.admin.identity_provider_config_service
import com.digitalasset.canton.ledger.client.services.admin.IdentityProviderConfigClient
import com.digitalasset.canton.http.json2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.*
import sttp.tapir.generic.auto.*

import scala.language.existentials
import scala.concurrent.Future

class JsIdentityProviderService(
    identityProviderConfigClient: IdentityProviderConfigClient,
    val loggerFactory: NamedLoggerFactory,
) extends Endpoints
    with NamedLogging {
  import JsIdentityProviderCodecs.*
  private val idps = baseEndpoint.in("idps")
  private val identityProviderPath = "idp-id"

  def endpoints() =
    List(
      json(
        idps.get
          .description("List all identity provider configs"),
        listIdps,
      ),
      jsonWithBody(
        idps.post
          .description("Create identity provider configs"),
        createIdps,
      ),
      jsonWithBody(
        idps.patch
          .in(path[String](identityProviderPath))
          .description("Update identity provider config"),
        updateIdp,
      ),
      json(
        idps.get
          .in(path[String](identityProviderPath))
          .description("Get identity provider config"),
        getIdp,
      ),
      json(
        idps.delete
          .in(path[String](identityProviderPath))
          .description("Delete identity provider config"),
        deleteIdp,
      ),
    )

  private def listIdps(
      callerContext: CallerContext
  ): TracedInput[Unit] => Future[
    Either[JsCantonError, identity_provider_config_service.ListIdentityProviderConfigsResponse]
  ] = req =>
    identityProviderConfigClient
      .serviceStub(callerContext.token())(req.traceContext)
      .listIdentityProviderConfigs(
        new identity_provider_config_service.ListIdentityProviderConfigsRequest()
      )
      .toRight

  private def createIdps(
      callerContext: CallerContext
  ): (
      TracedInput[Unit],
      identity_provider_config_service.CreateIdentityProviderConfigRequest,
  ) => Future[
    Either[JsCantonError, identity_provider_config_service.CreateIdentityProviderConfigResponse]
  ] =
    (req, body) =>
      identityProviderConfigClient
        .serviceStub(callerContext.token())(req.traceContext)
        .createIdentityProviderConfig(body)
        .toRight

  private def updateIdp(
      callerContext: CallerContext
  ): (
      TracedInput[String],
      identity_provider_config_service.UpdateIdentityProviderConfigRequest,
  ) => Future[
    Either[JsCantonError, identity_provider_config_service.UpdateIdentityProviderConfigResponse]
  ] =
    (req, body) =>
      if (body.identityProviderConfig.map(_.identityProviderId) == Some(req.in)) {
        identityProviderConfigClient
          .serviceStub(callerContext.token())(req.traceContext)
          .updateIdentityProviderConfig(body)
          .toRight
      } else {
        implicit val traceContext = req.traceContext
        error(
          JsCantonError.fromErrorCode(
            InvalidArgument.Reject(
              s"${req.in} does not match idp in body: ${body.identityProviderConfig}"
            )
          )
        )
      }

  private def getIdp(
      callerContext: CallerContext
  ): TracedInput[String] => Future[
    Either[JsCantonError, identity_provider_config_service.GetIdentityProviderConfigResponse]
  ] =
    req =>
      identityProviderConfigClient
        .serviceStub(callerContext.token())(req.traceContext)
        .getIdentityProviderConfig(
          identity_provider_config_service
            .GetIdentityProviderConfigRequest(identityProviderId = req.in)
        )
        .toRight

  private def deleteIdp(
      callerContext: CallerContext
  ): TracedInput[String] => Future[
    Either[JsCantonError, identity_provider_config_service.DeleteIdentityProviderConfigResponse]
  ] =
    req =>
      identityProviderConfigClient
        .serviceStub(callerContext.token())(req.traceContext)
        .deleteIdentityProviderConfig(
          identity_provider_config_service
            .DeleteIdentityProviderConfigRequest(identityProviderId = req.in)
        )
        .toRight

}

object JsIdentityProviderCodecs {
  implicit val identityProviderConfig
      : Codec[identity_provider_config_service.IdentityProviderConfig] =
    deriveCodec
  implicit val listIdentityProviderConfigsRequest
      : Codec[identity_provider_config_service.ListIdentityProviderConfigsRequest] =
    deriveCodec
  implicit val listIdentityProviderConfigsResponse
      : Codec[identity_provider_config_service.ListIdentityProviderConfigsResponse] =
    deriveCodec

  implicit val createIdentityProviderConfigRequest
      : Codec[identity_provider_config_service.CreateIdentityProviderConfigRequest] =
    deriveCodec

  implicit val createIdentityProviderConfigResponse
      : Codec[identity_provider_config_service.CreateIdentityProviderConfigResponse] =
    deriveCodec

  implicit val updateIdentityProviderConfigRequest
      : Codec[identity_provider_config_service.UpdateIdentityProviderConfigRequest] =
    deriveCodec

  implicit val updateIdentityProviderConfigResponse
      : Codec[identity_provider_config_service.UpdateIdentityProviderConfigResponse] =
    deriveCodec

  implicit val getIdentityProviderConfigRequest
      : Codec[identity_provider_config_service.GetIdentityProviderConfigRequest] =
    deriveCodec

  implicit val getIdentityProviderConfigResponse
      : Codec[identity_provider_config_service.GetIdentityProviderConfigResponse] =
    deriveCodec

  implicit val deleteIdentityProviderConfigRequest
      : Codec[identity_provider_config_service.DeleteIdentityProviderConfigRequest] =
    deriveCodec

  implicit val deleteIdentityProviderConfigResponse
      : Codec[identity_provider_config_service.DeleteIdentityProviderConfigResponse] =
    deriveCodec

}
