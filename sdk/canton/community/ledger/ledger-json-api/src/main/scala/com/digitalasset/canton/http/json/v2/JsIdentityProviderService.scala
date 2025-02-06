// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.admin.identity_provider_config_service
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.ledger.client.services.admin.IdentityProviderConfigClient
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.server.ServerEndpoint

import scala.concurrent.Future

class JsIdentityProviderService(
    identityProviderConfigClient: IdentityProviderConfigClient,
    val loggerFactory: NamedLoggerFactory,
) extends Endpoints
    with NamedLogging {

  def endpoints(): List[ServerEndpoint[Any, Future]] =
    List(
      withServerLogic(
        JsIdentityProviderService.createIdpsEndpoint,
        createIdps,
      ),
      withServerLogic(
        JsIdentityProviderService.updateIdpEndpoint,
        updateIdp,
      ),
      withServerLogic(
        JsIdentityProviderService.getIdpEndpoint,
        getIdp,
      ),
      withServerLogic(
        JsIdentityProviderService.deleteIdpEndpoint,
        deleteIdp,
      ),
      withServerLogic(
        JsIdentityProviderService.listIdpsEndpoint,
        listIdps,
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
      .resultToRight

  private def createIdps(
      callerContext: CallerContext
  ): TracedInput[identity_provider_config_service.CreateIdentityProviderConfigRequest] => Future[
    Either[JsCantonError, identity_provider_config_service.CreateIdentityProviderConfigResponse]
  ] =
    req =>
      identityProviderConfigClient
        .serviceStub(callerContext.token())(req.traceContext)
        .createIdentityProviderConfig(req.in)
        .resultToRight

  private def updateIdp(
      callerContext: CallerContext
  ): TracedInput[
    (String, identity_provider_config_service.UpdateIdentityProviderConfigRequest)
  ] => Future[
    Either[JsCantonError, identity_provider_config_service.UpdateIdentityProviderConfigResponse]
  ] =
    req =>
      if (req.in._2.identityProviderConfig.map(_.identityProviderId).contains(req.in._1)) {
        identityProviderConfigClient
          .serviceStub(callerContext.token())(req.traceContext)
          .updateIdentityProviderConfig(req.in._2)
          .resultToRight
      } else {
        implicit val traceContext: TraceContext = req.traceContext
        error(
          JsCantonError.fromErrorCode(
            InvalidArgument.Reject(
              s"${req.in._1} does not match idp in body: ${req.in._2.identityProviderConfig}"
            )
          )
        )
      }

  private def getIdp(
      callerContext: CallerContext
  ): TracedInput[String] => Future[
    Either[JsCantonError, identity_provider_config_service.GetIdentityProviderConfigResponse]
  ] = { req =>
    identityProviderConfigClient
      .serviceStub(callerContext.token())(req.traceContext)
      .getIdentityProviderConfig(
        identity_provider_config_service
          .GetIdentityProviderConfigRequest(identityProviderId = req.in)
      )
      .resultToRight
  }

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
        .resultToRight

}

object JsIdentityProviderService extends DocumentationEndpoints {
  import Endpoints.*
  import JsIdentityProviderCodecs.*

  private val identityProviderPath = "idp-id"
  private val idps = v2Endpoint.in("idps")

  val listIdpsEndpoint =
    idps.get
      .out(jsonBody[identity_provider_config_service.ListIdentityProviderConfigsResponse])
      .description("List all identity provider configs")

  val createIdpsEndpoint =
    idps.post
      .in(jsonBody[identity_provider_config_service.CreateIdentityProviderConfigRequest])
      .out(jsonBody[identity_provider_config_service.CreateIdentityProviderConfigResponse])
      .description("Create identity provider configs")

  val updateIdpEndpoint =
    idps.patch
      .in(path[String](identityProviderPath))
      .in(jsonBody[identity_provider_config_service.UpdateIdentityProviderConfigRequest])
      .out(jsonBody[identity_provider_config_service.UpdateIdentityProviderConfigResponse])
      .description("Update identity provider config")

  val getIdpEndpoint =
    idps.get
      .in(path[String](identityProviderPath))
      .out(jsonBody[identity_provider_config_service.GetIdentityProviderConfigResponse])
      .description("Get identity provider config")

  val deleteIdpEndpoint =
    idps.delete
      .in(path[String](identityProviderPath))
      .out(jsonBody[identity_provider_config_service.DeleteIdentityProviderConfigResponse])
      .description("Delete identity provider config")

  override def documentation: Seq[AnyEndpoint] = List(
    createIdpsEndpoint,
    updateIdpEndpoint,
    getIdpEndpoint,
    deleteIdpEndpoint,
    listIdpsEndpoint,
  )
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
