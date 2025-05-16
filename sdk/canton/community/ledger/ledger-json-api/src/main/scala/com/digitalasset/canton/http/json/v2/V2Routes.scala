// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.DamlDefinitionsView
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.services.version.VersionClient
import com.digitalasset.canton.ledger.participant.state.PackageSyncService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{TraceContext, W3CTraceContext}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.Materializer
import sttp.model.Header
import sttp.tapir.server.interceptor.RequestInterceptor
import sttp.tapir.server.pekkohttp.{PekkoHttpServerInterpreter, PekkoHttpServerOptions}

import scala.concurrent.{ExecutionContext, Future}

class V2Routes(
    commandService: JsCommandService,
    eventService: JsEventService,
    identityProviderService: JsIdentityProviderService,
    interactiveSubmissionService: JsInteractiveSubmissionService,
    packageService: JsPackageService,
    partyManagementService: JsPartyManagementService,
    stateService: JsStateService,
    updateService: JsUpdateService,
    userManagementService: JsUserManagementService,
    versionService: JsVersionService,
    metadataServiceIfEnabled: Option[JsDamlDefinitionsService],
    versionClient: VersionClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends Endpoints
    with NamedLogging {
  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private val serverEndpoints =
    commandService.endpoints() ++ eventService.endpoints() ++ versionService
      .endpoints() ++ packageService.endpoints() ++ partyManagementService
      .endpoints() ++ stateService.endpoints() ++ updateService.endpoints() ++ userManagementService
      .endpoints() ++ identityProviderService
      .endpoints() ++ interactiveSubmissionService
      .endpoints() ++ metadataServiceIfEnabled.toList.flatMap(_.endpoints())

  private val docs =
    new JsApiDocsService(versionClient, serverEndpoints.map(_.endpoint), loggerFactory)

  private val pekkoOptions = PekkoHttpServerOptions.default
    .prependInterceptor(new RequestInterceptors(loggerFactory).loggingInterceptor())

  val v2Routes: Route =
    PekkoHttpServerInterpreter(pekkoOptions)(ec).toRoute(serverEndpoints)

  val docsRoute = PekkoHttpServerInterpreter()(ec).toRoute(docs.endpoints())
}

object V2Routes {
  def apply(
      ledgerClient: LedgerClient,
      metadataServiceEnabled: Boolean,
      packageSyncService: PackageSyncService,
      executionContext: ExecutionContext,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      esf: ExecutionSequencerFactory,
      ws: WebsocketConfig,
      materializer: Materializer,
  ): V2Routes = {
    implicit val ec: ExecutionContext = executionContext

    val schemaProcessors = new SchemaProcessors(
      packageSyncService.getPackageMetadataSnapshot(_).packages
    )
    val protocolConverters = new ProtocolConverters(schemaProcessors)
    val commandService =
      new JsCommandService(ledgerClient, protocolConverters, loggerFactory)

    val eventService =
      new JsEventService(ledgerClient, protocolConverters, loggerFactory)
    val versionService = new JsVersionService(ledgerClient.versionClient, loggerFactory)

    val stateService =
      new JsStateService(ledgerClient, protocolConverters, loggerFactory)
    val partyManagementService =
      new JsPartyManagementService(
        ledgerClient.partyManagementClient,
        protocolConverters,
        loggerFactory,
      )

    val jsPackageService =
      new JsPackageService(
        ledgerClient.packageService,
        ledgerClient.packageManagementClient,
        loggerFactory,
      )(
        executionContext,
        materializer,
      )

    val updateService =
      new JsUpdateService(ledgerClient, protocolConverters, loggerFactory)

    val userManagementService =
      new JsUserManagementService(ledgerClient.userManagementClient, loggerFactory)
    val identityProviderService = new JsIdentityProviderService(
      ledgerClient.identityProviderConfigClient,
      loggerFactory,
    )
    val interactiveSubmissionService =
      new JsInteractiveSubmissionService(ledgerClient, protocolConverters, loggerFactory)
    val damlDefinitionsServiceIfEnabled = Option.when(metadataServiceEnabled) {
      val damlDefinitionsService =
        new DamlDefinitionsView(packageSyncService.getPackageMetadataSnapshot(_))
      new JsDamlDefinitionsService(damlDefinitionsService, loggerFactory)
    }

    new V2Routes(
      commandService,
      eventService,
      identityProviderService,
      interactiveSubmissionService,
      jsPackageService,
      partyManagementService,
      stateService,
      updateService,
      userManagementService,
      versionService,
      damlDefinitionsServiceIfEnabled,
      ledgerClient.versionClient,
      loggerFactory,
    )(executionContext)
  }
}

class RequestInterceptors(val loggerFactory: NamedLoggerFactory) extends NamedLogging {
  def loggingInterceptor() =
    RequestInterceptor.transformServerRequest { request =>
      val incomingHeaders = request.headers.map(h => (h.name, h.value)).toMap
      val extractedW3cTrace = W3CTraceContext.fromHeaders(incomingHeaders)
      val uriScheme = request.uri.scheme.getOrElse("")

      def logIncomingRequest()(implicit traceContext: TraceContext): Unit =
        logger.debug(s"Incoming request ($uriScheme): ${request.showShort}")

      extractedW3cTrace match {
        case Some(trace) =>
          implicit val tc: TraceContext = trace.toTraceContext
          logIncomingRequest()
          Future.successful(request)

        case None =>
          implicit val newTraceContext: TraceContext = TraceContext.createNew()
          logger.trace(s"No TraceContext in headers, created new for ${request.showShort}")
          logIncomingRequest()

          val enrichedHeaders = request.headers ++ W3CTraceContext
            .extractHeaders(newTraceContext)
            .map { case (name, value) => Header(name, value) }

          Future.successful(
            request.withOverride(
              headersOverride = Some(enrichedHeaders),
              protocolOverride = None,
              connectionInfoOverride = None,
            )
          )
      }
    }
}
