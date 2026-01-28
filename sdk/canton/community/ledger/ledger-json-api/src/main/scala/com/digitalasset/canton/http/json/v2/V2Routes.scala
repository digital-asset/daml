// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.http.json.JsHealthService
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.DamlDefinitionsView
import com.digitalasset.canton.http.{HealthService, WebsocketConfig}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.services.version.VersionClient
import com.digitalasset.canton.ledger.participant.state.PackageSyncService
import com.digitalasset.canton.logging.audit.{ApiRequestLogger, ResponseKind, TransportType}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CallMetadata
import com.digitalasset.canton.platform.PackagePreferenceBackend
import com.digitalasset.canton.tracing.{TraceContext, W3CTraceContext}
import org.apache.pekko.http.scaladsl.model.{AttributeKeys, HttpRequest}
import org.apache.pekko.http.scaladsl.server.RequestContext
import org.apache.pekko.stream.Materializer
import sttp.model.{Header, StatusCode, StatusText}
import sttp.tapir.model.{ConnectionInfo, ServerRequest}
import sttp.tapir.server.interceptor.RequestInterceptor.RequestResultTransform
import sttp.tapir.server.interceptor.{RequestInterceptor, RequestResult}
import sttp.tapir.server.pekkohttp.{PekkoHttpServerInterpreter, PekkoHttpServerOptions}

import java.net.InetSocketAddress
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
    contractService: JsContractService,
    userManagementService: JsUserManagementService,
    versionService: JsVersionService,
    metadataServiceIfEnabled: Option[JsDamlDefinitionsService],
    versionClient: VersionClient,
    requestLogger: ApiRequestLogger,
    val loggerFactory: NamedLoggerFactory,
    jsHealthService: JsHealthService,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private val serverEndpoints =
    commandService.endpoints() ++ eventService.endpoints() ++ versionService
      .endpoints() ++ packageService.endpoints() ++ partyManagementService
      .endpoints() ++ stateService.endpoints() ++ updateService.endpoints() ++ userManagementService
      .endpoints() ++ identityProviderService
      .endpoints() ++ interactiveSubmissionService
      .endpoints() ++ metadataServiceIfEnabled.toList.flatMap(_.endpoints()) ++ jsHealthService
      .endpoints() ++ contractService.endpoints()

  private val docs =
    new JsApiDocsService(
      versionClient,
      serverEndpoints.map(_.endpoint),
      requestLogger,
      loggerFactory,
    )

  private val pekkoOptions = {
    val requestInterceptors = new RequestInterceptors(requestLogger, loggerFactory)
    PekkoHttpServerOptions.default
      .prependInterceptor(requestInterceptors.loggingInterceptor())
      .appendInterceptor(requestInterceptors.statusInterceptor())
  }

  val combinedRoutes =
    PekkoHttpServerInterpreter(pekkoOptions)(ec).toRoute(serverEndpoints ++ docs.endpoints())
}

object V2Routes {
  def apply(
      ledgerClient: LedgerClient,
      metadataServiceEnabled: Boolean,
      packageSyncService: PackageSyncService,
      packagePreferenceBackend: PackagePreferenceBackend,
      executionContext: ExecutionContext,
      apiLoggingConfig: ApiLoggingConfig,
      loggerFactory: NamedLoggerFactory,
      healthService: HealthService,
  )(implicit
      ws: WebsocketConfig,
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
      authInterceptor: AuthInterceptor,
  ): V2Routes = {
    implicit val ec: ExecutionContext = executionContext
    val requestLogger = new ApiRequestLogger(apiLoggingConfig, loggerFactory)
    val schemaProcessors = new SchemaProcessorsImpl(
      packageSyncService.getPackageMetadataSnapshot(_).packages,
      loggerFactory,
    )

    val transcodePackageIdResolver = TranscodePackageIdResolver.topologyStateBacked(
      packagePreferenceBackend,
      packageSyncService.getPackageMetadataSnapshot(_),
      loggerFactory,
    )
    val protocolConverters = new ProtocolConverters(schemaProcessors, transcodePackageIdResolver)
    val commandService =
      new JsCommandService(ledgerClient, protocolConverters, requestLogger, loggerFactory)

    val eventService =
      new JsEventService(ledgerClient, protocolConverters, requestLogger, loggerFactory)
    val versionService =
      new JsVersionService(ledgerClient.versionClient, requestLogger, loggerFactory)

    val stateService =
      new JsStateService(ledgerClient, protocolConverters, requestLogger, loggerFactory)
    val partyManagementService =
      new JsPartyManagementService(
        ledgerClient.partyManagementClient,
        requestLogger,
        loggerFactory,
      )

    val jsPackageService =
      new JsPackageService(
        ledgerClient.packageService,
        ledgerClient.packageManagementClient,
        requestLogger,
        loggerFactory,
      )

    val updateService =
      new JsUpdateService(ledgerClient, protocolConverters, requestLogger, loggerFactory)

    val contractService =
      new JsContractService(ledgerClient, protocolConverters, requestLogger, loggerFactory)

    val userManagementService =
      new JsUserManagementService(ledgerClient.userManagementClient, requestLogger, loggerFactory)
    val identityProviderService = new JsIdentityProviderService(
      ledgerClient.identityProviderConfigClient,
      requestLogger,
      loggerFactory,
    )
    val interactiveSubmissionService =
      new JsInteractiveSubmissionService(
        ledgerClient,
        protocolConverters,
        requestLogger,
        loggerFactory,
      )
    val damlDefinitionsServiceIfEnabled = Option.when(metadataServiceEnabled) {
      val damlDefinitionsService =
        new DamlDefinitionsView(packageSyncService.getPackageMetadataSnapshot(_))
      new JsDamlDefinitionsService(damlDefinitionsService, requestLogger, loggerFactory)
    }
    val jsHealthService = new JsHealthService(
      healthService = healthService,
      requestLogger = requestLogger,
      loggerFactory = loggerFactory,
    )

    new V2Routes(
      commandService,
      eventService,
      identityProviderService,
      interactiveSubmissionService,
      jsPackageService,
      partyManagementService,
      stateService,
      updateService,
      contractService,
      userManagementService,
      versionService,
      damlDefinitionsServiceIfEnabled,
      ledgerClient.versionClient,
      requestLogger,
      loggerFactory,
      jsHealthService,
    )(executionContext)
  }
}

class RequestInterceptors(
    private val auditLogger: ApiRequestLogger,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def loggingInterceptor() =
    RequestInterceptor.transformServerRequest { request =>
      val incomingHeaders = request.headers.map(h => (h.name, h.value)).toMap
      val extractedW3cTrace = W3CTraceContext.fromHeaders(incomingHeaders)

      def logIncomingRequest()(implicit traceContext: TraceContext): Unit =
        auditLogger.logIncomingRequest(
          RequestInterceptorsUtil.extractCallMetadata(request),
          // TODO (i28332) investigate if we can provide request body here
          s"ContentType:${request.contentType} ContentLength:${request.contentLength}",
        )
      extractedW3cTrace match {
        case Some(trace) =>
          implicit val tc: TraceContext = trace.toTraceContext
          logIncomingRequest()
          Future.successful(request)

        case None =>
          implicit val newTraceContext: TraceContext = TraceContext.createNew("request")
          logger.trace(s"No TraceContext in headers, created new for ${request.showShort}")
          logIncomingRequest()
          val remote = RequestInterceptorsUtil.extractAddress(request)
          val enrichedHeaders = request.headers ++ W3CTraceContext
            .extractHeaders(newTraceContext)
            .map { case (name, value) => Header(name, value) }

          Future.successful(
            request.withOverride(
              headersOverride = Some(enrichedHeaders),
              protocolOverride = None,
              connectionInfoOverride = Some(ConnectionInfo(None, remote.toOption, None)),
            )
          )
      }
    }

  object LoggingResultTransformer extends RequestResultTransform[scala.concurrent.Future] {

    private val securityRelatedCodes = Set(
      StatusCode.Unauthorized,
      StatusCode.Forbidden,
      StatusCode.ProxyAuthenticationRequired,
    )

    private def decideResponseKind(code: StatusCode): ResponseKind = code match {
      case c if c.isServerError => ResponseKind.SevereError
      case c if c.isClientError && securityRelatedCodes.contains(c) => ResponseKind.Security
      case c if c.isClientError => ResponseKind.MinorError
      case _ => ResponseKind.OK
    }

    def apply[B](request: ServerRequest, result: RequestResult[B]): Future[RequestResult[B]] = {
      val addr = RequestInterceptorsUtil.extractAddress(request)
      val incomingHeaders = request.headers.map(h => (h.name, h.value)).toMap
      val extractedW3cTrace = W3CTraceContext.fromHeaders(incomingHeaders)
      val callMetadata = CallMetadata(
        apiEndpoint = request.showShort,
        transport = TransportType.Http,
        remoteAddress = addr,
      )
      implicit val traceContext: TraceContext = extractedW3cTrace
        .map(_.toTraceContext)
        .getOrElse(
          TraceContext.createNew("request")
        )
      result match {
        case RequestResult.Response(response) =>
          auditLogger.logResponseStatus(
            callMetadata,
            decideResponseKind(response.code),
            s"${response.code} ${StatusText.default(response.code).getOrElse("")}",
            None,
          )
          Future.successful(result)
        case RequestResult.Failure(fails) =>
          val error = fails.map(_.failure.toString).mkString("; ")
          auditLogger.logResponseStatus(
            callMetadata,
            ResponseKind.MinorError,
            s"DECODE FAILURE $error",
            None,
          )
          Future.successful(result)
      }
    }
  }

  def statusInterceptor() = RequestInterceptor.transformResult[Future](LoggingResultTransformer)

}

object RequestInterceptorsUtil {
  def extractAddress(
      request: ServerRequest
  ): Either[String, InetSocketAddress] = {
    val remote = request.connectionInfo.remote.orElse {
      request.underlying match {
        case ctx: RequestContext =>
          val req: HttpRequest = ctx.request
          for {
            addr <- req.attribute(AttributeKeys.remoteAddress)
            inet <- addr.toOption
            port = addr.getPort
          } yield {
            new InetSocketAddress(inet, port)
          }
        case _ =>
          // Unexpected backend, cannot extract remote address
          None
      }
    }
    remote.toRight("unknown")
  }

  def extractCallMetadata(request: ServerRequest): CallMetadata = {
    val remote = extractAddress(request)
    val transport = request.uri.scheme match {
      case Some("wss") | Some("ws") => TransportType.HttpWs
      case _ => TransportType.Http
    }
    CallMetadata(
      apiEndpoint = request.showShort,
      transport = transport,
      remoteAddress = remote,
    )
  }
}
