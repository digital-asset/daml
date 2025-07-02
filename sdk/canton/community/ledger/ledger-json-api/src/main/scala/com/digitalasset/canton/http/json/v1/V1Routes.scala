// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v1

import com.daml.jwt.Jwt
import com.daml.logging.LoggingContextOf
import com.daml.logging.LoggingContextOf.withEnrichedLoggingContext
import com.daml.metrics.Timed
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.http.EndpointsCompanion.*
import com.digitalasset.canton.http.json.*
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.http.util.ApiValueToLfValueConverter
import com.digitalasset.canton.http.util.FutureUtil.{either, rightT}
import com.digitalasset.canton.http.util.Logging.{
  InstanceUUID,
  RequestID,
  extendWithRequestIdLogCtx,
}
import com.digitalasset.canton.http.{
  EndpointsCompanion,
  ErrorResponse,
  OkResponse,
  SyncResponse,
  WebsocketConfig,
  endpoints,
  json,
  util,
}
import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.digitalasset.canton.ledger.client.services.admin.UserManagementClient
import com.digitalasset.canton.ledger.client.services.pkg.PackageClient
import com.digitalasset.canton.logging.NoLogging.noTracingLogger
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.`Content-Type`
import org.apache.pekko.http.scaladsl.server
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.RouteResult.*
import org.apache.pekko.http.scaladsl.server.{Directive, Directive0, PathMatcher, Route}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.util.ByteString
import scalaz.EitherT.eitherT
import scalaz.Scalaz.some
import scalaz.std.scalaFuture.*
import scalaz.syntax.std.option.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, EitherT, \/, \/-}
import spray.json.*

import scala.annotation.nowarn
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import LedgerReader.PackageStore
import ContractsService.SearchResult

class V1Routes(
    allowNonHttps: Boolean,
    decodeJwt: EndpointsCompanion.ValidateJwt,
    commandService: CommandService,
    contractsService: ContractsService,
    partiesService: PartiesService,
    packageManagementService: PackageManagementService,
    websocketEndpoints: WebsocketEndpoints,
    encoder: ApiJsonEncoder,
    decoder: ApiJsonDecoder,
    shouldLogHttpBodies: Boolean,
    resolveUser: ResolveUser,
    userManagementClient: UserManagementClient,
    val loggerFactory: NamedLoggerFactory,
    maxTimeToCollectRequest: FiniteDuration = FiniteDuration(5, "seconds"),
)(implicit ec: ExecutionContext, mat: Materializer)
    extends NamedLogging
    with NoTracing {

  private[this] val routeSetup: RouteSetup = new RouteSetup(
    allowNonHttps = allowNonHttps,
    decodeJwt = decodeJwt,
    encoder = encoder,
    resolveUser,
    maxTimeToCollectRequest = maxTimeToCollectRequest,
    loggerFactory = loggerFactory,
  )

  private[this] val commandsHelper: CreateAndExercise =
    new CreateAndExercise(routeSetup, decoder, commandService, contractsService)
  import commandsHelper.*

  private[this] val userManagement: UserManagement = new UserManagement(
    decodeJwt = decodeJwt,
    userManagementClient,
  )
  import userManagement.*

  private[this] val packagesDars: PackagesAndDars =
    new PackagesAndDars(routeSetup, packageManagementService)
  import packagesDars.*

  private[this] val contractList: endpoints.ContractList =
    new endpoints.ContractList(routeSetup, decoder, contractsService, loggerFactory)
  import contractList.*

  private[this] val partiesEP: Parties = new Parties(partiesService)
  import partiesEP.*

  // Limit logging of bodies to content with size of less than 10 KiB.
  // Reason is that a char of an UTF-8 string consumes 1 up to 4 bytes such that the string length
  // with this limit will be 2560 chars up to 10240 chars. This can hold already the whole cascade
  // of import statements in this file, which I would consider already as very big string to log.
  private final val maxBodySizeForLogging = Math.pow(2, 10) * 10

  import V1Routes.*
  import json.JsonProtocol.*
  import util.ErrorOps.*

  private def responseToRoute(res: Future[HttpResponse]): Route = _ => res map Complete.apply
  private def toRoute[T: MkHttpResponse](res: => T)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Route =
    responseToRoute(httpResponse(res))

  private def toPostRoute[Req: JsonReader, Res: JsonWriter](
      httpRequest: HttpRequest,
      fn: (Jwt, Req) => ET[SyncResponse[Res]],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Route = {
    val res = for {
      t <- routeSetup.inputJsVal(httpRequest): ET[(Jwt, JsValue)]
      (jwt, reqBody) = t
      req <- either(SprayJson.decode[Req](reqBody).liftErr(InvalidUserInput.apply)): ET[Req]
      res <- eitherT(RouteSetup.handleFutureEitherFailure(fn(jwt, req).run)): ET[
        SyncResponse[Res]
      ]
    } yield res
    responseToRoute(httpResponse(res))
  }

  private def toGetRoute[Res](
      httpRequest: HttpRequest,
      fn: Jwt => ET[SyncResponse[Res]],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      mkHttpResponse: MkHttpResponse[ET[SyncResponse[Res]]],
  ): Route = {
    val res = for {
      t <- eitherT(routeSetup.input(httpRequest)): ET[(Jwt, String)]
      (jwt, _) = t
      res <- eitherT(RouteSetup.handleFutureEitherFailure(fn(jwt).run)): ET[
        SyncResponse[Res]
      ]
    } yield res
    responseToRoute(httpResponse(res))
  }

  private def toDownloadPackageRoute(
      httpRequest: HttpRequest,
      packageId: String,
      fn: (Jwt, String) => Future[HttpResponse],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Route =
    responseToRoute(
      httpResponse(
        extractJwt(httpRequest).flatMap { jwt =>
          rightT(fn(jwt, packageId))
        }
      )
    )

  private def extractJwt(
      httpRequest: HttpRequest
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[Jwt] = for {
    t <- eitherT(routeSetup.input(httpRequest)): ET[(Jwt, String)]
    (jwt, _) = t
  } yield jwt

  private def mkRequestLogMsg(request: HttpRequest, remoteAddress: RemoteAddress) =
    s"Incoming ${request.method.value} request on ${request.uri} from $remoteAddress"

  private def mkResponseLogMsg(statusCode: StatusCode) =
    s"Responding to client with HTTP $statusCode"

  // Always put this directive after a path to ensure
  // that you don't log request bodies multiple times (simply because a matching test was made multiple times).
  // TL;DR JUST PUT THIS THING AFTER YOUR FINAL PATH MATCHING
  private def logRequestResponseHelper(
      logIncomingRequest: (HttpRequest, RemoteAddress) => HttpRequest,
      logResponse: HttpResponse => HttpResponse,
  ): Directive0 =
    extractClientIP flatMap { remoteAddress =>
      mapRequest(request => logIncomingRequest(request, remoteAddress)) & mapRouteResultFuture {
        responseF =>
          for {
            response <- responseF
            transformedResponse <- response match {
              case Complete(httpResponse) =>
                Future.successful(Complete(logResponse(httpResponse)))
              case _ =>
                Future.failed(
                  new RuntimeException(
                    """Logging the request & response should never happen on routes which get rejected.
                    |Make sure to place the directive only at places where a match is guaranteed (e.g. after the path directive).""".stripMargin
                  )
                )
            }
          } yield transformedResponse
      }
    }

  private def logJsonRequestAndResult(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Directive0 = {
    def logWithHttpMessageBodyIfAvailable(
        httpMessage: HttpMessage,
        msg: String,
        kind: String,
    ): httpMessage.Self =
      if (
        httpMessage
          .header[`Content-Type`]
          .map(_.contentType)
          .contains(ContentTypes.`application/json`)
      ) {
        def logWithBodyInCtx(body: com.daml.logging.entries.LoggingValue) =
          withEnrichedLoggingContext(
            LoggingContextOf.label[RequestEntity],
            s"${kind}_body" -> body,
          )
            .run(implicit lc => logger.info(s"$msg, ${lc.makeString}"))
        httpMessage.entity.contentLengthOption match {
          case Some(length) if length < maxBodySizeForLogging =>
            import org.apache.pekko.stream.scaladsl.*
            httpMessage
              .transformEntityDataBytes(
                Flow.fromFunction { it =>
                  try logWithBodyInCtx(it.utf8String.parseJson)
                  catch {
                    case NonFatal(ex) =>
                      logger.error(s"Failed to log message body, ${lc.makeString}: ", ex)
                  }
                  it
                }
              )
          case other =>
            val reason = other
              .map(length => s"size of $length is too big for logging")
              .getOrElse {
                if (httpMessage.entity.isChunked())
                  "is chunked & overall size is unknown"
                else
                  "size is unknown"
              }
            logWithBodyInCtx(s"omitted because $kind body $reason")
            httpMessage.self
        }
      } else {
        logger.info(s"$msg, ${lc.makeString}")
        httpMessage.self
      }
    logRequestResponseHelper(
      (request, remoteAddress) =>
        logWithHttpMessageBodyIfAvailable(
          request,
          mkRequestLogMsg(request, remoteAddress),
          "request",
        ),
      httpResponse =>
        logWithHttpMessageBodyIfAvailable(
          httpResponse,
          mkResponseLogMsg(httpResponse.status),
          "response",
        ),
    )
  }

  def logRequestAndResultSimple(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Directive0 =
    logRequestResponseHelper(
      (request, remoteAddress) => {
        logger.info(s"${mkRequestLogMsg(request, remoteAddress)}, ${lc.makeString}")
        request
      },
      httpResponse => {
        logger.info(s"${mkResponseLogMsg(httpResponse.status)}, ${lc.makeString}")
        httpResponse
      },
    )
  val logRequestAndResultFn: LoggingContextOf[InstanceUUID with RequestID] => Directive0 =
    if (shouldLogHttpBodies) lc => logJsonRequestAndResult(lc)
    else lc => logRequestAndResultSimple(lc)

  def logRequestAndResult(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): Directive0 =
    logRequestAndResultFn(lc)

  def v1Routes(implicit
      lc0: LoggingContextOf[InstanceUUID],
      metrics: HttpApiMetrics,
  ): Route = extractRequest apply { req =>
    implicit val lc: LoggingContextOf[InstanceUUID with RequestID] =
      extendWithRequestIdLogCtx(identity)(lc0)
    val markThroughputAndLogProcessingTime: Directive0 = Directive { (fn: Unit => Route) =>
      val t0 = System.nanoTime
      fn(()).andThen { res =>
        res.onComplete(_ =>
          logger.trace(s"Processed request after ${System.nanoTime() - t0}ns, ${lc.makeString}")
        )
        res
      }
    }
    def path[L](pm: PathMatcher[L]) =
      server.Directives.path(pm) & markThroughputAndLogProcessingTime & logRequestAndResult

    concat(
      pathPrefix("v1") apply concat(
        post apply concat(
          path("create") apply toRoute(create(req)),
          path("exercise") apply toRoute(exercise(req)),
          path("create-and-exercise") apply toRoute(
            createAndExercise(req)
          ),
          path("query").apply(toRoute(query(req))),
          path("fetch") apply toRoute(fetch(req)),
          path("user") apply toPostRoute(req, getUser),
          path("user" / "create") apply toPostRoute(req, createUser),
          path("user" / "delete") apply toPostRoute(req, deleteUser),
          path("user" / "rights") apply toPostRoute(req, listUserRights),
          path("user" / "rights" / "grant") apply toPostRoute(req, grantUserRights),
          path("user" / "rights" / "revoke") apply toPostRoute(req, revokeUserRights),
          path("parties") apply toPostRoute(req, parties),
          path("parties" / "allocate") apply toPostRoute(
            req,
            allocateParty,
          ),
          path("packages") apply toRoute(uploadDarFile(req)),
        ),
        get apply concat(
          path("query") apply toRoute(retrieveAll(req)),
          path("user") apply toGetRoute(req, getAuthenticatedUser),
          path("user" / "rights") apply toGetRoute(req, listAuthenticatedUserRights),
          path("users") apply toGetRoute(req, listUsers),
          path("parties") apply toGetRoute(req, allParties),
          path("packages") apply toGetRoute(req, listPackages),
          path("packages" / ".+".r)(packageId =>
            extractRequest apply (req => toDownloadPackageRoute(req, packageId, downloadPackage))
          ),
        ),
      ),
      websocketEndpoints.transactionWebSocket,
    )
  }

  private def httpResponse[T](output: T)(implicit
      T: MkHttpResponse[T],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): Future[HttpResponse] =
    T.run(output)
      .recover(Error.fromThrowable andThen (httpResponseError(_, logger)))

  private implicit def sourceStreamSearchResults[A: JsonWriter](implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): MkHttpResponse[ET[SyncResponse[Source[Error \/ A, NotUsed]]]] =
    MkHttpResponse { output =>
      implicitly[MkHttpResponse[Future[Error \/ SearchResult[Error \/ JsValue]]]]
        .run(output.map(_ map (_ map (_ map ((_: A).toJson)))).run)
    }

  private implicit def searchResults(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): MkHttpResponse[Future[Error \/ SearchResult[Error \/ JsValue]]] =
    MkHttpResponse { output =>
      output.flatMap(_.fold(e => Future(httpResponseError(e, logger)), searchHttpResponse))
    }

  private implicit def mkHttpResponseEitherT(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): MkHttpResponse[ET[HttpResponse]] =
    MkHttpResponse { output =>
      implicitly[MkHttpResponse[Future[Error \/ HttpResponse]]].run(output.run)
    }

  private implicit def mkHttpResponse(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): MkHttpResponse[Future[Error \/ HttpResponse]] =
    MkHttpResponse { output =>
      output.map(_.fold(httpResponseError(_, logger), identity))
    }

  private def searchHttpResponse(
      searchResult: SearchResult[Error \/ JsValue]
  )(implicit lc: LoggingContextOf[RequestID]): Future[HttpResponse] = {
    import json.JsonProtocol.*

    (searchResult match {
      case OkResponse(result, warnings, _) =>
        val warningsJsVal: Option[JsValue] = warnings.map(SprayJson.encodeUnsafe(_))
        ResponseFormats.resultJsObject(result via filterStreamErrors, warningsJsVal)
      case error: ErrorResponse =>
        val jsVal: JsValue = SprayJson.encodeUnsafe(error)
        Future((Source.single(ByteString(jsVal.compactPrint)), StatusCodes.InternalServerError))
    }).map { case (response: Source[ByteString, NotUsed], statusCode: StatusCode) =>
      HttpResponse(
        status = statusCode,
        entity = HttpEntity
          .Chunked(ContentTypes.`application/json`, response.map(HttpEntity.ChunkStreamPart(_))),
      )
    }
  }

  private[this] def filterStreamErrors[A](implicit
      lc: LoggingContextOf[RequestID]
  ): Flow[Error \/ A, Error \/ A, NotUsed] =
    Flow[Error \/ A].map {
      case -\/(ServerError(t)) =>
        val hideMsg = "internal server error"
        logger.error(
          s"hiding internal error details from response, responding '$hideMsg' instead, ${lc.makeString}",
          t,
        )
        -\/(ServerError.fromMsg(hideMsg))
      case o => o
    }

  private implicit def fullySync[A: JsonWriter](implicit
      metrics: HttpApiMetrics,
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): MkHttpResponse[ET[SyncResponse[A]]] = MkHttpResponse { result =>
    Timed.future(
      metrics.responseCreationTimer,
      result
        .flatMap { x =>
          either(SprayJson.encode1(x).map(y => (y, x.status)).liftErr(ServerError.fromMsg))
        }
        .run
        .map {
          case -\/(e) =>
            httpResponseError(e, logger)
          case \/-((jsVal, status)) =>
            HttpResponse(
              entity = HttpEntity.Strict(ContentTypes.`application/json`, format(jsVal)),
              status = status,
            )
        },
    )
  }

}

object V1Routes extends NoTracing {
  type ET[A] = EitherT[Future, Error, A]

  final class IntoEndpointsError[-A](val run: A => Error) extends AnyVal
  object IntoEndpointsError {
    import LedgerClientJwt.Grpc.Category

    implicit val id: IntoEndpointsError[Error] = new IntoEndpointsError(identity)

    implicit val fromCommands: IntoEndpointsError[CommandService.Error] = new IntoEndpointsError({
      case CommandService.InternalError(id, reason) =>
        ServerError(
          new Exception(
            s"command service error, ${id.cata(sym => s"${sym.name}: ", "")}${reason.getMessage}",
            reason,
          )
        )
      case CommandService.GrpcError(status) =>
        ParticipantServerError(status)
      case CommandService.ClientError(-\/(Category.PermissionDenied), message) =>
        Unauthorized(message)
      case CommandService.ClientError(\/-(Category.InvalidArgument), message) =>
        InvalidUserInput(message)
    })

    implicit val fromContracts: IntoEndpointsError[ContractsService.Error] =
      new IntoEndpointsError({ case ContractsService.InternalError(id, msg) =>
        ServerError.fromMsg(s"contracts service error, ${id.name}: $msg")
      })
  }

  private final case class MkHttpResponse[-T](run: T => Future[HttpResponse])

  def doLoad(
      packageClient: PackageClient,
      ledgerReader: LedgerReader,
      loadCache: LedgerReader.LoadCache,
  )(jwt: Jwt)(ids: Set[String])(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[PackageService.ServerError \/ Option[PackageStore]] =
    ledgerReader
      .loadPackageStoreUpdates(
        packageClient,
        loadCache,
        some(jwt.value),
      )(ids)
      .map(_.leftMap(e => PackageService.ServerError(e)))

  def buildJsonCodecs(
      packageService: PackageService
  )(implicit ec: ExecutionContext): (ApiJsonEncoder, ApiJsonDecoder) = {

    val lfTypeLookup = LedgerReader.damlLfTypeLookup(() => packageService.packageStore) _
    val jsValueToApiValueConverter = new JsValueToApiValueConverter(lfTypeLookup)

    val apiValueToJsValueConverter = new ApiValueToJsValueConverter(
      ApiValueToLfValueConverter.apiValueToLfValue
    )

    val encoder = new ApiJsonEncoder(
      apiValueToJsValueConverter.apiRecordToJsObject,
      apiValueToJsValueConverter.apiValueToJsValue,
    )

    val decoder = new ApiJsonDecoder(
      packageService.resolveContractTypeId,
      packageService.resolveTemplateRecordType,
      packageService.resolveChoiceArgType,
      packageService.resolveKeyType,
      jsValueToApiValueConverter.jsValueToApiValue,
      jsValueToApiValueConverter.jsValueToLfValue,
    )

    (encoder, decoder)
  }

  // TODO(#23504) remove submitAndWaitForTransactionTree as it is deprecated
  @nowarn("cat=deprecation")
  def apply(
      ledgerClient: DamlLedgerClient,
      allowNonHttps: Boolean,
      decodeJwt: EndpointsCompanion.ValidateJwt,
      shouldLogHttpBodies: Boolean,
      resolveUser: ResolveUser,
      userManagementClient: UserManagementClient,
      loggerFactory: NamedLoggerFactory,
      websocketConfig: Option[WebsocketConfig],
      maxTimeToCollectRequest: FiniteDuration = FiniteDuration(5, "seconds"),
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      mat: Materializer,
  ): V1Routes = {

    val directEc = DirectExecutionContext(noTracingLogger)

    val packageCache = LedgerReader.LoadCache.freshCache()

    val packageService = new PackageService(
      reloadPackageStoreIfChanged =
        doLoad(ledgerClient.packageService, LedgerReader(loggerFactory), packageCache),
      loggerFactory = loggerFactory,
    )

    val (encoder, decoder) = buildJsonCodecs(packageService)

    val ledgerClientJwt = LedgerClientJwt(loggerFactory)

    val commandService = new CommandService(
      ledgerClientJwt.submitAndWaitForTransaction(ledgerClient),
      ledgerClientJwt.submitAndWaitForTransactionTree(ledgerClient),
      loggerFactory,
    )

    val contractsService = new ContractsService(
      packageService.resolveContractTypeId,
      packageService.allTemplateIds,
      ledgerClientJwt.getByContractId(ledgerClient),
      ledgerClientJwt.getActiveContracts(ledgerClient),
      ledgerClientJwt.getCreatesAndArchivesSince(ledgerClient),
      ledgerClientJwt.getLedgerEnd(ledgerClient),
      loggerFactory,
    )

    val partiesService = new PartiesService(
      ledgerClientJwt.listKnownParties(ledgerClient),
      ledgerClientJwt.getParties(ledgerClient),
      ledgerClientJwt.allocateParty(ledgerClient),
    )

    val packageManagementService = new PackageManagementService(
      ledgerClientJwt.listPackages(ledgerClient),
      ledgerClientJwt.getPackage(ledgerClient),
      { case (jwt, byteString) =>
        implicit lc =>
          ledgerClientJwt
            .uploadDar(ledgerClient)(directEc, traceContext)(
              jwt,
              byteString,
            )(lc)
            .flatMap(_ => packageService.reload(jwt))
            .map(_ => ())
      },
    )

    val websocketService = new WebSocketService(
      contractsService,
      packageService.resolveContractTypeId,
      decoder,
      websocketConfig,
      loggerFactory,
    )

    val websocketEndpoints = new WebsocketEndpoints(
      decodeJwt,
      websocketService,
      resolveUser,
      loggerFactory,
    )

    new V1Routes(
      allowNonHttps,
      decodeJwt,
      commandService,
      contractsService,
      partiesService,
      packageManagementService,
      websocketEndpoints,
      encoder,
      decoder,
      shouldLogHttpBodies,
      resolveUser,
      userManagementClient,
      loggerFactory,
      maxTimeToCollectRequest,
    )
  }
}
