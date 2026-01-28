// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import cats.implicits.{catsSyntaxSemigroup, toTraverseOps}
import com.daml.ledger.api.testtool.infrastructure.ChannelEndpoint.JsonApiEndpoint
import com.daml.ledger.api.testtool.infrastructure.JsonErrors.GenericErrorCode
import com.daml.ledger.api.testtool.infrastructure.ws.WsHelper
import com.daml.ledger.api.v2.admin.identity_provider_config_service.IdentityProviderConfigServiceGrpc.IdentityProviderConfigService
import com.daml.ledger.api.v2.admin.identity_provider_config_service.{
  CreateIdentityProviderConfigRequest,
  CreateIdentityProviderConfigResponse,
  DeleteIdentityProviderConfigRequest,
  DeleteIdentityProviderConfigResponse,
  GetIdentityProviderConfigRequest,
  GetIdentityProviderConfigResponse,
  IdentityProviderConfigServiceGrpc,
  ListIdentityProviderConfigsRequest,
  ListIdentityProviderConfigsResponse,
  UpdateIdentityProviderConfigRequest,
  UpdateIdentityProviderConfigResponse,
}
import com.daml.ledger.api.v2.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v2.admin.package_management_service.{
  ListKnownPackagesRequest,
  ListKnownPackagesResponse,
  PackageManagementServiceGrpc,
  UpdateVettedPackagesRequest,
  UpdateVettedPackagesResponse,
  UploadDarFileRequest,
  UploadDarFileResponse,
  ValidateDarFileRequest,
  ValidateDarFileResponse,
}
import com.daml.ledger.api.v2.admin.participant_pruning_service.ParticipantPruningServiceGrpc
import com.daml.ledger.api.v2.admin.participant_pruning_service.ParticipantPruningServiceGrpc.ParticipantPruningService
import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocateExternalPartyRequest,
  AllocateExternalPartyResponse,
  AllocatePartyRequest,
  AllocatePartyResponse,
  GenerateExternalPartyTopologyRequest,
  GenerateExternalPartyTopologyResponse,
  GetParticipantIdRequest,
  GetParticipantIdResponse,
  GetPartiesRequest,
  GetPartiesResponse,
  ListKnownPartiesRequest,
  ListKnownPartiesResponse,
  PartyManagementServiceGrpc,
  UpdatePartyDetailsRequest,
  UpdatePartyDetailsResponse,
  UpdatePartyIdentityProviderIdRequest,
  UpdatePartyIdentityProviderIdResponse,
}
import com.daml.ledger.api.v2.admin.user_management_service.UserManagementServiceGrpc.UserManagementService
import com.daml.ledger.api.v2.admin.user_management_service.{
  CreateUserRequest,
  CreateUserResponse,
  DeleteUserRequest,
  DeleteUserResponse,
  GetUserRequest,
  GetUserResponse,
  GrantUserRightsRequest,
  GrantUserRightsResponse,
  ListUserRightsRequest,
  ListUserRightsResponse,
  ListUsersRequest,
  ListUsersResponse,
  RevokeUserRightsRequest,
  RevokeUserRightsResponse,
  UpdateUserIdentityProviderIdRequest,
  UpdateUserIdentityProviderIdResponse,
  UpdateUserRequest,
  UpdateUserResponse,
  UserManagementServiceGrpc,
}
import com.daml.ledger.api.v2.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v2.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitForReassignmentRequest,
  SubmitAndWaitForReassignmentResponse,
  SubmitAndWaitForTransactionRequest,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitRequest,
  SubmitAndWaitResponse,
}
import com.daml.ledger.api.v2.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.api.v2.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitReassignmentRequest,
  SubmitReassignmentResponse,
  SubmitRequest,
  SubmitResponse,
}
import com.daml.ledger.api.v2.contract_service.ContractServiceGrpc.ContractService
import com.daml.ledger.api.v2.contract_service.{
  ContractServiceGrpc,
  GetContractRequest,
  GetContractResponse,
}
import com.daml.ledger.api.v2.event_query_service.EventQueryServiceGrpc.EventQueryService
import com.daml.ledger.api.v2.event_query_service.{
  EventQueryServiceGrpc,
  GetEventsByContractIdRequest,
}
import com.daml.ledger.api.v2.interactive.interactive_submission_service.InteractiveSubmissionServiceGrpc.InteractiveSubmissionService
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionAndWaitForTransactionRequest,
  ExecuteSubmissionAndWaitForTransactionResponse,
  ExecuteSubmissionAndWaitRequest,
  ExecuteSubmissionAndWaitResponse,
  ExecuteSubmissionRequest,
  ExecuteSubmissionResponse,
  GetPreferredPackageVersionRequest,
  GetPreferredPackageVersionResponse,
  GetPreferredPackagesRequest,
  GetPreferredPackagesResponse,
  InteractiveSubmissionServiceGrpc,
  PrepareSubmissionRequest,
  PrepareSubmissionResponse,
}
import com.daml.ledger.api.v2.package_service.PackageServiceGrpc.PackageService
import com.daml.ledger.api.v2.package_service.{
  GetPackageRequest,
  GetPackageResponse,
  GetPackageStatusRequest,
  GetPackageStatusResponse,
  ListPackagesRequest,
  ListPackagesResponse,
  ListVettedPackagesRequest,
  ListVettedPackagesResponse,
  PackageServiceGrpc,
}
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateService
import com.daml.ledger.api.v2.state_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse,
  GetConnectedSynchronizersRequest,
  GetConnectedSynchronizersResponse,
  GetLatestPrunedOffsetsRequest,
  GetLatestPrunedOffsetsResponse,
  GetLedgerEndRequest,
  GetLedgerEndResponse,
  StateServiceGrpc,
}
import com.daml.ledger.api.v2.testing.time_service.TimeServiceGrpc
import com.daml.ledger.api.v2.testing.time_service.TimeServiceGrpc.TimeService
import com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateService
import com.daml.ledger.api.v2.update_service.{
  GetUpdateByIdRequest,
  GetUpdateByOffsetRequest,
  GetUpdateResponse,
  GetUpdatesRequest,
  GetUpdatesResponse,
  UpdateServiceGrpc,
}
import com.daml.ledger.api.v2.version_service.VersionServiceGrpc.VersionService
import com.daml.ledger.api.v2.version_service.{GetLedgerApiVersionRequest, VersionServiceGrpc}
import com.digitalasset.base.error.ErrorCategory.GenericErrorCategory
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.base.error.{
  ErrorCategory,
  ErrorCategoryRetry,
  ErrorClass,
  ErrorCode,
  ErrorResource,
  Grouping,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, Jwt}
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.http.json.v2.{
  JsCommandService,
  JsContractService,
  JsEventService,
  JsExecuteSubmissionAndWaitForTransactionRequest,
  JsExecuteSubmissionAndWaitForTransactionResponse,
  JsExecuteSubmissionAndWaitRequest,
  JsExecuteSubmissionRequest,
  JsGetActiveContractsResponse,
  JsIdentityProviderService,
  JsInteractiveSubmissionService,
  JsPackageService,
  JsPartyManagementService,
  JsPrepareSubmissionRequest,
  JsPrepareSubmissionResponse,
  JsStateService,
  JsUpdateService,
  JsUserManagementService,
  JsVersionService,
  LegacyDTOs,
  PagedList,
  ProtocolConverters,
  SchemaProcessorsImpl,
  TranscodePackageIdResolver,
}
import com.digitalasset.canton.logging.audit.TransportType.Http
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NoLogging}
import com.digitalasset.canton.networking.grpc.CallMetadata
import com.digitalasset.canton.serialization.ProtoConverter.InstantConverter
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.store.packagemeta.PackageMetadata.Implicits.packageMetadataSemigroup
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.daml.lf.archive.{DarParser, Decode}
import io.grpc.health.v1.health.HealthGrpc
import io.grpc.health.v1.health.HealthGrpc.Health
import io.grpc.stub.StreamObserver
import io.grpc.{Channel, ClientInterceptor, Status}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.util.ByteString
import org.reactivestreams.{Subscriber, Subscription}
import org.slf4j.event.Level
import sttp.capabilities
import sttp.capabilities.pekko.PekkoStreams
import sttp.client3.pekkohttp.PekkoHttpBackend
import sttp.model.StatusCode
import sttp.tapir.client.sttp.SttpClientInterpreter
import sttp.tapir.{DecodeResult, Endpoint}

import java.io.InputStream
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.ZipInputStream
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Using

object LedgerServices {
  def apply(
      participantEndpoint: Either[JsonApiEndpoint, Channel],
      commandInterceptors: Seq[ClientInterceptor],
      dars: List[String],
  )(implicit executionContext: ExecutionContext): LedgerServices = participantEndpoint
    .map(new LedgerServicesGrpc(_, commandInterceptors))
    .left
    .map { case (hostname, port) =>
      implicit val actorSystem: ActorSystem = ActorSystem("LedgerServicesJson")
      implicit val materializer: Materializer = Materializer(actorSystem)

      new LedgerServicesJson(
        hostname,
        port,
        dars,
        None,
      )
    }
    .merge
}

private final class LedgerServicesJson(
    hostname: String,
    port: Int,
    dars: List[String],
    tokenParam: Option[String],
)(implicit executionContext: ExecutionContext, mat: Materializer)
    extends LedgerServices
    with NamedLogging {

  import com.digitalasset.canton.http.util.GrpcHttpErrorCodes.`gRPC status  as sttp`

  protected def loggerFactory: NamedLoggerFactory =
    NamedLoggerFactory("client-ledger-services", "json")

  implicit val traceContext: TraceContext =
    TraceContext.createNew("ledger_services")
  private val tokenPrefix: String = "jwt.token."
  private val wsProtocol: String = "daml.ws.auth"
  implicit val token: Option[String] = tokenParam

  private val backend = PekkoHttpBackend(
    customConnectionPoolSettings = Some(
      ConnectionPoolSettings
        .apply(mat.system)
        .withMaxConnections(16)
        .withMaxOpenRequests(256)
    ),
    customizeRequest = { request =>
      logger.debug(s"JSON Request ${request.method} ${request.uri}")
      request
    },
    customizeWebsocketRequest = { request =>
      val prot = s"""$tokenPrefix${token.getOrElse("")},$wsProtocol"""
      val reqf = request.copy(
        subprotocol = Some(prot),
        extraHeaders = Seq(),
      ) // We clear extraheaders, as they contain redundant wsProtocol header
      reqf
    },
  )

  private def client[INPUT, OUTPUT](
      endpoint: sttp.tapir.Endpoint[
        CallerContext,
        INPUT,
        (StatusCode, JsCantonError),
        OUTPUT,
        sttp.capabilities.pekko.PekkoStreams & sttp.capabilities.WebSockets,
      ],
      ws: Boolean,
  ): CallerContext => INPUT => Future[DecodeResult[Either[(StatusCode, JsCantonError), OUTPUT]]] =
    SttpClientInterpreter().toSecureClient(
      endpoint,
      Some(sttp.model.Uri(if (ws) "ws" else "http", hostname, port)),
      backend,
    )(WsHelper.webSocketsSupportedForPekkoStreams)

  private def clientContext[INPUT, OUTPUT](
      endpoint: sttp.tapir.Endpoint[
        CallerContext,
        INPUT,
        (StatusCode, JsCantonError),
        OUTPUT,
        sttp.capabilities.pekko.PekkoStreams & sttp.capabilities.WebSockets,
      ],
      ws: Boolean,
  ): INPUT => Future[DecodeResult[Either[(StatusCode, JsCantonError), OUTPUT]]] =
    client(endpoint, ws)(
      CallerContext(
        jwt = token.map(Jwt.apply),
        // CallMetadata for client are not relevant, but we need to provide one nonetheless
        call = CallMetadata(endpoint.showShort, Http, Left("integration-test-client")),
      )
    )

  private def clientCall[INPUT, OUTPUT](
      endpoint: sttp.tapir.Endpoint[
        CallerContext,
        INPUT,
        (StatusCode, JsCantonError),
        OUTPUT,
        sttp.capabilities.pekko.PekkoStreams & sttp.capabilities.WebSockets,
      ],
      input: INPUT,
      ws: Boolean = false,
  ): Future[OUTPUT] =
    clientContext(endpoint, ws)(input)
      .map {
        case DecodeResult.Value(Right(value)) =>
          value
        case DecodeResult.Value(Left(jsonCallError)) =>
          val decoded: DecodedCantonError = toDecodedCantonError(jsonCallError._2)
          val grpcErrorCode = decoded.code.category.grpcCode.getOrElse(Status.UNKNOWN.getCode)
          assert(
            grpcErrorCode.asSttpStatus == jsonCallError._1,
            "returned grpcError should match http status code from response",
          )
          val sre = ErrorCode.asGrpcError(decoded)(NoLogging)
          throw sre
        case DecodeResult.Error(error, thr) =>
          throw new RuntimeException(s"error calling service $error", thr)
        case otherError =>
          throw new RuntimeException(s"unknown error calling service $otherError")
      }

  private def toDecodedCantonError(jsCantonError: JsCantonError): DecodedCantonError =
    new DecodedCantonError(
      code = GenericErrorCode(
        id = jsCantonError.code,
        category = GenericErrorCategory(
          grpcCode = jsCantonError.grpcCodeValue.map(Status.fromCodeValue).map(_.getCode),
          logLevel = Level.INFO,
          retryable = jsCantonError.retryInfo.map(duration =>
            ErrorCategoryRetry(FiniteDuration(duration.length, duration.unit))
          ),
          redactDetails = false,
          asInt = jsCantonError.errorCategory,
          // unused
          rank = 1,
        ),
      ),
      cause = jsCantonError.cause,
      correlationId = jsCantonError.correlationId,
      traceId = jsCantonError.traceId,
      context = jsCantonError.context,
      resources = jsCantonError.resources.map { case (k, v) => (ErrorResource(k), v) },
      definiteAnswerO = jsCantonError.definiteAnswer,
    )

  private def wsCall[REQ, JSRESP, RESP](
      endpoint: Endpoint[CallerContext, Unit, (StatusCode, JsCantonError), Flow[
        REQ,
        Either[JsCantonError, JSRESP],
        Any,
      ], PekkoStreams & capabilities.WebSockets],
      input: REQ,
      responseObserver: StreamObserver[RESP],
      converter: JSRESP => Future[RESP],
  ): Unit = {
    val unusedF = for {
      wsFlow <- clientCall(endpoint = endpoint, input = (), ws = true)

      sink = Sink.fromSubscriber(new Subscriber[Either[JsCantonError, RESP]]() {
        override def onSubscribe(subscription: Subscription): Unit =
          subscription.request(Long.MaxValue)

        override def onNext(t: Either[JsCantonError, RESP]): Unit =
          t match {
            case Left(cantonError) =>
              val decoded = toDecodedCantonError(cantonError)
              val err = io.grpc.protobuf.StatusProto.toStatusRuntimeException(
                com.google.rpc.status.Status
                  .toJavaProto(decoded.toRpcStatusWithForwardedRequestId)
              )
              responseObserver.onError(err)
            case Right(v) => responseObserver.onNext(v)
          }

        override def onError(t: Throwable): Unit =
          responseObserver.onError(t)

        override def onComplete(): Unit =
          responseObserver.onCompleted()
      })

      notUsed_ = Source
        .single(input)
        .via(wsFlow)
        .mapAsync(1) {
          case Left(e) => Future.successful(Left(e))
          case Right(js) => converter(js).map(Right(_))
        }
        .to(sink)
        .run()
    } yield ()

    // It is fine to discard the future here since the client implicitly
    // awaits for it to complete via the responseObserver
    unusedF.discard
  }

  private val packageMetadataView: AtomicReference[PackageMetadata] =
    new AtomicReference(PackageMetadata())

  dars.foreach(dar => addDarToPackageMetadataView(Dars.read(dar).newInput()))

  private val schemaProcessors =
    new SchemaProcessorsImpl(
      _ => packageMetadataView.get().packages,
      loggerFactory,
      allowMissingFields = true,
    )
  private val protocolConverters =
    new ProtocolConverters(
      schemaProcessors,
      // For simplicity, use the package metadata built from all the DARs expected
      // to be used in tests. This means that the package name resolution done by the conformance tests
      // ledger client will not be affected by the topology of the target participant.
      TranscodePackageIdResolver.packageMetadataBacked(
        () => packageMetadataView.get(),
        loggerFactory,
      ),
    )

  def command: CommandService = new CommandService {

    override def submitAndWait(
        request: SubmitAndWaitRequest
    ): Future[SubmitAndWaitResponse] = for {
      jsCommands <- protocolConverters.Commands.toJson(request.getCommands)
      response <- clientCall(JsCommandService.submitAndWait, jsCommands)
    } yield response

    override def submitAndWaitForTransaction(
        request: SubmitAndWaitForTransactionRequest
    ): Future[SubmitAndWaitForTransactionResponse] = for {
      jsRequest <- protocolConverters.SubmitAndWaitForTransactionRequest.toJson(request)
      jsResponse <- clientCall(JsCommandService.submitAndWaitForTransactionEndpoint, jsRequest)
      response <- protocolConverters.SubmitAndWaitTransactionResponse.fromJson(jsResponse)
    } yield response

    override def submitAndWaitForReassignment(
        request: SubmitAndWaitForReassignmentRequest
    ): Future[SubmitAndWaitForReassignmentResponse] = for {
      jsResponse <- clientCall(JsCommandService.submitAndWaitForReassignmentEndpoint, request)
      response <- protocolConverters.SubmitAndWaitForReassignmentResponse.fromJson(jsResponse)
    } yield response
  }

  def commandCompletion: CommandCompletionService = (
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ) =>
    wsCall(
      JsCommandService.completionStreamEndpoint,
      request,
      responseObserver,
      Future.successful(_: CompletionStreamResponse),
    )

  def commandSubmission: CommandSubmissionService = new CommandSubmissionService {

    override def submit(request: SubmitRequest): Future[SubmitResponse] =
      for {
        jsCommands <- protocolConverters.Commands.toJson(request.getCommands)
        resp <- clientCall(JsCommandService.submitAsyncEndpoint, jsCommands)
      } yield resp

    override def submitReassignment(
        request: SubmitReassignmentRequest
    ): Future[SubmitReassignmentResponse] =
      clientCall(JsCommandService.submitReassignmentAsyncEndpoint, request)
  }

  def health: Health = throw new UnsupportedOperationException(
    "Health is not available in JSON API"
  )

  def state: StateService = new StateService {
    override def getActiveContracts(
        request: GetActiveContractsRequest,
        responseObserver: StreamObserver[GetActiveContractsResponse],
    ): Unit = wsCall(
      JsStateService.activeContractsEndpoint,
      toGetActiveContractsRequestLegacy(request),
      responseObserver,
      (v: JsGetActiveContractsResponse) =>
        protocolConverters.GetActiveContractsResponse
          .fromJson(v),
    )

    private def toGetActiveContractsRequestLegacy(
        req: GetActiveContractsRequest
    ): LegacyDTOs.GetActiveContractsRequest =
      LegacyDTOs.GetActiveContractsRequest(
        filter = None,
        activeAtOffset = req.activeAtOffset,
        eventFormat = req.eventFormat,
      )

    override def getConnectedSynchronizers(
        request: GetConnectedSynchronizersRequest
    ): Future[GetConnectedSynchronizersResponse] =
      clientCall(
        JsStateService.getConnectedSynchronizersEndpoint,
        (
          Option(request.party).filter(_.nonEmpty),
          Option(request.participantId).filter(_.nonEmpty),
          Option(request.identityProviderId).filter(_.nonEmpty),
        ),
      )

    override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
      clientCall(JsStateService.getLedgerEndEndpoint, ())

    override def getLatestPrunedOffsets(
        request: GetLatestPrunedOffsetsRequest
    ): Future[GetLatestPrunedOffsetsResponse] =
      clientCall(JsStateService.getLastPrunedOffsetsEndpoint, ())

  }

  def partyManagement: PartyManagementService = new PartyManagementService {
    override def getParticipantId(
        request: GetParticipantIdRequest
    ): Future[GetParticipantIdResponse] =
      clientCall(JsPartyManagementService.getParticipantIdEndpoint, ())

    override def getParties(request: GetPartiesRequest): Future[GetPartiesResponse] =
      clientCall(
        JsPartyManagementService.getPartyEndpoint,
        (
          request.parties.headOption.getOrElse(
            sys.error(
              s"Expected at least a party in the ${classOf[GetPartiesRequest].getSimpleName}"
            )
          ),
          Some(request.identityProviderId),
          request.parties.drop(1).toList,
        ),
      )

    override def listKnownParties(
        request: ListKnownPartiesRequest
    ): Future[ListKnownPartiesResponse] =
      clientCall(
        JsPartyManagementService.listKnownPartiesEndpoint,
        PagedList(
          (None, None): (Option[String], Option[String]),
          Some(request.pageSize),
          Some(request.pageToken),
        ),
      )

    override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] =
      clientCall(JsPartyManagementService.allocatePartyEndpoint, request)

    override def updatePartyDetails(
        request: UpdatePartyDetailsRequest
    ): Future[UpdatePartyDetailsResponse] =
      clientCall(
        JsPartyManagementService.updatePartyEndpoint,
        (request.getPartyDetails.party, request),
      )

    override def updatePartyIdentityProviderId(
        request: UpdatePartyIdentityProviderIdRequest
    ): Future[UpdatePartyIdentityProviderIdResponse] = throw new UnsupportedOperationException(
      "updatePartyIdentityProviderIs is not available in JSON API"
    )

    override def allocateExternalParty(
        request: AllocateExternalPartyRequest
    ): Future[AllocateExternalPartyResponse] = clientCall(
      JsPartyManagementService.allocateExternalPartyEndpoint,
      request,
    )

    override def generateExternalPartyTopology(
        request: GenerateExternalPartyTopologyRequest
    ): Future[GenerateExternalPartyTopologyResponse] =
      clientCall(JsPartyManagementService.externalPartyGenerateTopologyEndpoint, request)

  }

  def packageManagement: PackageManagementService = new PackageManagementService {
    override def listKnownPackages(
        request: ListKnownPackagesRequest
    ): Future[ListKnownPackagesResponse] =
      throw new UnsupportedOperationException(
        "PackageManagement listKnownPackages is not available in JSON API"
      )

    override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] = {
      val src: Source[ByteString, NotUsed] = Source.fromIterator(() =>
        request.darFile
          .asReadOnlyByteBufferList()
          .iterator
          .asScala
          .map(org.apache.pekko.util.ByteString(_))
      )
      clientCall(
        JsPackageService.uploadDar,
        (
          src,
          request.vettingChange match {
            case UploadDarFileRequest.VettingChange.VETTING_CHANGE_UNSPECIFIED =>
              None
            case UploadDarFileRequest.VettingChange.VETTING_CHANGE_VET_ALL_PACKAGES =>
              Some(true)
            case UploadDarFileRequest.VettingChange.VETTING_CHANGE_DONT_VET_ANY_PACKAGES =>
              Some(false)
            case UploadDarFileRequest.VettingChange.Unrecognized(unrecognizedValue) =>
              throw new IllegalArgumentException(
                s"could not convert unrecognized VettingChange enum $unrecognizedValue to a boolean"
              )
          },
          OptionUtil.emptyStringAsNone(request.synchronizerId),
        ),
      )
    }.map { result =>
      Using(request.darFile.newInput()) {
        addDarToPackageMetadataView
      }
      result
    }

    override def validateDarFile(
        request: ValidateDarFileRequest
    ): Future[ValidateDarFileResponse] = {
      val src: Source[ByteString, NotUsed] = Source.fromIterator(() =>
        request.darFile
          .asReadOnlyByteBufferList()
          .iterator
          .asScala
          .map(org.apache.pekko.util.ByteString(_))
      )
      clientCall(
        JsPackageService.validateDar,
        (
          src,
          OptionUtil.emptyStringAsNone(request.synchronizerId),
        ),
      )
    }.map(_ => ValidateDarFileResponse())

    override def updateVettedPackages(
        request: UpdateVettedPackagesRequest
    ): Future[UpdateVettedPackagesResponse] =
      clientCall(JsPackageService.updateVettedPackagesEndpoint, request)
  }

  private def addDarToPackageMetadataView(inputStream: InputStream): Unit =
    Using(new ZipInputStream(inputStream)) { zip =>
      (for {
        archive <- DarParser.readArchive("Uploaded DAR", zip)
        pkgs <- archive.all.traverse(Decode.decodeArchive)
      } yield {
        pkgs.map { case (pkgId, pkg) => PackageMetadata.from(pkgId, pkg) }.foreach {
          newPackageMetadata =>
            packageMetadataView.updateAndGet(_ |+| newPackageMetadata).discard
        }
      }).fold(
        err => {
          logger.error("Could not load a requested DAR", err)
          throw err
        },
        identity,
      )
    }

  def participantPruning: ParticipantPruningService =
    throw new UnsupportedOperationException(
      "ParticipantPruningService is not available in JSON API"
    )

  def packages: PackageService = new PackageService {

    override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
      clientCall(JsPackageService.listPackagesEndpoint, ())

    override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
      for {
        body <- clientCall(JsPackageService.downloadPackageEndpoint, request.packageId)
        bytes <- body._1.runFold(ByteString.empty)(_ ++ _)
      } yield GetPackageResponse(
        hashFunction = com.daml.ledger.api.v2.package_service.HashFunction.HASH_FUNCTION_SHA256,
        archivePayload = com.google.protobuf.ByteString.copyFrom(bytes.toArray),
        hash = body._2,
      )

    override def getPackageStatus(
        request: GetPackageStatusRequest
    ): Future[GetPackageStatusResponse] =
      clientCall(JsPackageService.packageStatusEndpoint, request.packageId)

    override def listVettedPackages(
        request: ListVettedPackagesRequest
    ): Future[ListVettedPackagesResponse] =
      clientCall(JsPackageService.listVettedPackagesEndpoint, request)
  }

  def update: UpdateService = new UpdateService {

    override def getUpdates(
        request: GetUpdatesRequest,
        responseObserver: StreamObserver[GetUpdatesResponse],
    ): Unit =
      wsCall(
        JsUpdateService.getUpdatesEndpoint,
        toGetUpdatesRequestLegacy(request),
        responseObserver,
        protocolConverters.GetUpdatesResponse.fromJson,
      )

    private def toGetUpdatesRequestLegacy(
        req: GetUpdatesRequest
    ): LegacyDTOs.GetUpdatesRequest =
      LegacyDTOs.GetUpdatesRequest(
        beginExclusive = req.beginExclusive,
        endInclusive = req.endInclusive,
        filter = None,
        updateFormat = req.updateFormat,
      )

    override def getUpdateByOffset(
        request: GetUpdateByOffsetRequest
    ): Future[GetUpdateResponse] = clientCall(
      JsUpdateService.getUpdateByOffsetEndpoint,
      request,
    )
      .flatMap(protocolConverters.GetUpdateResponse.fromJson)

    override def getUpdateById(
        request: GetUpdateByIdRequest
    ): Future[GetUpdateResponse] =
      clientCall(
        JsUpdateService.getUpdateByIdEndpoint,
        request,
      )
        .flatMap(protocolConverters.GetUpdateResponse.fromJson)
  }

  def eventQuery: EventQueryService = (request: GetEventsByContractIdRequest) =>
    clientCall(
      JsEventService.getEventsByContractIdEndpoint,
      request,
    )
      .flatMap(protocolConverters.GetEventsByContractIdResponse.fromJson)

  def time: TimeService = throw new UnsupportedOperationException(
    "TimeService is not available in JSON API"
  )

  def version: VersionService = (_: GetLedgerApiVersionRequest) =>
    clientCall(JsVersionService.versionEndpoint, ())

  def userManagement: UserManagementService = new UserManagementService {
    override def createUser(request: CreateUserRequest): Future[CreateUserResponse] =
      clientCall(JsUserManagementService.createUserEndpoint, request)

    override def getUser(request: GetUserRequest): Future[GetUserResponse] =
      if (request.userId.isEmpty) {
        clientCall(
          JsUserManagementService.getCurrentUserEndpoint,
          Some(request.identityProviderId),
        )
      } else {
        clientCall(
          JsUserManagementService.getUserEndpoint,
          (request.userId, Some(request.identityProviderId)),
        )
      }

    override def updateUser(request: UpdateUserRequest): Future[UpdateUserResponse] =
      clientCall(JsUserManagementService.updateUserEndpoint, (request.getUser.id, request))

    override def deleteUser(request: DeleteUserRequest): Future[DeleteUserResponse] =
      clientCall(JsUserManagementService.deleteUserEndpoint, request.userId).map(_ =>
        DeleteUserResponse()
      )

    override def listUsers(request: ListUsersRequest): Future[ListUsersResponse] =
      clientCall(
        JsUserManagementService.listUsersEndpoint,
        PagedList((), Some(request.pageSize), Some(request.pageToken)),
      )

    override def grantUserRights(request: GrantUserRightsRequest): Future[GrantUserRightsResponse] =
      clientCall(JsUserManagementService.grantUserRightsEndpoint, (request.userId, request))

    override def revokeUserRights(
        request: RevokeUserRightsRequest
    ): Future[RevokeUserRightsResponse] =
      clientCall(JsUserManagementService.revokeUserRightsEndpoint, (request.userId, request))

    override def listUserRights(request: ListUserRightsRequest): Future[ListUserRightsResponse] =
      clientCall(JsUserManagementService.listUserRightsEndpoint, request.userId)

    override def updateUserIdentityProviderId(
        request: UpdateUserIdentityProviderIdRequest
    ): Future[UpdateUserIdentityProviderIdResponse] =
      clientCall(
        JsUserManagementService.updateUserIdentityProviderEndpoint,
        (request.userId, request),
      )

  }

  def identityProviderConfig: IdentityProviderConfigService = new IdentityProviderConfigService {

    override def createIdentityProviderConfig(
        request: CreateIdentityProviderConfigRequest
    ): Future[CreateIdentityProviderConfigResponse] =
      clientCall(JsIdentityProviderService.createIdpsEndpoint, request)

    override def getIdentityProviderConfig(
        request: GetIdentityProviderConfigRequest
    ): Future[GetIdentityProviderConfigResponse] =
      clientCall(JsIdentityProviderService.getIdpEndpoint, request.identityProviderId)

    override def updateIdentityProviderConfig(
        request: UpdateIdentityProviderConfigRequest
    ): Future[UpdateIdentityProviderConfigResponse] =
      clientCall(
        JsIdentityProviderService.updateIdpEndpoint,
        (request.getIdentityProviderConfig.identityProviderId, request),
      )

    override def listIdentityProviderConfigs(
        request: ListIdentityProviderConfigsRequest
    ): Future[ListIdentityProviderConfigsResponse] =
      clientCall(JsIdentityProviderService.listIdpsEndpoint, ())

    override def deleteIdentityProviderConfig(
        request: DeleteIdentityProviderConfigRequest
    ): Future[DeleteIdentityProviderConfigResponse] =
      clientCall(JsIdentityProviderService.deleteIdpEndpoint, request.identityProviderId)
  }

  override def interactiveSubmission: InteractiveSubmissionService =
    new InteractiveSubmissionService {
      override def prepareSubmission(
          request: PrepareSubmissionRequest
      ): Future[PrepareSubmissionResponse] = for {
        jsPrepareRequest <- protocolConverters.PrepareSubmissionRequest.toJson(request)
        response <- clientCall[JsPrepareSubmissionRequest, JsPrepareSubmissionResponse](
          JsInteractiveSubmissionService.prepareEndpoint,
          jsPrepareRequest,
        ).flatMap { jsResponse =>
          protocolConverters.PrepareSubmissionResponse.fromJson(jsResponse)
        }
      } yield response

      override def executeSubmission(
          request: ExecuteSubmissionRequest
      ): Future[ExecuteSubmissionResponse] =
        for {
          jsExecuteRequest <- protocolConverters.ExecuteSubmissionRequest.toJson(request)
          response <- clientCall[JsExecuteSubmissionRequest, ExecuteSubmissionResponse](
            JsInteractiveSubmissionService.executeEndpoint,
            jsExecuteRequest,
          )
        } yield response

      override def getPreferredPackages(
          request: GetPreferredPackagesRequest
      ): Future[GetPreferredPackagesResponse] =
        clientCall(
          JsInteractiveSubmissionService.preferredPackagesEndpoint,
          request,
        )

      override def getPreferredPackageVersion(
          request: GetPreferredPackageVersionRequest
      ): Future[GetPreferredPackageVersionResponse] =
        clientCall(
          JsInteractiveSubmissionService.preferredPackageVersionEndpoint,
          (
            request.parties.toList,
            request.packageName,
            request.vettingValidAt.map(
              InstantConverter
                .fromProtoPrimitive(_)
                .getOrElse(
                  throw new IllegalArgumentException(
                    s"could not transform ${request.vettingValidAt} to an Instant"
                  )
                )
            ),
            Option(request.synchronizerId).filter(_.nonEmpty),
          ),
        )

      override def executeSubmissionAndWait(
          request: ExecuteSubmissionAndWaitRequest
      ): Future[ExecuteSubmissionAndWaitResponse] =
        for {
          jsExecuteRequest <- protocolConverters.ExecuteSubmissionAndWaitRequest.toJson(request)
          response <- clientCall[
            JsExecuteSubmissionAndWaitRequest,
            ExecuteSubmissionAndWaitResponse,
          ](
            JsInteractiveSubmissionService.executeAndWaitEndpoint,
            jsExecuteRequest,
          )
        } yield response

      override def executeSubmissionAndWaitForTransaction(
          request: ExecuteSubmissionAndWaitForTransactionRequest
      ): Future[ExecuteSubmissionAndWaitForTransactionResponse] =
        for {
          jsExecuteRequest <- protocolConverters.ExecuteSubmissionAndWaitForTransactionRequest
            .toJson(request)
          response <- clientCall[
            JsExecuteSubmissionAndWaitForTransactionRequest,
            JsExecuteSubmissionAndWaitForTransactionResponse,
          ](
            JsInteractiveSubmissionService.executeAndWaitForTransactionEndpoint,
            jsExecuteRequest,
          )
          grpcResponse <- protocolConverters.ExecuteSubmissionAndWaitForTransactionResponse
            .fromJson(response)
        } yield grpcResponse
    }

  def contract: ContractService = new ContractService {
    override def getContract(request: GetContractRequest): Future[GetContractResponse] =
      clientCall(JsContractService.getContractEndpoint, request)
        .flatMap(protocolConverters.GetContractResponse.fromJson)
  }
}

sealed trait LedgerServices {
  def command: CommandService
  def commandCompletion: CommandCompletionService
  def commandSubmission: CommandSubmissionService
  def health: Health
  def interactiveSubmission: InteractiveSubmissionService
  def state: StateService
  def partyManagement: PartyManagementService
  def packageManagement: PackageManagementService
  def participantPruning: ParticipantPruningService
  def packages: PackageService
  def update: UpdateService
  def eventQuery: EventQueryService
  def time: TimeService
  def version: VersionService
  def userManagement: UserManagementService
  def identityProviderConfig: IdentityProviderConfigService
  def contract: ContractService
}

private final class LedgerServicesGrpc(
    channel: Channel,
    commandInterceptors: Seq[ClientInterceptor],
) extends LedgerServices {

  val command: CommandService =
    CommandServiceGrpc.stub(channel).withInterceptors(commandInterceptors*)

  val commandCompletion: CommandCompletionService =
    CommandCompletionServiceGrpc.stub(channel)

  val commandSubmission: CommandSubmissionService =
    CommandSubmissionServiceGrpc.stub(channel).withInterceptors(commandInterceptors*)

  val health: Health =
    HealthGrpc.stub(channel)

  val state: StateService =
    StateServiceGrpc.stub(channel)

  val partyManagement: PartyManagementService =
    PartyManagementServiceGrpc.stub(channel)

  val packageManagement: PackageManagementService =
    PackageManagementServiceGrpc.stub(channel)

  val participantPruning: ParticipantPruningService =
    ParticipantPruningServiceGrpc.stub(channel)

  val packages: PackageService =
    PackageServiceGrpc.stub(channel)

  val update: UpdateService =
    UpdateServiceGrpc.stub(channel)

  val eventQuery: EventQueryService =
    EventQueryServiceGrpc.stub(channel)

  val time: TimeService =
    TimeServiceGrpc.stub(channel)

  val version: VersionService =
    VersionServiceGrpc.stub(channel)

  val userManagement: UserManagementService =
    UserManagementServiceGrpc.stub(channel)

  val identityProviderConfig: IdentityProviderConfigService =
    IdentityProviderConfigServiceGrpc.stub(channel)

  val interactiveSubmission: InteractiveSubmissionService =
    InteractiveSubmissionServiceGrpc.stub(channel)

  val contract: ContractService =
    ContractServiceGrpc.stub(channel)
}

object JsonErrors {
  implicit val genericErrorClass: ErrorClass = ErrorClass(
    List(Grouping("generic", "ErrorClass"))
  )

  final case class GenericErrorCode(
      override val id: String,
      override val category: ErrorCategory,
  ) extends ErrorCode(id, category)
}
