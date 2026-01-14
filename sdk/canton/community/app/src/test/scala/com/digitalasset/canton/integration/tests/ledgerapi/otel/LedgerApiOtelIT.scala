// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.otel

import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v2.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v2.commands.CreateCommand
import com.daml.ledger.api.v2.state_service.{GetLedgerEndRequest, StateServiceGrpc}
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.update_service.*
import com.daml.ledger.api.v2.value.{Record, RecordField, Value}
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.config.AuthServiceConfig.Wildcard
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.config.{CantonConfig, PemFile, TlsServerConfig}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UseOtlp}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.ValueConversions.*
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.{CantonFixture, CreatesParties}
import com.digitalasset.canton.integration.tests.ledgerapi.services.TestCommands
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.ledger.api.MockMessages
import com.digitalasset.canton.logging.NamedLogging.loggerWithoutTracing
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.tracing.Spanning.SpanWrapper
import com.digitalasset.canton.tracing.{
  SerializableTraceContextConverter,
  Spanning,
  TraceContext,
  TraceContextGrpc,
}
import io.grpc.stub.StreamObserver
import monocle.macros.syntax.lens.*
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Assertion, Succeeded}
import org.slf4j.event.Level

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

trait LedgerApiOtelITBase
    extends CantonFixture
    with CreatesParties
    with TestCommands
    with Spanning {

  val otlpHeaders = Map("custom-key" -> "custom-value")

  registerPlugin(LedgerApiOtelOverrideConfig(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  protected def useOtlp: UseOtlp
  registerPlugin(useOtlp)

  implicit override val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(60, Seconds)), interval = scaled(Span(150, Millis)))

  protected def command(party: String) =
    CreateCommand(
      Some(templateIds.dummy),
      Some(
        Record(
          Some(templateIds.dummy),
          Seq(RecordField("operator", Option(Value(Value.Sum.Party(party))))),
        )
      ),
    ).wrap

  protected def submitAndWaitRequest(party: String, userId: String) =
    MockMessages.submitAndWaitRequest
      .update(
        _.commands.commands := List(command(party)),
        _.commands.commandId := UUID.randomUUID().toString,
        _.commands.actAs := Seq(party),
        _.commands.userId := userId,
      )

  protected def submitAndWaitRequestForTransaction(party: String, userId: String) =
    MockMessages.submitAndWaitForTransactionRequest
      .update(
        _.commands.commands := List(command(party)),
        _.commands.commandId := UUID.randomUUID().toString,
        _.commands.actAs := Seq(party),
        _.commands.userId := userId,
        _.transactionFormat := TransactionFormat(
          eventFormat = Some(
            EventFormat(
              filtersByParty = Map(party -> Filters(Nil)),
              filtersForAnyParty = None,
              verbose = false,
            )
          ),
          transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
        ),
      )

  protected def submitRequest(party: String, userId: String) =
    MockMessages.submitRequest
      .update(
        _.commands.commands := List(command(party)),
        _.commands.commandId := UUID.randomUUID().toString,
        _.commands.actAs := Seq(party),
        _.commands.userId := userId,
      )

  protected val partyHint = MockMessages.submitRequest.getCommands.actAs.headOption.getOrElse("")

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPrerequisiteParties(None, List(partyHint))(
      directExecutionContext
    )
  }

  val OTLPHeadersSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("HeaderPrinter") &&
      SuppressionRule.Level(Level.INFO)

  val CommandSubmissionSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("CommandSubmissionServiceImpl") &&
      SuppressionRule.Level(Level.INFO)

  private def assertLogs(expected: Seq[(Option[String], Level, String)]): Assertion = {
    val logEntries = loggerFactory.fetchRecordedLogEntries
    logEntries
      .map(entry =>
        (entry.mdc.get("trace-id"), entry.level, entry.message.takeWhile(_ != '('))
      ) should contain theSameElementsInOrderAs expected
    Succeeded
  }

  protected def assertSpans(
      traceContext: TraceContext,
      span: SpanWrapper,
      expectedNames: Seq[String],
  ): Assertion = {
    val traceId = traceContext.traceId.getOrElse(
      throw new IllegalArgumentException("Missing traceId in context")
    )
    eventually() {
      val filtered = useOtlp.getSpans
        .filter(_.traceId == traceId)
      filtered should not be empty
      val names = filtered.map(_.name)
      expectedNames.foreach(names should contain(_))
      filtered.exists(_.parentSpanId == span.getSpanId) should be(true)
    }
    Succeeded
  }

  private def assertStreamMessages[StreamResponse](
      traceContexts: List[TraceContext],
      streamRequester: StreamObserver[StreamResponse] => Unit,
      extractTraceContext: StreamResponse => Seq[TraceContext],
      filterRelevantResponses: StreamResponse => Boolean,
  )(implicit ec: ExecutionContext): Future[Assertion] = {
    val givenTraceIds = traceContexts.flatMap(_.traceId)
    logger.info(s"given: $givenTraceIds")
    new StreamConsumer[StreamResponse](streamRequester)
      .filterTake(filterRelevantResponses)(traceContexts.size)
      .map(_.flatMap(extractTraceContext).flatMap(_.traceId))
      .map { returnedTraceIds =>
        logger.info(s"returned: $returnedTraceIds")
        forAll(givenTraceIds)(returnedTraceIds should contain(_))
      }
  }

  protected def userId: String = UUID.randomUUID().toString

  def testCommandService[Response](
      submit: String => Future[Response],
      origin: String,
  ): Unit =
    s"report span to otlp server and log from $origin" in { env =>
      import env.*
      val party = participant1.parties.find(partyHint).toProtoPrimitive
      withNewTrace(origin) { traceContext => span =>
        loggerFactory.suppress(CommandSubmissionSuppressionRule) {
          TraceContextGrpc
            .withGrpcContext(traceContext)(
              submit(party)
            )
            .transform(Success.apply)
            .map { _ =>
              assertLogs(
                Seq(
                  (
                    traceContext.traceId,
                    Level.INFO,
                    "Phase 1 started: Submitting commands for interpretation: Commands",
                  )
                )
              )
              assertSpans(
                traceContext,
                span,
                Seq(
                  origin,
                  "ApiSubmissionService.evaluate",
                  "CantonSyncService.submitTransaction",
                  "ConfirmationRequestAndResponseProcessor.processRequest",
                  "ConfirmationRequestAndResponseProcessor.processResponse",
                  "Indexer.mapInput",
                ),
              )
            }
        }
      }.futureValue
    }

  def testStreamingService[StreamResponse](
      submissionService: => CommandService,
      ledgerEndRequester: ExecutionContext => Future[Long],
      streamRequester: (String, Long) => StreamObserver[StreamResponse] => Unit,
      extractTraceContext: StreamResponse => Seq[TraceContext],
      origin: String,
      userId: String,
      filterRelevantResponses: StreamResponse => Boolean,
  ): Unit =
    s"receive trace context through $origin" in { env =>
      import env.*
      val party = participant1.parties.find(partyHint).toProtoPrimitive
      (for {
        offset <- ledgerEndRequester(executionContext)
        tcs <- Future.sequence(
          List
            .fill(2)(())
            .map(_ =>
              withNewTrace(origin) { traceContext => _ =>
                TraceContextGrpc
                  .withGrpcContext(traceContext)(
                    submissionService
                      .submitAndWait(
                        submitAndWaitRequest(party, userId)
                      )
                  )
                  .map(_ => traceContext)
              }
            )
        )
        _ <- assertStreamMessages(
          traceContexts = tcs,
          streamRequester = streamRequester(party, offset),
          extractTraceContext = extractTraceContext,
          filterRelevantResponses = filterRelevantResponses,
        )
      } yield succeed).futureValue
    }

  def testPointwiseQuery[Response](
      submissionService: => CommandService,
      query: (String, String) => Future[Response],
      extractTraceContext: Response => TraceContext,
      origin: String,
      userId: String,
  ): Unit =
    s"receive trace context through $origin" in { env =>
      import env.*
      val party = participant1.parties.find(partyHint).toProtoPrimitive
      (for {
        (tId, submissionTc) <- withNewTrace(origin) { traceContext => _ =>
          TraceContextGrpc
            .withGrpcContext(traceContext)(
              submissionService
                .submitAndWait(
                  submitAndWaitRequest(party, userId)
                )
            )
            .map(resp => (resp.updateId, traceContext))
        }
        transactionTc <- query(tId, party).map(extractTraceContext)
      } yield {
        submissionTc === transactionTc
      }).futureValue
    }
}

class LedgerApiOtelIT extends LedgerApiOtelITBase {

  protected override lazy val useOtlp: UseOtlp =
    new UseOtlp(
      port = UniquePortGenerator.next,
      loggerFactory = loggerFactory,
    )

  "CommandSubmissionService" when {

    def submissionService = CommandSubmissionServiceGrpc
      .stub(channel)
      .withInterceptors(TraceContextGrpc.clientInterceptor())

    "receiving a command with a span and trace" should {
      testCommandService(
        submit = { party =>
          submissionService
            .submit(
              submitRequest(party, userId)
            )
        },
        origin = "com.daml.ledger.api.v2.CommandSubmissionService/Submit",
      )
    }
  }

  "CommandService" when {
    def commandService =
      CommandServiceGrpc.stub(channel).withInterceptors(TraceContextGrpc.clientInterceptor())

    "receiving a command with a span and trace" should {
      testCommandService(
        submit = { party =>
          commandService
            .submitAndWait(
              submitAndWaitRequest(party, userId)
            )
        },
        origin = "com.daml.ledger.api.v2.CommandService/SubmitAndWait",
      )

      testCommandService(
        submit = { party =>
          commandService
            .submitAndWaitForTransaction(
              submitAndWaitRequestForTransaction(party, userId)
            )
        },
        origin = "com.daml.ledger.api.v2.CommandService/SubmitAndWaitForTransaction",
      )
    }
  }

  "CommandCompletionService" when {

    def submissionService = CommandServiceGrpc
      .stub(channel)
      .withInterceptors(TraceContextGrpc.clientInterceptor())

    def completionService = CommandCompletionServiceGrpc
      .stub(channel)
      .withInterceptors(TraceContextGrpc.clientInterceptor())

    def stateService =
      StateServiceGrpc.stub(channel).withInterceptors(TraceContextGrpc.clientInterceptor())

    def extractTraceContext(response: CompletionStreamResponse): Seq[TraceContext] =
      response.completionResponse.completion
        .map(c =>
          SerializableTraceContextConverter
            .fromDamlProtoSafeOpt(loggerWithoutTracing(logger))(c.traceContext)
            .traceContext
        )
        .toList

    val aUserId = userId

    "handling a completion with a span and trace" should {
      testStreamingService[CompletionStreamResponse](
        submissionService,
        stateService
          .getLedgerEnd(GetLedgerEndRequest())
          .map(_.offset)(_),
        (party, offset) =>
          completionService.completionStream(
            CompletionStreamRequest(aUserId, Seq(party), offset),
            _,
          ),
        extractTraceContext,
        origin = "com.daml.ledger.api.v2.CommandCompletionService/Subscribe",
        userId = aUserId,
        filterRelevantResponses =
          // Filter out offset checkpoints as they are not directly triggered by the command submission,
          // hence not relevant in asserting spans/traces
          !_.completionResponse.isOffsetCheckpoint,
      )
    }
  }

  "UpdateService" when {
    val svcName = "com.daml.ledger.api.v2.UpdateService"

    def submissionService = CommandServiceGrpc
      .stub(channel)
      .withInterceptors(TraceContextGrpc.clientInterceptor())

    def stateService =
      StateServiceGrpc.stub(channel).withInterceptors(TraceContextGrpc.clientInterceptor())

    def updateService =
      UpdateServiceGrpc.stub(channel).withInterceptors(TraceContextGrpc.clientInterceptor())

    def extractTraceContextFromUpdates(response: GetUpdatesResponse): Seq[TraceContext] =
      response.update.transaction
        .map(c =>
          SerializableTraceContextConverter
            .fromDamlProtoSafeOpt(loggerWithoutTracing(logger))(c.traceContext)
            .traceContext
        )
        .toList

    def extractTraceContextFromPointwiseUpdate(
        response: GetUpdateResponse
    ): TraceContext =
      SerializableTraceContextConverter
        .fromDamlProtoSafeOpt(loggerWithoutTracing(logger))(
          response.update.transaction.flatMap(_.traceContext)
        )
        .traceContext

    val aUserId = userId

    def updateFormat(party: String) = Some(
      UpdateFormat(
        includeTransactions = Some(
          TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = Map(party -> Filters.defaultInstance),
                filtersForAnyParty = None,
                verbose = false,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
          )
        ),
        includeReassignments = None,
        includeTopologyEvents = None,
      )
    )

    "retrieving updates with a span and trace" should {
      testStreamingService[GetUpdatesResponse](
        submissionService,
        stateService
          .getLedgerEnd(GetLedgerEndRequest())
          .map(_.offset)(_),
        (party, offset) =>
          updateService.getUpdates(
            GetUpdatesRequest(
              beginExclusive = offset,
              endInclusive = None,
              updateFormat = updateFormat(party),
            ),
            _,
          ),
        extractTraceContextFromUpdates,
        origin = s"$svcName/getUpdates",
        userId = aUserId,
        filterRelevantResponses =
          // Filter out offset checkpoints as they are not triggered by the command submission,
          // hence not relevant in asserting spans/traces
          !_.update.isOffsetCheckpoint,
      )
      testPointwiseQuery(
        submissionService,
        (tId, party) =>
          updateService.getUpdateById(
            GetUpdateByIdRequest(
              updateId = tId,
              updateFormat = updateFormat(party),
            )
          ),
        extractTraceContextFromPointwiseUpdate,
        s"$svcName/getTransactionById",
        aUserId,
      )
    }
  }
}

class LedgerApiOtelTlsIT extends LedgerApiOtelITBase {

  protected override lazy val useOtlp: UseOtlp = {
    val pathPrefix = "./community/app/src/test/resources/tls"
    val certChainFile = PemFile(ExistingFile.tryCreate(s"$pathPrefix/ledger-api.crt"))
    val privateKeyFile = PemFile(ExistingFile.tryCreate(s"$pathPrefix/ledger-api.pem"))
    val trustCertCollectionFile = PemFile(ExistingFile.tryCreate(s"$pathPrefix/root-ca.crt"))

    new UseOtlp(
      port = UniquePortGenerator.next,
      loggerFactory = loggerFactory,
      trustCollectionPath = Some(s"$pathPrefix/root-ca.crt"),
      tls = Some(
        TlsServerConfig(
          certChainFile = certChainFile,
          privateKeyFile = privateKeyFile,
          trustCollectionFile = Some(trustCertCollectionFile),
        )
      ),
      otlpHeaders = otlpHeaders,
    )
  }

  def testOTLPHeader[Response](
      submit: String => Future[Response],
      origin: String,
  ): Unit =
    s"otlp server call contains required headers from $origin" in { env =>
      import env.*
      val party = participant1.parties.find(partyHint).toProtoPrimitive
      val otlpHeadersRendered = otlpHeaders.map(kv => s"${kv._1}=${kv._2}").mkString(",")
      withNewTrace(origin) { traceContext => span =>
        loggerFactory
          .assertLogsSeq(OTLPHeadersSuppressionRule)(
            TraceContextGrpc
              .withGrpcContext(traceContext)(
                submit(party)
              )
              .transform(Success.apply)
              .map { _ =>
                assertSpans(
                  traceContext,
                  span,
                  Seq(
                    origin,
                    "ApiSubmissionService.evaluate",
                    "CantonSyncService.submitTransaction",
                    "ConfirmationRequestAndResponseProcessor.processRequest",
                    "ConfirmationRequestAndResponseProcessor.processResponse",
                    "Indexer.mapInput",
                  ),
                )
              },
            _.exists(_.message.contains(otlpHeadersRendered)) should be(true),
          )
          .futureValue
      }
    }

  "CommandSubmissionService" when {

    def submissionService = CommandSubmissionServiceGrpc
      .stub(channel)
      .withInterceptors(TraceContextGrpc.clientInterceptor())

    "OTLP headers have been transferred" should {
      testOTLPHeader(
        submit = { party =>
          submissionService
            .submit(
              submitRequest(party, userId)
            )
        },
        origin = "com.daml.ledger.api.v2.CommandSubmissionService/Submit",
      )
    }
  }
}

//  plugin to override the configuration
final case class LedgerApiOtelOverrideConfig(
    protected val loggerFactory: NamedLoggerFactory
) extends EnvironmentSetupPlugin {
  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    ConfigTransforms
      .updateParticipantConfig("participant1")(
        _.focus(_.testingTime)
          .replace(None)
          .focus(_.ledgerApi.authServices)
          .replace(Seq(Wildcard))
      )(config)
}
