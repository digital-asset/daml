// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.store.platform

import java.time.{Duration, Instant}
import java.net.URLEncoder
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import akka.pattern.ask
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Scheduler, Stash}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.digitalasset.navigator.util.RetryHelper
import com.digitalasset.navigator.model._
import com.digitalasset.navigator.store.Store._
import com.digitalasset.navigator.time._
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
  TlsConfiguration
}
import com.digitalasset.ledger.client.services.testing.time.StaticTime
import com.digitalasset.ledger.api.refinements.{ApiTypes, IdGenerator}
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.navigator.ApplicationInfo
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.grpc.{ManagedChannel, Status}
import io.netty.handler.ssl.SslContext
import org.slf4j.LoggerFactory

import scala.util.{Failure, Random, Success, Try}
import scala.concurrent.Future
import scala.concurrent.duration._

object PlatformStore {
  def props(
      platformHost: String,
      platformPort: Int,
      tlsConfig: Option[TlsConfiguration],
      timeProviderType: TimeProviderType,
      applicationInfo: ApplicationInfo
  ): Props =
    Props(
      classOf[PlatformStore],
      platformHost,
      platformPort,
      tlsConfig,
      timeProviderType,
      applicationInfo)

  type PlatformTime = Instant

  case class ConnectionResult(
      ledgerClient: LedgerClient,
      staticTime: Option[StaticTime],
      time: TimeProviderWithType)
  case class Connected(ledgerClient: Try[ConnectionResult])

  case class StateConnected(
      ledgerClient: LedgerClient,
      parties: List[PartyState],
      staticTime: Option[StaticTime],
      time: TimeProviderWithType
  )
  case class StateInitial(parties: List[PartyState])
  case class StateFailed(error: Throwable)
}

/** Store implementation that accesses the platform API to fetch data and execute commands. */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class PlatformStore(
    platformHost: String,
    platformPort: Int,
    tlsConfig: Option[TlsConfiguration],
    timeProviderType: TimeProviderType,
    applicationInfo: ApplicationInfo
) extends Actor
    with ActorLogging
    with Stash {

  // ----------------------------------------------------------------------------------------------
  // Global immutable state - mutable state is stored in parameters of the receive methods
  // ----------------------------------------------------------------------------------------------
  private val system = context.system

  implicit val s: Scheduler = system.scheduler
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val esf: ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool("esf-" + this.getClass.getSimpleName)(system)

  private val applicationId = applicationInfo.id + " #" + new Random(System.currentTimeMillis())
    .nextLong()
    .toHexString

  private[this] def userFacingLogger = LoggerFactory.getLogger("user-facing-logs")

  import PlatformStore._

  // ----------------------------------------------------------------------------------------------
  // Lifecycle
  // ----------------------------------------------------------------------------------------------
  override def preStart(): Unit = {
    connect()
  }

  override def postStop(): Unit = {
    esf.close()
  }

  // ----------------------------------------------------------------------------------------------
  // Messages
  // ----------------------------------------------------------------------------------------------
  override def receive: Receive = connecting(StateInitial(List.empty[PartyState]))

  def connecting(state: StateInitial): Receive = {
    case Connected(Success(value)) =>
      context.become(
        connected(StateConnected(value.ledgerClient, state.parties, value.staticTime, value.time)))
      unstashAll

      state.parties.foreach(party => startPartyActor(value.ledgerClient, party))

    case Connected(Failure(e)) =>
      // Connection failed even after several retries - not sure how to recover from this
      val message = s"Permanently failed to connect to the ledger at $platformHost:$platformPort. " +
        "Please fix any issues and restart this application."
      userFacingLogger.error(message)
      context.become(failed(StateFailed(e)))

    case GetApplicationStateInfo =>
      sender ! ApplicationStateConnecting(
        platformHost,
        platformPort,
        tlsConfig.exists(_.enabled),
        applicationId)

    case _ =>
      stash
  }

  def connected(state: StateConnected): Receive = {
    case Subscribe(party) =>
      startPartyActor(state.ledgerClient, party)
      context.become(connected(state.copy(parties = party :: state.parties)))

    case CreateContract(party, templateId, value) =>
      createContract(
        state.ledgerClient,
        state.time.time.getCurrentTime,
        party,
        templateId,
        value,
        sender)

    case ExerciseChoice(party, contractId, choiceId, value) =>
      exerciseChoice(
        state.ledgerClient,
        state.time.time.getCurrentTime,
        party,
        contractId,
        choiceId,
        value,
        sender)

    case ReportCurrentTime =>
      sender ! Success(state.time)

    case AdvanceTime(to) =>
      advanceTime(state.staticTime, to, sender)

    case ResetConnection =>
      // Wait for all children to stop, then initiate new connection
      implicit val actorTimeout: Timeout = Timeout(60, TimeUnit.SECONDS)
      context.children.foreach(child => child ? ResetConnection)
      context.become(connecting(StateInitial(state.parties)))
      connect()

    case GetApplicationStateInfo =>
      implicit val actorTimeout: Timeout = Timeout(5, TimeUnit.SECONDS)

      // Store the original sender (sender is mutable)
      val snd = sender

      Future
        .traverse(state.parties)(ps => {
          val result = for {
            ref <- context.child(childName(ps))
            pi <- Try(
              (ref ? GetPartyActorInfo)
                .mapTo[PartyActorInfo]
                .recover { case _ => PartyActorUnresponsive(ps.name) }
            ).toOption
          } yield pi
          result.getOrElse(Future.successful(PartyActorUnresponsive(ps.name)))
        })
        .andThen {
          case Success(actorStatus) =>
            snd ! ApplicationStateConnected(
              platformHost,
              platformPort,
              tlsConfig.exists(_.enabled),
              applicationId,
              state.ledgerClient.ledgerId,
              state.time,
              actorStatus
            )
          case Failure(error) =>
            log.error(error.getMessage)
            snd ! ApplicationStateConnected(
              platformHost,
              platformPort,
              tlsConfig.exists(_.enabled),
              applicationId,
              state.ledgerClient.ledgerId,
              state.time,
              List.empty
            )
        }
      ()
  }

  // Permanently failed state
  def failed(state: StateFailed): Receive = {
    case GetApplicationStateInfo =>
      sender ! ApplicationStateFailed(
        platformHost,
        platformPort,
        tlsConfig.exists(_.enabled),
        applicationId,
        state.error)

    case _ => ()
  }

  // ----------------------------------------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------------------------------------
  private def childName(party: PartyState): String =
    "party-" + URLEncoder.encode(ApiTypes.Party.unwrap(party.name), "UTF-8")

  private def startPartyActor(ledgerClient: LedgerClient, party: PartyState): ActorRef = {
    context.actorOf(PlatformSubscriber.props(ledgerClient, party, applicationId), childName(party))
  }

  private def sslContext: Option[SslContext] =
    tlsConfig.flatMap { c =>
      if (c.enabled)
        Some(
          GrpcSslContexts
            .forClient()
            .trustManager(c.trustCertCollectionFile.orNull)
            .keyManager(c.keyCertChainFile.orNull, c.keyFile.orNull)
            .build
        )
      else None
    }

  private def connect(): Unit = {
    val retryMaxAttempts = 10
    val retryDelay = 5.seconds
    val maxCommandsInFlight = 10
    val maxParallelSubmissions = 10

    val configuration = LedgerClientConfiguration(
      applicationId,
      LedgerIdRequirement("_", enabled = false),
      CommandClientConfiguration(
        maxCommandsInFlight,
        maxParallelSubmissions,
        overrideTtl = false,
        Duration.ofSeconds(30)),
      sslContext
    )

    val result = RetryHelper.retry(retryMaxAttempts, retryDelay)(RetryHelper.always)(
      tryConnect(configuration)
    )

    result onComplete {
      case Failure(error) =>
        log.error(
          "Failed to connect to platform at '{}:{}': {}",
          platformHost,
          platformPort,
          error.getMessage)
        self ! PlatformStore.Connected(Failure(error))
      case Success(connection) =>
        log.info("Connected to platform at '{}:{}'", platformHost, platformPort)
        self ! PlatformStore.Connected(Success(connection))
    }
  }

  private def tryConnect(configuration: LedgerClientConfiguration): Future[ConnectionResult] = {

    val builder = NettyChannelBuilder
      .forAddress(platformHost, platformPort)
      .maxInboundMessageSize(50 * 1024 * 1024)
    configuration.sslContext match {
      case None => {
        log.info("Connecting to {}:{}, using a plaintext connection", platformHost, platformPort)
        builder.usePlaintext()
      }
      case Some(ssl) => {
        log.info("Connecting to {}:{}, using TLS", platformHost, platformPort)
        builder.useTransportSecurity().sslContext(ssl)
      }
    }

    val channel = builder.build()

    sys.addShutdownHook({ channel.shutdownNow(); () })

    for {
      ledgerClient <- LedgerClient.forChannel(configuration, channel)
      staticTime <- getStaticTime(channel, ledgerClient.ledgerId)
      time <- getTimeProvider(staticTime)
    } yield ConnectionResult(ledgerClient, staticTime, time)
  }

  private def getStaticTime(
      channel: ManagedChannel,
      ledgerId: String): Future[Option[StaticTime]] = {
    // Note: StaticTime is a TimeProvider that is automatically updated by push events from the ledger.
    Future
      .fromTry(Try(TimeServiceGrpc.stub(channel)))
      .flatMap(tp => StaticTime.updatedVia(tp, ledgerId))
      .map(staticTime => {
        log.info(s"Time service is available, platform time is ${staticTime.getCurrentTime}")
        Some(staticTime)
      })
      .recover({
        // If the time service is not implemented, then the ledger uses UTC time.
        case e: io.grpc.StatusRuntimeException
            if e.getStatus.getCode == Status.Code.UNIMPLEMENTED => {
          log.info("Time service is not implemented")
          None
        }
      })
  }

  private def getTimeProvider(ledgerTime: Option[StaticTime]): Future[TimeProviderWithType] = {
    TimeProviderFactory(timeProviderType, ledgerTime)
      .fold[Future[TimeProviderWithType]]({
        log.error("Unable to initialize the time provider")
        Future.failed(StoreException("Unable to initialize the time provider"))
      })(t => {
        log.debug(s"Time provider initialized: type=${t.`type`}, time=${t.time.getCurrentTime}")
        Future.successful(t)
      })
  }

  private def advanceTime(staticTime: Option[StaticTime], to: Instant, sender: ActorRef): Unit = {
    staticTime.fold[Unit](
      sender ! Failure(StoreException("staticTime not available"))
    )(t => {
      log.info("Advancing time from {} to {}.", t.getCurrentTime, to)
      t.setTime(to)
        .map(_ => TimeProviderWithType(t, TimeProviderType.Static))
        .recoverWith(apiFailure)
        .andThen(returnToSender[TimeProviderWithType](sender))
      ()
    })
  }

  private def createContract(
      ledgerClient: LedgerClient,
      platformTime: Instant,
      party: PartyState,
      templateId: TemplateStringId,
      value: ApiRecord,
      sender: ActorRef
  ): Unit = {
    val commandId = commandIdGenerator.generateRandom
    val workflowId = workflowIdGenerator.generateRandom
    val index = commandIndex.incrementAndGet()

    parseOpaqueIdentifier(templateId).fold({
      val msg = s"Create contract command not sent, '$templateId' is not a valid DAML-LF identifier"
      log.warning(msg)
      sender ! Failure(StoreException(msg))
    })(id => {
      val command = CreateCommand(commandId, index, workflowId, platformTime, id, value)
      submitCommand(ledgerClient, party, command, sender)
    })

  }

  private def exerciseChoice(
      ledgerClient: LedgerClient,
      platformTime: Instant,
      party: PartyState,
      contractId: ApiTypes.ContractId,
      choice: ApiTypes.Choice,
      value: ApiValue,
      sender: ActorRef
  ): Unit = {
    val commandId = commandIdGenerator.generateRandom
    val workflowId = workflowIdGenerator.generateRandom
    val index = commandIndex.incrementAndGet()
    // Note: the ledger API does not need the template ID to submit an exercise command.
    // However, Navigator needs it to serialize/deserialize the choice argument, so we look up the template here.
    party.ledger
      .contract(contractId, party.packageRegistry)
      .fold({
        val msg = s"Exercise contract command not sent, contract $contractId not found"
        log.warning(msg)
        sender ! Failure(StoreException(msg))
      })(contract => {
        val command =
          ExerciseCommand(
            commandId,
            index,
            workflowId,
            platformTime,
            contractId,
            contract.template.id,
            choice,
            value)

        submitCommand(ledgerClient, party, command, sender)
      })
  }

  private def submitCommand(
      ledgerClient: LedgerClient,
      party: PartyState,
      command: Command,
      sender: ActorRef
  ): Unit = {
    // Each party has its own command completion stream.
    // Forward the request to the party actor, so that it can be tracked.
    context
      .child(childName(party))
      .foreach(child => child ! PlatformSubscriber.SubmitCommand(command, sender))
  }

  private val idGeneratorSeed = System.currentTimeMillis()
  private val workflowIdGenerator: IdGenerator[ApiTypes.WorkflowIdTag] = new IdGenerator(
    idGeneratorSeed)
  private val commandIdGenerator: IdGenerator[ApiTypes.CommandIdTag] = new IdGenerator(
    idGeneratorSeed + 1)
  private val commandIndex = new AtomicLong(0)

  private def returnToSender[T](sender: ActorRef): PartialFunction[Try[T], Unit] = {
    case Success(e) =>
      log.debug(s"Sending Success($e) to $sender")
      sender ! Success(e)
    case Failure(f) =>
      log.debug(s"Sending Failure($f) to $sender")
      sender ! Failure(f)
  }

  private def apiFailure[T]: PartialFunction[Throwable, Future[T]] = {
    case exception: Exception =>
      log.error("Unable to perform API operation: {}", exception.getMessage)
      Future.failed(StoreException(exception.getMessage))
  }

}
