// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.store.platform

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import org.apache.pekko.actor.{Actor, ActorLogging, ActorRef, Props, Scheduler, Stash}
import org.apache.pekko.pattern.{ask, pipe}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout
import com.daml.grpc.GrpcException
import com.daml.grpc.adapter.{PekkoExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.domain.Feature
import com.daml.ledger.api.refinements.{ApiTypes, IdGenerator}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.client.services.testing.time.StaticTime
import com.daml.lf.data.Ref
import com.daml.navigator.ApplicationInfo
import com.daml.navigator.model._
import com.daml.navigator.store.Store._
import com.daml.navigator.time._
import com.daml.navigator.util.RetryHelper
import io.grpc.Channel
import io.grpc.netty.GrpcSslContexts
import io.netty.handler.ssl.SslContext
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}

object PlatformStore {
  def props(
      platformHost: String,
      platformPort: Int,
      tlsConfig: Option[TlsConfiguration],
      accessToken: Option[String],
      timeProviderType: TimeProviderType,
      applicationInfo: ApplicationInfo,
      ledgerMaxInbound: Int,
      enableUserManagement: Boolean,
  ): Props =
    Props(
      classOf[PlatformStore],
      platformHost,
      platformPort,
      tlsConfig,
      accessToken,
      timeProviderType,
      applicationInfo,
      ledgerMaxInbound,
      enableUserManagement,
    )

  type PlatformTime = Instant

  case class ConnectionResult(
      ledgerClient: LedgerClient,
      staticTime: Option[StaticTime],
      time: TimeProviderWithType,
  )
  case class Connected(ledgerClient: Try[ConnectionResult])

  case class StateConnected(
      ledgerClient: LedgerClient,
      // A key in `parties` could be: a Party's displayName, a party's name, a User's id, or a name specified in our config
      parties: Map[String, PartyState],
      staticTime: Option[StaticTime],
      time: TimeProviderWithType,
  )
  case class StateInitial(parties: Map[String, PartyState])
  case class StateFailed(error: Throwable)
}

/** Store implementation that accesses the platform API to fetch data and execute commands. */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class PlatformStore(
    platformHost: String,
    platformPort: Int,
    tlsConfig: Option[TlsConfiguration],
    token: Option[String],
    timeProviderType: TimeProviderType,
    applicationInfo: ApplicationInfo,
    ledgerMaxInbound: Int,
    enableUserManagement: Boolean,
) extends Actor
    with ActorLogging
    with Stash {

  // ----------------------------------------------------------------------------------------------
  // Global immutable state - mutable state is stored in parameters of the receive methods
  // ----------------------------------------------------------------------------------------------
  private val system = context.system

  implicit val s: Scheduler = system.scheduler
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val materializer: Materializer = Materializer(system)
  implicit val esf: ExecutionSequencerFactory =
    new PekkoExecutionSequencerPool("esf-" + this.getClass.getSimpleName)(system)

  private val applicationId =
    Ref.LedgerString.assertFromString(
      applicationInfo.id + "#" + new Random(System.currentTimeMillis()).nextLong().toHexString
    )

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
  override def receive: Receive = connecting(StateInitial(Map.empty[String, PartyState]))

  def connecting(state: StateInitial): Receive = {
    case Connected(Success(value)) =>
      context.become(
        connected(StateConnected(value.ledgerClient, state.parties, value.staticTime, value.time))
      )
      unstashAll()

    case Connected(Failure(e)) =>
      // Connection failed even after several retries - not sure how to recover from this
      val message =
        s"Permanently failed to connect to the ledger at $platformHost:$platformPort. " +
          "Please fix any issues and restart this application."
      userFacingLogger.error(message)
      context.become(failed(StateFailed(e)))

    case GetApplicationStateInfo =>
      sender() ! ApplicationStateConnecting(
        platformHost,
        platformPort,
        tlsConfig.exists(_.enabled),
        applicationId,
      )

    case _ =>
      stash()
  }

  def connected(state: StateConnected): Receive = {
    case UpdateUsersOrParties =>
      // If user management is enabled for navigator (the default),
      // and the ledger supports it, only display users on the login screen.
      // Otherwise, we fall back to legacy behavior of logging in as a party.
      val hasUserManagement =
        if (!enableUserManagement) Future.successful(false)
        else
          state.ledgerClient.versionClient
            .getApiFeatures(state.ledgerClient.ledgerId)
            .map { features =>
              val hasUserManagement = features.contains(Feature.UserManagement)
              if (!hasUserManagement) {
                log.warning("User management is enabled but ledger does not expose this feature.")
              }
              hasUserManagement
            }

      // TODO https://github.com/digital-asset/daml/issues/12663 participant user management: Emulating no-pagination
      def listAllUsers(client: LedgerClient) =
        client.userManagementClient
          .listUsers(token = None /* set at startup */, pageToken = "", pageSize = 10000)
          .map { case (users, _) =>
            users.toList
          }

      hasUserManagement
        .flatMap {
          case true =>
            listAllUsers(state.ledgerClient)
              .map(details => UpdatedUsers(details): Any)
          //                                       ^^^^^
          // make wartremover happy about the lub of UpdatedUsers and UpdatedParties
          case false =>
            state.ledgerClient.partyManagementClient
              .listKnownParties()
              .map(details => UpdatedParties(details): Any)
        }
        .pipeTo(self)

      ()

    case UpdatedUsers(users) =>
      // Note: you cannot log in as a user without a primary party
      val usersWithPrimaryParties = users.flatMap { user =>
        user.primaryParty.map(p => (user.id, p))
      }

      users.filter(_.primaryParty.isEmpty) match {
        case usersWithoutPrimaryParty if !usersWithoutPrimaryParty.isEmpty =>
          log.warning(
            s"Users without primary party (counted ${usersWithoutPrimaryParty.length})" +
              s" cannot be used for login ${usersWithoutPrimaryParty.take(10).map(_.id.toString).mkString("(e.g., ", ", ", ")")}."
          )
      }

      usersWithPrimaryParties.foreach { case (userId, party) =>
        self ! Subscribe(userId, ApiTypes.Party(party))
      }

    case UpdatedParties(details) =>
      details.foreach { partyDetails =>
        if (partyDetails.isLocal) {
          val displayName = partyDetails.displayName.getOrElse(partyDetails.party)
          self ! Subscribe(displayName, ApiTypes.Party(partyDetails.party))
        } else {
          log.debug(s"Ignoring non-local party ${partyDetails.party}")
        }
      }

    case Subscribe(displayName, name, userRole, useDatabase) =>
      if (!state.parties.contains(displayName)) {
        val partyState =
          new PartyState(name, userRole, useDatabase) // do this allocation only once per party
        log.info(s"Starting actor for party ${partyState.name} (display name $displayName)")

        // start party actor if needed (since users subscribe to their primary party,
        // we may subscribe to the same party under different display names, but we should only create one actor per party)
        val partyActorName = partyState.actorName
        if (context.child(partyActorName).isEmpty)
          context.actorOf(
            PlatformSubscriber.props(state.ledgerClient, partyState, applicationId, token),
            partyActorName,
          )

        val updatedParties = state.parties + (displayName -> partyState)
        context.become(connected(state.copy(parties = updatedParties)))
      } else {
        log.debug(s"Actor for party $name (display name $displayName) is already running")
      }

    case CreateContract(party, templateId, value) =>
      createContract(state.time.time.getCurrentTime, party, templateId, value, sender())

    case ExerciseChoice(party, contractId, interfaceId, choiceId, value) =>
      exerciseChoice(
        state.time.time.getCurrentTime,
        party,
        contractId,
        interfaceId,
        choiceId,
        value,
        sender(),
      )

    case GetParties(search) =>
      val lowerCaseSearch = search.toLowerCase
      val result = state.parties.values.view.collect {
        case party if party.name.unwrap.toLowerCase.contains(lowerCaseSearch) => party.name
      }
      sender() ! PartyList(result.toList)

    case ReportCurrentTime =>
      sender() ! Success(state.time)

    case AdvanceTime(to) =>
      advanceTime(state.staticTime, to, sender())

    case ResetConnection =>
      // Wait for all children to stop, then initiate new connection
      implicit val actorTimeout: Timeout = Timeout(60, TimeUnit.SECONDS)
      context.children.foreach(child => child ? ResetConnection)
      context.become(connecting(StateInitial(state.parties)))
      connect()

    case GetApplicationStateInfo =>
      implicit val actorTimeout: Timeout = Timeout(5, TimeUnit.SECONDS)

      // Store the original sender (sender is mutable)
      val snd = sender()

      // context.child must be invoked from actor thread
      val userIdToActorRef = state.parties.view
        .mapValues(partyState => context.child(partyState.actorName))
        .toList // TODO can we keep this a map and make traverse work?

      Future
        .traverse(userIdToActorRef) { case (userId, actorRef) =>
          // let Future deal with empty option actorRef
          Future { actorRef.get }
            .flatMap(_ ? GetPartyActorInfo)
            .mapTo[PartyActorInfo]
            .map(info => PartyActorRunning(info): PartyActorResponse)
            .recover { case _ => PartyActorUnresponsive }
            .map((userId, _))
        }
        .map(_.toMap)
        .recover { case error =>
          log.error(error.getMessage)
          Map.empty[String, PartyActorResponse]
        }
        .foreach { userIdToPartyActorResponse =>
          snd ! ApplicationStateConnected(
            platformHost,
            platformPort,
            tlsConfig.exists(_.enabled),
            applicationId,
            state.ledgerClient.ledgerId.unwrap,
            state.time,
            userIdToPartyActorResponse,
          )
        }
  }

  // Permanently failed state
  def failed(state: StateFailed): Receive = {
    case GetApplicationStateInfo =>
      sender() ! ApplicationStateFailed(
        platformHost,
        platformPort,
        tlsConfig.exists(_.enabled),
        applicationId,
        state.error,
      )

    case _ => ()
  }

  // ----------------------------------------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------------------------------------
  private def sslContext: Option[SslContext] =
    tlsConfig.flatMap { c =>
      if (c.enabled)
        Some(
          GrpcSslContexts
            .forClient()
            .trustManager(c.trustCollectionFile.orNull)
            .keyManager(c.certChainFile.orNull, c.privateKeyFile.orNull)
            .build
        )
      else None
    }

  private def connect(): Unit = {
    val retryMaxAttempts = 10
    val retryDelay = 5.seconds
    val maxCommandsInFlight = 10
    val maxParallelSubmissions = 10

    val clientConfig = LedgerClientConfiguration(
      applicationId,
      LedgerIdRequirement.none,
      CommandClientConfiguration(
        maxCommandsInFlight,
        maxParallelSubmissions,
        Duration.ofSeconds(30),
      ),
      token,
    )

    val channelConfig = LedgerClientChannelConfiguration(sslContext)

    val result =
      RetryHelper.retry(retryMaxAttempts, retryDelay)(RetryHelper.failFastOnPermissionDenied)(
        tryConnect(clientConfig, channelConfig)
      )

    result onComplete {
      case Failure(error) =>
        log.error(
          "Failed to connect to platform at '{}:{}': {}",
          platformHost,
          platformPort,
          error.getMessage,
        )
        self ! PlatformStore.Connected(Failure(error))
      case Success(connection) =>
        log.info("Connected to platform at '{}:{}'", platformHost, platformPort)
        self ! PlatformStore.Connected(Success(connection))
    }
  }

  private def tryConnect(
      clientConfig: LedgerClientConfiguration,
      channelConfig: LedgerClientChannelConfiguration,
  ): Future[ConnectionResult] = {

    if (channelConfig.sslContext.isDefined) {
      log.info("Connecting to {}:{}, using TLS", platformHost, platformPort)
    } else {
      log.info("Connecting to {}:{}, using a plaintext connection", platformHost, platformPort)
    }

    for {
      ledgerClient <- LedgerClient.singleHost(
        platformHost,
        platformPort,
        clientConfig,
        channelConfig.copy(maxInboundMessageSize = ledgerMaxInbound),
      )
      staticTime <- getStaticTime(ledgerClient.channel, ledgerClient.ledgerId.unwrap)
      time <- getTimeProvider(staticTime)
    } yield ConnectionResult(ledgerClient, staticTime, time)
  }

  private def getStaticTime(channel: Channel, ledgerId: String): Future[Option[StaticTime]] = {
    // Note: StaticTime is a TimeProvider that is automatically updated by push events from the ledger.
    Future
      .fromTry(Try(TimeServiceGrpc.stub(channel)))
      .flatMap(tp => StaticTime.updatedVia(tp, ledgerId, token))
      .map(staticTime => {
        log.info(s"Time service is available, platform time is ${staticTime.getCurrentTime}")
        Some(staticTime)
      })
      .recover({
        // If the time service is not implemented, then the ledger uses UTC time.
        case GrpcException.UNIMPLEMENTED() =>
          log.info("Time service is not implemented")
          None
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
      platformTime: Instant,
      party: PartyState,
      templateId: TemplateStringId,
      value: ApiRecord,
      sender: ActorRef,
  ): Unit = {
    val commandId = commandIdGenerator.generateRandom
    val workflowId = workflowIdGenerator.generateRandom
    val index = commandIndex.incrementAndGet()

    parseTemplateOpaqueIdentifier(templateId).fold({
      val msg = s"Create contract command not sent, '$templateId' is not a valid Daml-LF identifier"
      log.warning(msg)
      sender ! Failure(StoreException(msg))
    })(id => {
      val command = CreateCommand(commandId, index, workflowId, platformTime, id, value)
      submitCommand(party, command, sender)
    })

  }

  private def exerciseChoice(
      platformTime: Instant,
      party: PartyState,
      contractId: ApiTypes.ContractId,
      interfaceId: Option[InterfaceStringId],
      choice: ApiTypes.Choice,
      value: ApiValue,
      sender: ActorRef,
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
            interfaceId.flatMap(parseInterfaceOpaqueIdentifier),
            choice,
            value,
          )

        submitCommand(party, command, sender)
      })
  }

  private def submitCommand(partyState: PartyState, command: Command, sender: ActorRef): Unit = {
    // Each party has its own command completion stream.
    // Forward the request to the party actor, so that it can be tracked.
    context
      .child(partyState.actorName)
      .foreach(child => child ! PlatformSubscriber.SubmitCommand(command, sender))
  }

  private val idGeneratorSeed = System.currentTimeMillis()
  private val workflowIdGenerator: IdGenerator[ApiTypes.WorkflowIdTag] = new IdGenerator(
    idGeneratorSeed
  )
  private val commandIdGenerator: IdGenerator[ApiTypes.CommandIdTag] = new IdGenerator(
    idGeneratorSeed + 1
  )
  private val commandIndex = new AtomicLong(0)

  private def returnToSender[T](sender: ActorRef): PartialFunction[Try[T], Unit] = {
    case Success(e) =>
      log.debug(s"Sending Success($e) to $sender")
      sender ! Success(e)
    case Failure(f) =>
      log.debug(s"Sending Failure($f) to $sender")
      sender ! Failure(f)
  }

  private def apiFailure[T]: PartialFunction[Throwable, Future[T]] = { case exception: Exception =>
    log.error("Unable to perform API operation: {}", exception.getMessage)
    Future.failed(StoreException(exception.getMessage))
  }

}
