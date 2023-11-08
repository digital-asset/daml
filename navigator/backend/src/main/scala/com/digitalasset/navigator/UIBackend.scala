// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

import org.apache.pekko.actor.{ActorSystem, Cancellable}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.HttpHeader
import org.apache.pekko.http.scaladsl.model.StatusCodes._
import org.apache.pekko.http.scaladsl.model.headers.CacheDirectives.{
  `max-age`,
  `no-cache`,
  immutableDirective,
}
import org.apache.pekko.http.scaladsl.model.headers.{Cookie, EntityTag, HttpCookie, `Cache-Control`}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.settings.RoutingSettings
import org.apache.pekko.http.scaladsl.settings.ServerSettings
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import com.daml.grpc.GrpcException
import com.daml.navigator.SessionJsonProtocol._
import com.daml.navigator.config._
import com.daml.navigator.graphql.GraphQLContext
import com.daml.navigator.graphqless.GraphQLObject
import com.daml.navigator.model.{Ledger, PackageRegistry, PartyState}
import com.daml.navigator.store.Store
import com.daml.navigator.store.Store._
import com.daml.navigator.store.platform.PlatformStore
import com.daml.scalautil.Statement.discard
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.LoggerFactory
import sangria.schema._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Base abstract class for UI backends.
  *
  * A new UI backend can be implemented by extending [[UIBackend]] and by providing
  * the [[customEndpoints]], [[customRoutes]], [[applicationInfo]] definitions.
  */
abstract class UIBackend extends LazyLogging with ApplicationInfoJsonSupport {

  def customEndpoints: Set[CustomEndpoint[_]]
  def customRoutes: List[Route]
  def applicationInfo: ApplicationInfo
  def banner: Option[String] = None

  private[this] def userFacingLogger = LoggerFactory.getLogger("user-facing-logs")

  /** Allow subclasses to customize config file name, but don't require it
    * (supply a default)
    */
  def defaultConfigFile: Path = Paths.get("ui-backend.conf")

  private[navigator] def getRoute(
      system: ActorSystem,
      arguments: Arguments,
      graphQL: GraphQLHandler,
      info: InfoHandler,
      getAppState: () => Future[ApplicationStateInfo],
  ): Route = {

    def openSession(userId: String, userRole: Option[String], state: PartyState): Route = {
      val sessionId = UUID.randomUUID().toString
      setCookie(HttpCookie("session-id", sessionId, path = Some("/"))) {
        complete(Session.open(sessionId, userId, userRole, state))
      }
    }

    def findSession(httpHeader: HttpHeader): Option[(String, Session)] = httpHeader match {
      case Cookie(cookies) =>
        cookies
          .filter(_.name == "session-id")
          .flatMap(cookiePair =>
            Session.current(cookiePair.value).map(cookiePair.value -> _).toList
          )
          .headOption
      case _ =>
        None
    }

    def signIn(): Route =
      onSuccess(getAppState()) {
        case ApplicationStateConnecting(_, _, _, _) =>
          complete(SignIn(SignInSelect(Set.empty), Some(NotConnected)))
        case ApplicationStateConnected(_, _, _, _, _, _, partyActors) =>
          complete(SignIn(SignInSelect(partyActors.keySet), None))
        case ApplicationStateFailed(_, _, _, _, _) =>
          complete(SignIn(SignInSelect(Set.empty), Some(Unknown)))
      }

    // A resource with content that may change.
    // Users can quickly switch between Navigator versions, so we don't want to cache this resource for any amount of time.
    // Note: The server still uses E-Tags, so repeated fetches will complete with a "304: Not Modified".
    def mutableResource = respondWithHeader(`Cache-Control`(`no-cache`))
    // Add an ETag with value equal to the version used to build the application.
    // Use as a cheap ETag for resources that may change between builds.
    def versionETag = conditional(EntityTag(applicationInfo.version))
    // A resource that is never going to change. Its path must use some kind of content hash.
    // Such a resource may be cached indefinitely.
    def immutableResource =
      respondWithHeaders(`Cache-Control`(`max-age`(31536000), immutableDirective))
    val noFileGetConditional = RoutingSettings(system).withFileGetConditional(false)
    def withoutFileETag = withSettings(noFileGetConditional)

    optionalHeaderValue(findSession) { session =>
      {
        // Custom routes
        customRoutes.foldLeft[Route](reject)(_ ~ _) ~
          // Built-in API routes
          pathPrefix("api") {
            path("about") {
              complete(applicationInfo)
            } ~
              path("session"./) {
                get {
                  session match {
                    case Some((_, session)) => complete(session)
                    case None => signIn()
                  }
                } ~
                  post {
                    entity(as[LoginRequest]) { request =>
                      onSuccess(getAppState()) {
                        case ApplicationStateConnecting(_, _, _, _) =>
                          complete(SignIn(SignInSelect(Set.empty), Some(NotConnected)))
                        case ApplicationStateConnected(_, _, _, _, _, _, partyActors) =>
                          partyActors.get(request.userId) match {
                            case Some(resp) =>
                              resp match {
                                case PartyActorRunning(info) =>
                                  openSession(request.userId, info.state.userRole, info.state)
                                case Store.PartyActorUnresponsive =>
                                  complete(
                                    SignIn(SignInSelect(partyActors.keySet), Some(Unresponsive))
                                  )
                              }
                            case None =>
                              logger.error(
                                s"Attempt to signin with non-existent user ${request.userId}"
                              )
                              complete(
                                SignIn(SignInSelect(partyActors.keySet), Some(InvalidCredentials))
                              )
                          }
                        case ApplicationStateFailed(
                              _,
                              _,
                              _,
                              _,
                              GrpcException.PERMISSION_DENIED(),
                            ) =>
                          logger.warn("Attempt to sign in without valid token")
                          complete(SignIn(SignInSelect(Set.empty), Some(InvalidCredentials)))
                        case ApplicationStateFailed(_, _, _, _, _) =>
                          complete(SignIn(SignInSelect(Set.empty), Some(Unknown)))
                      }
                    }
                  } ~
                  delete {
                    deleteCookie("session-id", path = "/") {
                      session match {
                        case Some((sessionId, sessionUser)) =>
                          Session.close(sessionId)
                          logger.info(s"Logged out user '${sessionUser.user.id}'")
                        case None =>
                          logger.error("Cannot delete session without session-id, cookie not found")
                      }
                      signIn()
                    }
                  }
              } ~
              path("info") {
                mutableResource { complete(info.getInfo) }
              } ~
              path("graphql") {
                get {
                  versionETag { mutableResource { getFromResource("graphiql.html") } }
                } ~
                  post {
                    authorize(session.isDefined) {
                      entity(as[JsValue]) { request =>
                        logger.debug(s"Handling GraphQL query: $request")
                        graphQL.parse(request) match {
                          case Success(parsed) =>
                            complete(graphQL.executeQuery(parsed, session.get._2.user.party))
                          case Failure(error) =>
                            logger.debug("Cannot execute query: " + error.getMessage)
                            complete((BadRequest, JsObject("error" -> JsString(error.getMessage))))
                        }
                      }
                    }
                  }
              }
          } ~ {
            arguments.assets match {
              case None =>
                // Serve assets from resource directory at /assets
                pathPrefix("assets") {
                  // Webpack makes sure all files use content hashing
                  immutableResource { withoutFileETag { getFromResourceDirectory("frontend") } }
                } ~
                  // Serve index on root and anything else to allow History API to behave
                  // as expected on reloading.
                  versionETag {
                    mutableResource { withoutFileETag { getFromResource("frontend/index.html") } }
                  }
              case Some(folder) =>
                // Serve assets under /assets
                pathPrefix("assets") {
                  mutableResource { getFromDirectory(folder) }
                } ~
                  // Serve index on root and anything else to allow History API to behave
                  // as expected on reloading.
                  mutableResource { getFromFile(folder + "/index.html") }
            }
          }
      }
    }
  }

  // Factored out for integration tests
  private[navigator] def setup(arguments: Arguments, config: Config)(implicit
      system: ActorSystem
  ) = {
    import system.dispatcher
    // Read from the access token file or crash
    val token =
      arguments.accessTokenFile.map { path =>
        try {
          Files.readAllLines(path).stream.collect(Collectors.joining("\n"))
        } catch {
          case NonFatal(e) =>
            throw new RuntimeException(s"Unable to read the access token from $path", e)
        }
      }

    val store = system.actorOf(
      PlatformStore.props(
        arguments.participantHost,
        arguments.participantPort,
        arguments.tlsConfig,
        token,
        arguments.time,
        applicationInfo,
        arguments.ledgerInboundMessageSizeMax,
        arguments.enableUserManagement,
      )
    )
    // TODO: usermgmt switching: for now we just poll both user and party mgmt
    // If no parties are specified, we periodically poll from the party management service.
    // If parties are specified, we only use those. This allows users to use custom display names
    // if they are non-unique or use only a subset of parties for performance reasons.
    // Currently, we subscribe to all available parties. We could change that to do it lazily only on login
    // but given that Navigator is only a development tool that might not be worth the complexity.
    val partyRefresh: Option[Cancellable] =
      if (config.users.isEmpty || arguments.ignoreProjectParties) {
        Some(
          system.scheduler
            .scheduleWithFixedDelay(Duration.Zero, 1.seconds, store, UpdateUsersOrParties)
        )
      } else {
        config.users.foreach { case (displayName, config) =>
          store ! Subscribe(
            displayName,
            config.party,
            config.role,
            config.useDatabase,
          )
        }
        None
      }

    def graphQL: GraphQLHandler = DefaultGraphQLHandler(customEndpoints, Some(store))
    def info: InfoHandler = DefaultInfoHandler(arguments, store)
    val getAppState: () => Future[ApplicationStateInfo] = () => {
      implicit val actorTimeout: Timeout = Timeout(5, TimeUnit.SECONDS)
      (store ? GetApplicationStateInfo).mapTo[ApplicationStateInfo]
    }
    (graphQL, info, store, getAppState, partyRefresh)
  }

  private[navigator] def runServer(arguments: Arguments, config: Config): Unit = {

    banner.foreach(println)

    implicit val system: ActorSystem = ActorSystem("da-ui-backend")

    val (graphQL, info, store @ _, getAppState, partyRefresh) = setup(arguments, config)

    val stopServer = if (arguments.startWebServer) {
      val binding = Http()
        .newServerAt("0.0.0.0", arguments.port)
        .withSettings(ServerSettings(system).withTransparentHeadRequests(true))
        .bind(getRoute(system, arguments, graphQL, info, getAppState))
      logger.info(s"DA UI backend server listening on port ${arguments.port}")
      println(s"Frontend running at http://localhost:${arguments.port}.")
      () => Try(Await.result(binding, 10.seconds).unbind())
    } else { () =>
      ()
    }

    val stopPekko = () => Try(Await.result(system.terminate(), 10.seconds))

    discard {
      sys.addShutdownHook {
        // Stop the web server, then the Pekko system consuming the ledger API
        stopServer()
        partyRefresh.foreach(_.cancel())
        stopPekko()
        ()
      }
    }
  }

  private def dumpGraphQLSchema(): Unit = {
    import ExecutionContext.Implicits.global

    def graphQL: GraphQLHandler = DefaultGraphQLHandler(customEndpoints, None)
    scala.Console.out.println(graphQL.renderSchema)
  }

  final def main(rawArgs: Array[String]): Unit =
    Arguments.parse(rawArgs, defaultConfigFile) foreach run

  private def run(args: Arguments): Unit = {
    val navigatorConfigFile =
      args.configFile.fold[ConfigOption](DefaultConfig(defaultConfigFile))(ExplicitConfig(_))

    args.command match {
      case ShowUsage =>
        Arguments.showUsage(defaultConfigFile)
      case DumpGraphQLSchema =>
        dumpGraphQLSchema()
      case CreateConfig =>
        userFacingLogger.info(
          s"Creating a configuration template file at ${navigatorConfigFile.path.toAbsolutePath()}"
        )
        Config.writeTemplateToPath(navigatorConfigFile.path, args.useDatabase)
      case RunServer =>
        Config.load(navigatorConfigFile, args.useDatabase) match {
          case Left(ConfigNotFound(_)) =>
            val message =
              s"""No configuration file found!
                 |Please specify a configuration file and restart ${applicationInfo.name}.
                 |Config file path was '$navigatorConfigFile'.
                 |Hint: use the create-config command to create a sample config file.""".stripMargin
            userFacingLogger.error(message)
            sys.error("No configuration file found.")
          case Left(ConfigParseFailed(message)) =>
            userFacingLogger.error(s"Configuration file could not be parsed: $message")
            sys.error(message)
          case Left(ConfigInvalid(message)) =>
            userFacingLogger.error(s"Configuration file is invalid: $message")
            sys.error(message)
          case Right(config) =>
            runServer(args, config)
        }
    }
  }
}

/** A custom endpoint for a backend to serve data of type `T`.
  *
  * Note that two `CustomEndpoint`s are considered equal if their `endpointName`s are the same to simplify
  * registering new `CustomEndpoint`s and validate them
  */
abstract class CustomEndpoint[T](implicit tGraphQL: GraphQLObject[T]) {

  /** The endpoint to be used as GraphQL top-level for the data served by this */
  def endpointName: String

  /** Calculate the data to serve within this endpoint */
  def calculate(ledger: Ledger, templates: PackageRegistry): Seq[T]

  final def endpoint: Field[GraphQLContext, Unit] = {
    val listOfTType: ListType[T] = ListType(tGraphQL.to[GraphQLContext])
    val resolve: Context[GraphQLContext, Unit] => Action[GraphQLContext, Seq[T]] =
      (context: Context[GraphQLContext, Unit]) =>
        calculate(context.ctx.ledger, context.ctx.templates)
    Field(endpointName, listOfTType, resolve = resolve)
  }

  final override def hashCode: Int = endpointName.hashCode

  final override def equals(obj: scala.Any): Boolean = obj match {
    case that: CustomEndpoint[_] => this.endpointName equals that.endpointName
    case _ => false
  }

  override def toString: String = endpointName
}
