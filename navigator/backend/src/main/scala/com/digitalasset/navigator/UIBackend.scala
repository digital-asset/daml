// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.CacheDirectives.{`max-age`, `no-cache`, immutableDirective}
import akka.http.scaladsl.model.headers.{Cookie, EntityTag, HttpCookie, `Cache-Control`}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.settings.RoutingSettings
import akka.pattern.ask
import akka.stream.Materializer
import akka.util.Timeout
import com.daml.grpc.GrpcException
import com.daml.navigator.SessionJsonProtocol._
import com.daml.navigator.config._
import com.daml.navigator.graphql.GraphQLContext
import com.daml.navigator.graphqless.GraphQLObject
import com.daml.navigator.model.{Ledger, PackageRegistry}
import com.daml.navigator.store.Store._
import com.daml.navigator.store.platform.PlatformStore
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.LoggerFactory
import sangria.schema._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Base abstract class for UI backends.
  *
  * A new UI backend can be implemented by extending [[UIBackend]] and by providing
  * the [[customEndpoints]], [[customRoutes]], [[applicationInfo]] definitions.
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
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
      config: Config,
      graphQL: GraphQLHandler,
      info: InfoHandler,
      getAppState: () => Future[ApplicationStateInfo]): Route = {

    def openSession(userId: String, userConfig: UserConfig): Route = {
      val sessionId = UUID.randomUUID().toString
      setCookie(HttpCookie("session-id", sessionId, path = Some("/"))) {
        complete(Session.open(sessionId, userId, userConfig))
      }
    }

    def findSession(httpHeader: HttpHeader): Option[(String, Session)] = httpHeader match {
      case Cookie(cookies) =>
        cookies
          .filter(_.name == "session-id")
          .flatMap(cookiePair =>
            Session.current(cookiePair.value).map(cookiePair.value -> _).toList)
          .headOption
      case _ =>
        None
    }

    def signIn(error: Option[SignInError] = None): SignIn =
      SignIn(SignInSelect(config.users.keySet), error)

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
                  complete {
                    session match {
                      case Some((_, session)) => session
                      case None => signIn()
                    }
                  }
                } ~
                  post {
                    entity(as[LoginRequest]) { request =>
                      config.users.get(request.userId) match {
                        case None =>
                          logger.error(
                            s"Attempt to signin with non-existent user ${request.userId}")
                          complete(signIn(Some(InvalidCredentials)))
                        case Some(userConfig) =>
                          onSuccess(getAppState()) {
                            case ApplicationStateFailed(
                                _,
                                _,
                                _,
                                _,
                                GrpcException.PERMISSION_DENIED()) =>
                              logger.warn("Attempt to sign in without valid token")
                              complete(signIn(Some(InvalidCredentials)))
                            case _: ApplicationStateFailed =>
                              complete(signIn(Some(Unknown)))
                            case _: ApplicationStateConnecting =>
                              complete(signIn(Some(NotConnected)))
                            case _ =>
                              openSession(request.userId, userConfig)
                          }
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
                      complete(signIn())
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

  private[navigator] def runServer(arguments: Arguments, config: Config): Unit = {

    banner.foreach(println)

    implicit val system: ActorSystem = ActorSystem("da-ui-backend")
    implicit val materializer: Materializer = Materializer(system)

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
        arguments.ledgerInboundMessageSizeMax
      ))
    config.parties.foreach(store ! Subscribe(_))

    def graphQL: GraphQLHandler = DefaultGraphQLHandler(customEndpoints, Some(store))
    def info: InfoHandler = DefaultInfoHandler(arguments, store)
    val getAppState: () => Future[ApplicationStateInfo] = () => {
      implicit val actorTimeout: Timeout = Timeout(5, TimeUnit.SECONDS)
      (store ? GetApplicationStateInfo).mapTo[ApplicationStateInfo]
    }

    val stopServer = if (arguments.startWebServer) {
      val binding = Http().bindAndHandle(
        getRoute(system, arguments, config, graphQL, info, getAppState),
        "0.0.0.0",
        arguments.port)
      logger.info(s"DA UI backend server listening on port ${arguments.port}")
      println(s"Frontend running at http://localhost:${arguments.port}.")
      () =>
        Try(Await.result(binding, 10.seconds).unbind())
    } else { () =>
      ()
    }

    val stopAkka = () => Try(Await.result(system.terminate(), 10.seconds))

    if (arguments.startConsole) {
      console.Console.run(arguments, config, store, graphQL, applicationInfo)
      // Stop the web server, then the Akka system consuming the ledger API
      stopServer()
      stopAkka()
      ()
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
          s"Creating a configuration template file at ${navigatorConfigFile.path.toAbsolutePath()}")
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

/**
  * A custom endpoint for a backend to serve data of type `T`.
  *
  * Note that two `CustomEndpoint`s are considered equal if their `endpointName`s are the same to simplify
  * registering new `CustomEndpoint`s and validate them
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
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
