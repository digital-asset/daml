// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator

import java.nio.file.{Path, Paths}
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.{Cookie, EntityTag, HttpCookie, `Cache-Control`}
import akka.http.scaladsl.model.headers.CacheDirectives.{`max-age`, `no-cache`, immutableDirective}
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.settings.RoutingSettings
import akka.stream.ActorMaterializer
import com.digitalasset.navigator.SessionJsonProtocol._
import com.digitalasset.navigator.config._
import com.digitalasset.navigator.graphql.GraphQLContext
import com.digitalasset.navigator.graphqless.GraphQLObject
import com.digitalasset.navigator.model.{Ledger, PackageRegistry}
import com.digitalasset.navigator.store.Store.Subscribe
import com.digitalasset.navigator.store.platform.PlatformStore
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.LoggerFactory
import sangria.schema._
import spray.json._

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Base abstract class for UI backends.
  *
  * A new UI backend can be implemented by extending [[UIBackend]] and by providing
  * the [[customEndpoints]], [[customRoutes]], [[applicationInfo]] definitions.
  */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.ExplicitImplicitTypes",
    "org.wartremover.warts.Option2Iterable"))
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
      info: InfoHandler): Route = {

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
          .flatMap(cookiePair => Session.current(cookiePair.value).map(cookiePair.value -> _))
          .headOption
      case _ =>
        None
    }

    def signIn: SignIn =
      SignIn(
        if (arguments.requirePassword) SignInPassword else SignInSelect(config.users.keySet),
        invalidCredentials = false)

    def invalidCredentials: SignIn =
      SignIn(
        if (arguments.requirePassword) SignInPassword else SignInSelect(config.users.keySet),
        invalidCredentials = true)

    // A resource with content that may change.
    // Users can quickly switch between Navigator versions, so we don't want to cache this resource for any amount of time.
    // Note: The server still uses E-Tags, so repeated fetches will complete with a "304: Not Modified".
    def mutableResource = respondWithHeader(`Cache-Control`(`no-cache`))
    // Add an ETag with value equal to the revision used to build the application.
    // Use as a cheap ETag for resources that may change between builds.
    def revisionETag = conditional(EntityTag(applicationInfo.revision))
    // Add an ETag with value equal to ID of this application instance.
    // Use as a cheap ETag for resources that may change between application instances.
    def applicationIdETag = conditional(EntityTag(applicationInfo.id))
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
                    case None => complete(signIn)
                  }
                } ~
                  post {
                    entity(as[LoginRequest]) { request =>
                      if (arguments.requirePassword) {
                        request.maybePassword match {
                          case None =>
                            logger.error(
                              s"Attempt to signin with user ${request.userId} without password")
                            complete(invalidCredentials)
                          case Some(password) =>
                            config.users.get(request.userId) match {
                              case None =>
                                logger.error(
                                  s"Attempt to signin with non-existent user ${request.userId}")
                                complete(invalidCredentials)
                              case Some(userConfig)
                                  if userConfig.password == None || userConfig.password.contains(
                                    password) =>
                                openSession(request.userId, userConfig)
                              case Some(_) =>
                                logger.error(
                                  s"Attempt to signin with user ${request.userId} and wrong password")
                                complete(invalidCredentials)
                            }
                        }
                      } else {
                        config.users.get(request.userId) match {
                          case None =>
                            logger.error(
                              s"Attept to signin with non-existent user ${request.userId}")
                            complete(invalidCredentials)
                          case Some(userConfig) =>
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
                      complete(signIn)
                    }
                  }
              } ~
              path("info") {
                mutableResource { complete(info.getInfo) }
              } ~
              path("graphql") {
                get {
                  revisionETag { mutableResource { getFromResource("graphiql.html") } }
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
                revisionETag {
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

    banner.foreach(b => scala.Console.out.println(b))

    implicit val system = ActorSystem("da-ui-backend")
    implicit val materializer = ActorMaterializer()

    import system.dispatcher

    val store = system.actorOf(
      PlatformStore.props(
        arguments.platformIp,
        arguments.platformPort,
        arguments.tlsConfig,
        arguments.time,
        applicationInfo,
        arguments.ledgerInboundMessageSizeMax))
    config.parties.foreach(store ! Subscribe(_))

    def graphQL: GraphQLHandler = DefaultGraphQLHandler(customEndpoints, Some(store))
    def info: InfoHandler = DefaultInfoHandler(arguments, store)

    val stopServer = if (arguments.startWebServer) {
      val binding = Http().bindAndHandle(
        getRoute(system, arguments, config, graphQL, info),
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
    val navigatorConfigFile = args.configFile.getOrElse(defaultConfigFile)

    args.command match {
      case ShowUsage =>
        Arguments.showUsage(defaultConfigFile)
      case DumpGraphQLSchema =>
        dumpGraphQLSchema()
      case CreateConfig =>
        userFacingLogger.info(s"Creating a configuration template file at $navigatorConfigFile")
        Config.writeTemplateToPath(navigatorConfigFile, args.useDatabase)
      case RunServer =>
        Config.load(navigatorConfigFile, args.useDatabase) match {
          case Left(ConfigNotFound(message)) =>
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
