// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.service.server

import java.io.File
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.{Executors, ExecutorService}

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsValueUnmarshaller
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.daml.bazeltools.BazelRunfiles
import com.daml.grpc.adapter.{ExecutionSequencerFactory, AkkaExecutionSequencerPool}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.archive.{DarDecoder, Dar}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{QualifiedName, Identifier}
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.lf.engine.script.{LfValueCodec, Participants, Runner, ApiParameters}
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SValue
import com.daml.ports.Port
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext}
import spray.json.JsValue

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.DurationInt
import scala.util.{Success, Failure}

// TODO many items re: configurability -- use Typesafe Config/pureconfig -- align configuration keys with Canton
// TODO allow usage of logging, use contextualized logging, make it configurable, possibly at runtime
// TODO record metrics, report them, make them configurable
object Server {

  // TODO the following items should probably be made configurable
  private val ActorSystemName = "daml-script-service-actor-system"
  private val ExecutionSequencerPoolName = "daml-script-service-sequencer-pool"
  private val HttpRoutePrefix = "script"

  // this pool is only used to start the server and bind the `Future` (see `defineServerStart` method below)
  // TODO evaluate a sync and/or single-threaded approach for simplicity
  private def newWorkStealingPool(): ExecutorService =
    Executors.newWorkStealingPool(Runtime.getRuntime.availableProcessors())

  private def newAkkaExecutionSequencerPool(implicit
      system: ActorSystem
  ): () => ExecutionSequencerFactory =
    () =>
      new AkkaExecutionSequencerPool(
        poolName = ExecutionSequencerPoolName,
        terminationTimeout = 5.seconds, // TODO make this configurable
      )

  // TODO handle exception-safe sanitazion and handling of `rawScriptId`
  private def qualify(packageId: Ref.PackageId, rawScriptId: String): Identifier =
    Identifier(packageId, QualifiedName.assertFromString(rawScriptId))

  // TODO improve Daml Script itself to allow us to maintain a compiled script available
  // TODO make this work with the Auth Middleware
  private def connectAndRun(
      input: JsValue,
      dar: Dar[(Ref.PackageId, Ast.Package)],
      rawScriptId: String,
      port: Port,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[SValue] =
    for {
      clients <-
        Runner.connect(
          Participants[ApiParameters](
            default_participant = Some(
              ApiParameters(
                host = InetAddress.getLoopbackAddress.getHostAddress, // TODO make this configurable
                port = port.value, // TODO make this configurable
                access_token = None, // TODO enable secure access, make this configurable
                application_id = None, // TODO make this configurable
              )
            ),
            participants = Map.empty, // TODO inquire: what is this?
            party_participants = Map.empty, // TODO inquire: what is this?
          ),
          // TODO enable secure access, make `tlsConfig` configurable
          tlsConfig = TlsConfiguration.Empty.copy(enabled = false),
          maxInboundMessageSize = 4 * 1024 * 1024, // TODO make this configurable
        )
      value <- Runner.run(
        dar = dar,
        scriptId = qualify(dar.main._1, rawScriptId),
        inputValue = Some(input),
        initialClients = clients,
        timeMode = ScriptTimeMode.WallClock, // TODO make this configurable
      )
    } yield value

  // TODO open point: how to acquire and expose scripts?
  private[server] val dar = DarDecoder.assertReadArchiveFromFile(darFile =
    new File(BazelRunfiles.rlocation("daml-script/test/script-test.dar"))
  )

  private[server] def route(port: Port)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Route =
    path(HttpRoutePrefix / Remaining) { scriptId =>
      // TODO possibly index known scripts and explicitly route to a 404 for unknown scripts
      // TODO evaluate a translation between URL and module separators, i.e. `/` -> `:`
      // TODO evaluate consequences of translation with the language team
      // TODO include the package identifier in the route for endpoint versioning
      // TODO could one use the version from `daml.yaml` to "tag" the version? would it buy us anything?
      post {
        entity(as[JsValue]) { json =>
          // TODO accept arbitrary `dar`, make them configurable
          // TODO evaluate how to map a script identifier to a route
          onComplete(connectAndRun(json, dar, scriptId, port)) {
            case Success(result) =>
              complete(responseEntity(result))
            case Failure(throwable) =>
              failWith(throwable)
          }
        }
      }
    }

  // TODO make binding host and port configurable
  private def defineServerStart(port: Port)(implicit
      system: ActorSystem,
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): () => Future[Http.ServerBinding] =
    () => Http().newServerAt("localhost", 8080).bind(route(port))

  // TODO consider using `akka.http.scaladsl.marshalling.Marshal`
  private def responseEntity(value: SValue): HttpEntity.Strict =
    HttpEntity(
      ContentTypes.`application/json`,
      LfValueCodec.apiValueToJsValue(value.toUnnormalizedValue).compactPrint,
    )

  def owner[Context](port: Port)(implicit
      context: HasExecutionContext[Context]
  ): AbstractResourceOwner[Context, InetSocketAddress] = {
    val owner = new ResourceOwners[Context](context)
    for {
      actorSystem <- owner.forActorSystem(() => ActorSystem(ActorSystemName))
      executor <- owner.forExecutorService(newWorkStealingPool)
      materializer <- owner.forMaterializer(() => Materializer(actorSystem))
      executionSequencerPool <- owner.forCloseable(newAkkaExecutionSequencerPool(actorSystem))
      executionContext = ExecutionContext.fromExecutorService(executor)
      serverStartDefinition = defineServerStart(port)(
        actorSystem,
        executionContext,
        executionSequencerPool,
        materializer,
      )
      server <- owner.forHttpServerBinding(serverStartDefinition)
    } yield server.localAddress
  }

}
