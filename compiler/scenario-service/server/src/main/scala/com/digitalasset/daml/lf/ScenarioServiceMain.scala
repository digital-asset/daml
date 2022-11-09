// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.scenario

import akka.actor.ActorSystem
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import akka.stream.Materializer
import java.net.{InetAddress, InetSocketAddress}
import java.util.logging.{Level, Logger}
import scalaz.std.option._
import scalaz.std.scalaFuture._
import scalaz.syntax.traverse._
import com.daml.lf.archive
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ModuleName
import com.daml.lf.language.LanguageVersion
import com.daml.lf.scenario.api.v1.{Map => _, _}
import com.daml.logging.LoggingContext
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.netty.NettyServerBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private final case class ScenarioServiceConfig(
    maxInboundMessageSize: Int,
    enableScenarios: Boolean,
)

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
private object ScenarioServiceConfig {
  // default to 128MB
  val DefaultMaxInboundMessageSize: Int = 128 * 1024 * 1024

  val parser = new scopt.OptionParser[ScenarioServiceConfig]("scenario-service") {
    head("scenario-service")

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to ${DefaultMaxInboundMessageSize}."
      )

    opt[Boolean]("enable-scenarios")
      .optional()
      .action((x, c) => c.copy(enableScenarios = x))
      .text(
        "Enable/disable support for running scenarios. Defaults to true."
      )
  }

  def parse(args: Array[String]): Option[ScenarioServiceConfig] =
    parser.parse(
      args,
      ScenarioServiceConfig(
        maxInboundMessageSize = DefaultMaxInboundMessageSize,
        enableScenarios = true,
      ),
    )
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
object ScenarioServiceMain extends App {
  ScenarioServiceConfig.parse(args) match {
    case None => sys.exit(1)
    case Some(config) =>
      // Needed for the akka Ledger bindings used by Daml Script.
      val system = ActorSystem("ScriptService")
      implicit val sequencer: ExecutionSequencerFactory =
        new AkkaExecutionSequencerPool("ScriptServicePool")(system)
      implicit val materializer: Materializer = Materializer(system)
      implicit val ec: ExecutionContext = system.dispatcher
      LoggingContext.newLoggingContext { implicit lc: LoggingContext =>
        val server =
          NettyServerBuilder
            .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, 0)) // any free port
            .addService(new ScenarioService(config.enableScenarios))
            .maxInboundMessageSize(config.maxInboundMessageSize)
            .build
        server.start()
        // Print the allocated port for the client
        println("PORT=" + server.getPort.toString)

        // Bump up the log level
        Logger.getLogger("io.grpc").setLevel(Level.ALL)

        // Start a thread to watch stdin and terminate
        // if it closes. This makes sure we do not leave
        // this process running if the parent exits.
        new Thread(new Runnable {
          def run(): Unit = {
            while (System.in.read >= 0) {}
            System.err.println("ScenarioService: stdin closed, terminating server.")
            server.shutdown()
            system.terminate()
            ()
          }
        }).start()

        println("Server started.")
        server.awaitTermination()

      }
  }
}

object ScenarioService {
  private def notFoundContextError(id: Long): StatusRuntimeException =
    Status.NOT_FOUND.withDescription(s" context $id not found!").asRuntimeException
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class ScenarioService(
    enableScenarios: Boolean
)(implicit
    ec: ExecutionContext,
    esf: ExecutionSequencerFactory,
    mat: Materializer,
    lc: LoggingContext,
) extends ScenarioServiceGrpc.ScenarioServiceImplBase {

  import ScenarioService._

  private val contexts = TrieMap.empty[Context.ContextId, Context]

  private def log(msg: String) =
    System.err.println("ScenarioService: " + msg)

  override def runScenario(
      req: RunScenarioRequest,
      respObs: StreamObserver[RunScenarioResponse],
  ): Unit = {
    if (enableScenarios) {
      run(
        req,
        respObs,
        { case (ctx, pkgId, name) =>
          Future.successful(ctx.interpretScenario(pkgId, name))
        },
      )
    } else {
      log("Rejected scenario gRPC request.")
      respObs.onError(new UnsupportedOperationException("Scenarios are disabled"))
    }
  }

  override def runScript(
      req: RunScenarioRequest,
      respObs: StreamObserver[RunScenarioResponse],
  ): Unit = run(req, respObs, { case (ctx, pkgId, name) => ctx.interpretScript(pkgId, name) })

  private def run(
      req: RunScenarioRequest,
      respObs: StreamObserver[RunScenarioResponse],
      interpret: (Context, String, String) => Future[Option[ScenarioRunner.ScenarioResult]],
  ): Unit = {
    val scenarioId = req.getScenarioId
    val contextId = req.getContextId
    val response =
      contexts
        .get(contextId)
        .traverse { context =>
          val packageId = scenarioId.getPackage.getSumCase match {
            case PackageIdentifier.SumCase.SELF =>
              context.homePackageId
            case PackageIdentifier.SumCase.PACKAGE_ID =>
              scenarioId.getPackage.getPackageId
            case PackageIdentifier.SumCase.SUM_NOT_SET =>
              throw new RuntimeException(
                s"Package id not set when running scenario, context id $contextId"
              )
          }
          interpret(context, packageId, scenarioId.getName)
            .map(_.map {
              case error: ScenarioRunner.ScenarioError =>
                RunScenarioResponse.newBuilder
                  .setError(
                    new Conversions(
                      context.homePackageId,
                      error.ledger,
                      error.currentSubmission.map(_.ptx),
                      error.traceLog,
                      error.warningLog,
                      error.currentSubmission.flatMap(_.commitLocation),
                      error.stackTrace,
                    )
                      .convertScenarioError(error.error)
                  )
                  .build
              case success: ScenarioRunner.ScenarioSuccess =>
                RunScenarioResponse.newBuilder
                  .setResult(
                    new Conversions(
                      context.homePackageId,
                      success.ledger,
                      None,
                      success.traceLog,
                      success.warningLog,
                      None,
                      ImmArray.Empty,
                    )
                      .convertScenarioResult(success.resultValue)
                  )
                  .build
            })
        }
        .map(_.flatMap(x => x))

    response.onComplete {
      case Success(None) =>
        log(s"runScript[$contextId]: $scenarioId not found")
        respObs.onError(notFoundContextError(req.getContextId))
      case Success(Some(resp)) =>
        respObs.onNext(resp)
        respObs.onCompleted()
      case Failure(err) =>
        System.err.println(err)
        respObs.onError(err)
    }
  }

  override def newContext(
      req: NewContextRequest,
      respObs: StreamObserver[NewContextResponse],
  ): Unit = {
    val lfVersion = LanguageVersion(
      LanguageVersion.Major.V1,
      LanguageVersion.Minor(req.getLfMinor),
    )
    val ctx = Context.newContext(lfVersion)
    contexts += (ctx.contextId -> ctx)
    val response = NewContextResponse.newBuilder.setContextId(ctx.contextId).build
    respObs.onNext(response)
    respObs.onCompleted()
  }

  override def cloneContext(
      req: CloneContextRequest,
      respObs: StreamObserver[CloneContextResponse],
  ): Unit = {
    val ctxId = req.getContextId
    contexts.get(ctxId) match {
      case None =>
        log(s"cloneContext[$ctxId]: context not found!")
        respObs.onError(notFoundContextError(req.getContextId))
      case Some(ctx) =>
        val clonedCtx = ctx.cloneContext()
        contexts += (clonedCtx.contextId -> clonedCtx)
        val response = CloneContextResponse.newBuilder.setContextId(clonedCtx.contextId).build
        respObs.onNext(response)
        respObs.onCompleted()
    }
  }

  override def deleteContext(
      req: DeleteContextRequest,
      respObs: StreamObserver[DeleteContextResponse],
  ): Unit = {
    val ctxId = req.getContextId
    if (contexts.contains(ctxId)) {
      contexts -= ctxId
      respObs.onNext(DeleteContextResponse.newBuilder.build)
      respObs.onCompleted()
    } else {
      respObs.onError(notFoundContextError(req.getContextId))
    }
  }

  override def gCContexts(
      req: GCContextsRequest,
      respObs: StreamObserver[GCContextsResponse],
  ): Unit = {
    val ctxIds: Set[Context.ContextId] =
      req.getContextIdsList.asScala.map(x => x: Context.ContextId).toSet
    val ctxToRemove = contexts.keySet.diff(ctxIds)
    contexts --= ctxToRemove
    respObs.onNext(GCContextsResponse.newBuilder.build)
    respObs.onCompleted()
  }

  override def updateContext(
      req: UpdateContextRequest,
      respObs: StreamObserver[UpdateContextResponse],
  ): Unit = {

    val ctxId = req.getContextId
    val resp = UpdateContextResponse.newBuilder

    contexts.get(ctxId) match {
      case None =>
        log(s"updateContext[${req.getContextId}]: context not found!")
        respObs.onError(notFoundContextError(req.getContextId))

      case Some(ctx) =>
        try {
          val unloadModules =
            if (req.hasUpdateModules)
              req.getUpdateModules.getUnloadModulesList.asScala
                .map(ModuleName.assertFromString)
                .toSet
            else
              Set.empty[ModuleName]

          val loadModules =
            if (req.hasUpdateModules)
              req.getUpdateModules.getLoadModulesList.asScala
            else
              Seq.empty

          val unloadPackages =
            if (req.hasUpdatePackages)
              req.getUpdatePackages.getUnloadPackagesList.asScala
                .map(Ref.PackageId.assertFromString)
                .toSet
            else
              Set.empty[Ref.PackageId]

          val loadPackages =
            if (req.hasUpdatePackages)
              req.getUpdatePackages.getLoadPackagesList.asScala
            else
              Seq.empty

          ctx.update(
            unloadModules,
            loadModules,
            unloadPackages,
            loadPackages,
            req.getNoValidation,
          )

          resp.addAllLoadedModules(ctx.loadedModules().map(_.toString).asJava)
          resp.addAllLoadedPackages((ctx.loadedPackages(): Iterable[String]).asJava)
          respObs.onNext(resp.build)
          respObs.onCompleted()

        } catch {
          case e: archive.Error =>
            respObs.onError(Status.INVALID_ARGUMENT.withDescription(e.msg).asRuntimeException())
          case NonFatal(e) =>
            respObs.onError(Status.INTERNAL.withDescription(e.toString).asRuntimeException())
        }
    }
  }

}
