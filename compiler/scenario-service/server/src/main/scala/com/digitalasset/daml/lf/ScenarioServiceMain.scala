// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import java.time.Instant
import scala.util.Random
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
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

sealed abstract class ScriptStream {
  def sendStatus(status: ScenarioStatus): Unit;
  def sendFinalResponse(result: Either[ScenarioError, ScenarioResult]): Unit;
  def sendError(t: Throwable): Unit;
}

// All methods that call to `internal` MUST synchronize over the response, otherwise we risk calling it in multiple threads and throwing a netty error.
object ScriptStream {
  final case class WithoutStatus(internal: StreamObserver[RunScenarioResponse])
      extends ScriptStream {
    override def sendFinalResponse(finalResponse: Either[ScenarioError, ScenarioResult]): Unit =
      internal.synchronized {
        val message = finalResponse match {
          case Left(error: ScenarioError) => RunScenarioResponse.newBuilder.setError(error).build
          case Right(result: ScenarioResult) =>
            RunScenarioResponse.newBuilder.setResult(result).build
        }
        internal.onNext(message)
        internal.onCompleted()
      }

    override def sendStatus(status: ScenarioStatus): Unit = {}
    override def sendError(t: Throwable): Unit = internal.synchronized(internal.onError(t))
  }

  final case class WithStatus(internal: StreamObserver[RunScenarioResponseOrStatus])
      extends ScriptStream {
    override def sendFinalResponse(finalResponse: Either[ScenarioError, ScenarioResult]): Unit =
      internal.synchronized {
        val message = finalResponse match {
          case Left(error: ScenarioError) =>
            RunScenarioResponseOrStatus.newBuilder.setError(error).build
          case Right(result: ScenarioResult) =>
            RunScenarioResponseOrStatus.newBuilder.setResult(result).build
        }
        internal.onNext(message)
        internal.onCompleted()
      }

    override def sendStatus(status: ScenarioStatus): Unit = internal.synchronized {
      val message = RunScenarioResponseOrStatus.newBuilder.setStatus(status).build
      internal.onNext(message)
    }
    override def sendError(t: Throwable): Unit = internal.synchronized(internal.onError(t))
  }
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
      if (req.hasStart) {
        runLive(
          req.getStart,
          ScriptStream.WithoutStatus(respObs),
          { case (ctx, pkgId, name) =>
            Future(ctx.interpretScenario(pkgId, name))
          },
        )
      }
    } else {
      log("Rejected scenario gRPC request.")
      respObs.onError(new UnsupportedOperationException("Scenarios are disabled"))
    }
  }

  override def runLiveScenario(
      req: RunScenarioRequest,
      respObs: StreamObserver[RunScenarioResponseOrStatus],
  ): Unit = {
    val stream = ScriptStream.WithStatus(respObs)
    if (enableScenarios) {
      if (req.hasStart) {
        runLive(
          req.getStart,
          stream,
          { case (ctx, pkgId, name) => Future(ctx.interpretScenario(pkgId, name)) },
        )
      }
    } else {
      log("Rejected scenario gRPC request.")
      stream.sendError(new UnsupportedOperationException("Scenarios are disabled"))
    }
  }

  override def runScript(
      req: RunScenarioRequest,
      respObs: StreamObserver[RunScenarioResponse],
  ): Unit =
    if (req.hasStart) {
      runLive(
        req.getStart,
        ScriptStream.WithoutStatus(respObs),
        { case (ctx, pkgId, name) => ctx.interpretScript(pkgId, name) },
      )
    }

  import java.io._
  override def runLiveScript(
      respObs: StreamObserver[RunScenarioResponseOrStatus]
  ): StreamObserver[RunScenarioRequest] = {
    val id: Int = Random.nextInt()
    def append(idx: Int, msg: String): Unit = {
      val file = new File(f"""/home/dylan-thinnes/root/daml-script-in-ide/log-output-$idx""")
      val fw = new BufferedWriter(new FileWriter(file, true))
      try fw.write(f"""$id $msg\n""")
      finally fw.close()
    }

    append(6, "runLiveScript called")
    var cancelled = false
    new StreamObserver[RunScenarioRequest] {
      override def onNext(req: RunScenarioRequest): Unit = {
        if (req.hasCancel) {
          append(6, "cancel called")
          cancelled = true
        } else if (req.hasStart) {
          append(6, "next called")
          runLive(
            req.getStart,
            ScriptStream.WithStatus(respObs),
            { case (ctx, pkgId, name) =>
                ctx.interpretScript(
                  pkgId,
                  name,
                  () => {
                    if (cancelled) append(6, "cancel function called, cancelling")
                    cancelled
                  }
                )
            },
          )
        }
      }

      override def onError(t: Throwable): Unit = {
        println("Custom onError Received ERROR")
      }

      override def onCompleted(): Unit = {
        println("Completed on client side!")
        respObs.onCompleted()
      }
    }
  }

  private def runLive(
      req: RunScenarioStart,
      respStream: ScriptStream,
      interpret: (Context, String, String) => Future[Option[ScenarioRunner.ScenarioResult]],
  ): Unit = {
    val scenarioId = req.getScenarioId
    val contextId = req.getContextId
    val response: Future[Option[Either[ScenarioError, ScenarioResult]]] =
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
                Left(
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
              case success: ScenarioRunner.ScenarioSuccess =>
                Right(
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
            })
        }
        .map(_.flatten)

    Future {
      val startedAt = Instant.now.toEpochMilli

      var millisPassed: Long = 0
      def sleepRandom() = {
        val delta: Long = 300 + (Random.nextDouble() * 300).toLong
        Thread.sleep(delta)
        millisPassed += delta
      }

      sleepRandom()
      while (!response.isCompleted) {
        respStream.sendStatus(
          ScenarioStatus.newBuilder
            .setMillisecondsPassed(millisPassed)
            .setStartedAt(startedAt)
            .build
        )
        sleepRandom()
      }
    }

    response.onComplete {
      case Success(None) =>
        log(s"runScript[$contextId]: $scenarioId not found")
        respStream.sendError(notFoundContextError(req.getContextId))
      case Success(Some(resp)) =>
        respStream.sendFinalResponse(resp)
      case Failure(err) =>
        System.err.println(err)
        respStream.sendError(err)
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
    val ctx = Context.newContext(lfVersion, req.getEvaluationTimeout.seconds)
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
