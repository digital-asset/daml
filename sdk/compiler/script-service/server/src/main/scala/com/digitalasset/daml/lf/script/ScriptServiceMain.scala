// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.script

import org.apache.pekko.actor.ActorSystem
import com.daml.grpc.adapter.{PekkoExecutionSequencerPool, ExecutionSequencerFactory}
import org.apache.pekko.stream.Materializer

import java.net.{InetAddress, InetSocketAddress}
import java.util.logging.{Level, Logger}
import scalaz.std.option._
import scalaz.std.scalaFuture._
import scalaz.syntax.traverse._
import com.digitalasset.daml.lf.archive
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.ModuleName
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.script.api.v1.{Map => _, _}
import com.daml.logging.LoggingContext
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.netty.NettyServerBuilder

import java.time.Instant
import scala.util.Random
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private final case class ScriptServiceConfig(
    maxInboundMessageSize: Int
)

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
private object ScriptServiceConfig {
  // We default to MAXINT as we rely on the ledger to manage the message size
  val DefaultMaxInboundMessageSize: Int = Int.MaxValue

  val parser = new scopt.OptionParser[ScriptServiceConfig]("script-service") {
    head("script-service")

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to ${DefaultMaxInboundMessageSize}."
      )
  }

  def parse(args: Array[String]): Option[ScriptServiceConfig] =
    parser.parse(
      args,
      ScriptServiceConfig(
        maxInboundMessageSize = DefaultMaxInboundMessageSize
      ),
    )
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
object ScriptServiceMain extends App {
  ScriptServiceConfig.parse(args) match {
    case None => sys.exit(1)
    case Some(config) =>
      // Needed for the pekko Ledger bindings used by Daml Script.
      val system = ActorSystem("ScriptService")
      implicit val sequencer: ExecutionSequencerFactory =
        new PekkoExecutionSequencerPool("ScriptServicePool")(system)
      implicit val materializer: Materializer = Materializer(system)
      implicit val ec: ExecutionContext = system.dispatcher
      LoggingContext.newLoggingContext { implicit lc: LoggingContext =>
        val server =
          NettyServerBuilder
            .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, 0)) // any free port
            .addService(new ScriptService())
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
            System.err.println("ScriptService: stdin closed, terminating server.")
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

object ScriptService {
  private def notFoundContextError(id: Long): StatusRuntimeException =
    Status.NOT_FOUND.withDescription(s" context $id not found!").asRuntimeException

  private def unknownMajorVersion(str: String): StatusRuntimeException =
    Status.INVALID_ARGUMENT.withDescription(s"unknonw major LF version: $str").asRuntimeException
}

sealed abstract class ScriptStream {
  def sendStatus(status: ScriptStatus): Unit;
  def sendFinalResponse(result: Either[ScriptError, ScriptResult]): Unit;
  def sendError(t: Throwable): Unit;
}

// All methods that call to `internal` MUST synchronize over the response, otherwise we risk calling it in multiple threads and throwing a netty error.
object ScriptStream {
  final case class WithoutStatus(internal: StreamObserver[RunScriptResponse]) extends ScriptStream {
    override def sendFinalResponse(finalResponse: Either[ScriptError, ScriptResult]): Unit =
      internal.synchronized {
        val message = finalResponse match {
          case Left(error: ScriptError) => RunScriptResponse.newBuilder.setError(error).build
          case Right(result: ScriptResult) =>
            RunScriptResponse.newBuilder.setResult(result).build
        }
        finalResponse match {
          case Left(error: ScriptError) if error.hasCancelledByRequest =>
            println(f"Script cancelled.")
          case _ => {}
        }
        internal.onNext(message)
        internal.onCompleted()
        println(f"Script finished.")
      }

    override def sendStatus(status: ScriptStatus): Unit = {}
    override def sendError(t: Throwable): Unit = internal.synchronized(internal.onError(t))
  }

  final case class WithStatus(internal: StreamObserver[RunScriptResponseOrStatus])
      extends ScriptStream {
    override def sendFinalResponse(finalResponse: Either[ScriptError, ScriptResult]): Unit =
      internal.synchronized {
        val message = finalResponse match {
          case Left(error: ScriptError) =>
            RunScriptResponseOrStatus.newBuilder.setError(error).build
          case Right(result: ScriptResult) =>
            RunScriptResponseOrStatus.newBuilder.setResult(result).build
        }
        finalResponse match {
          case Left(error: ScriptError) if error.hasCancelledByRequest =>
            println(f"Script cancelled.")
          case _ => {}
        }
        internal.onNext(message)
        internal.onCompleted()
        println(f"Script finished.")
      }

    override def sendStatus(status: ScriptStatus): Unit = internal.synchronized {
      val message = RunScriptResponseOrStatus.newBuilder.setStatus(status).build
      internal.onNext(message)
    }
    override def sendError(t: Throwable): Unit = internal.synchronized(internal.onError(t))
  }
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class ScriptService(implicit
    ec: ExecutionContext,
    esf: ExecutionSequencerFactory,
    mat: Materializer,
    lc: LoggingContext,
) extends ScriptServiceGrpc.ScriptServiceImplBase {

  import ScriptService._

  private val contexts = TrieMap.empty[Context.ContextId, Context]

  private def log(msg: String) =
    System.err.println("ScriptService: " + msg)

  override def runScript(
      req: RunScriptRequest,
      respObs: StreamObserver[RunScriptResponse],
  ): Unit =
    if (req.hasStart) {
      runLive(
        req.getStart,
        ScriptStream.WithoutStatus(respObs),
        { case (ctx, pkgId, name) => ctx.interpretScript(pkgId, name) },
      )
    }

  override def runLiveScript(
      respObs: StreamObserver[RunScriptResponseOrStatus]
  ): StreamObserver[RunScriptRequest] = {
    var cancelled = false
    println(f"Connection started.")
    new StreamObserver[RunScriptRequest] {
      override def onNext(req: RunScriptRequest): Unit = {
        if (req.hasCancel) {
          println(f"Script cancelling.")
          cancelled = true
        } else if (req.hasStart) {
          println(f"Script started.")
          runLive(
            req.getStart,
            ScriptStream.WithStatus(respObs),
            { case (ctx, pkgId, name) => ctx.interpretScript(pkgId, name, () => cancelled) },
          )
        }
      }

      override def onError(t: Throwable): Unit = {
        println(f"Received error $t")
      }

      override def onCompleted(): Unit = {}
    }
  }

  private def runLive(
      req: RunScriptStart,
      respStream: ScriptStream,
      interpret: (Context, String, String) => Future[Option[IdeLedgerRunner.ScriptResult]],
  ): Unit = {
    val scriptId = req.getScriptId
    val contextId = req.getContextId
    val response: Future[Option[Either[ScriptError, ScriptResult]]] =
      contexts
        .get(contextId)
        .traverse { context =>
          val packageId = scriptId.getPackage.getSumCase match {
            case PackageIdentifier.SumCase.SELF =>
              context.homePackageId
            case PackageIdentifier.SumCase.PACKAGE_ID =>
              scriptId.getPackage.getPackageId
            case PackageIdentifier.SumCase.SUM_NOT_SET =>
              throw new RuntimeException(
                s"Package id not set when running script, context id $contextId"
              )
          }
          interpret(context, packageId, scriptId.getName)
            .map(_.map {
              case error: IdeLedgerRunner.ScriptError =>
                Left(
                  new Conversions(
                    context.homePackageId,
                    error.ledger,
                    error.currentSubmission.map(_.ptx),
                    error.traceLog,
                    error.warningLog,
                    error.currentSubmission.flatMap(_.commitLocation),
                    error.stackTrace,
                    context.devMode,
                  )
                    .convertScriptError(error.error)
                )
              case success: IdeLedgerRunner.ScriptSuccess =>
                Right(
                  new Conversions(
                    context.homePackageId,
                    success.ledger,
                    None,
                    success.traceLog,
                    success.warningLog,
                    None,
                    ImmArray.Empty,
                    context.devMode,
                  )
                    .convertScriptResult(success.resultValue)
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
          ScriptStatus.newBuilder
            .setMillisecondsPassed(millisPassed)
            .setStartedAt(startedAt)
            .build
        )
        sleepRandom()
      }
    }

    response.onComplete {
      case Success(None) =>
        log(s"runScript[$contextId]: $scriptId not found")
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
    LanguageVersion.Major.fromString(req.getLfMajor) match {
      case Some(majorVersion) =>
        val lfVersion = LanguageVersion(
          majorVersion,
          LanguageVersion.Minor(req.getLfMinor),
        )
        val ctx = Context.newContext(lfVersion, req.getEvaluationTimeout.seconds)
        contexts += (ctx.contextId -> ctx)
        val response = NewContextResponse.newBuilder.setContextId(ctx.contextId).build
        respObs.onNext(response)
        respObs.onCompleted()
      case None =>
        respObs.onError(unknownMajorVersion(req.getLfMajor))
    }
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
