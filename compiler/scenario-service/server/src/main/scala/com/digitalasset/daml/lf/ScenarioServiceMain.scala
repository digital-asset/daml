// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import com.daml.lf.archive.Decode.ParseError
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ModuleName
import com.daml.lf.language.LanguageVersion
import com.daml.lf.scenario.api.v1.{Map => _, _}
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.netty.NettyServerBuilder

import scala.concurrent.ExecutionContext
import scala.util.{Success, Failure}
import scala.collection.concurrent.TrieMap
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object ScenarioServiceMain extends App {
  // default to 128MB
  val maxMessageSize = args.headOption.map(_.toInt).getOrElse(128 * 1024 * 1024)

  // Needed for the akka Ledger bindings used by DAML Script.
  val system = ActorSystem("ScriptService")
  implicit val sequencer: ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool("ScriptServicePool")(system)
  implicit val materializer: Materializer = Materializer(system)
  implicit val ec: ExecutionContext = system.dispatcher

  val server =
    NettyServerBuilder
      .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, 0)) // any free port
      .addService(new ScenarioService())
      .maxInboundMessageSize(maxMessageSize)
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
  }).start

  println("Server started.")
  server.awaitTermination()

}

object ScenarioService {
  private def notFoundContextError(id: Long): StatusRuntimeException =
    Status.NOT_FOUND.withDescription(s" context $id not found!").asRuntimeException
}

class ScenarioService(
    implicit ec: ExecutionContext,
    esf: ExecutionSequencerFactory,
    mat: Materializer)
    extends ScenarioServiceGrpc.ScenarioServiceImplBase {

  import ScenarioService._

  private val contexts = TrieMap.empty[Context.ContextId, Context]

  private def log(msg: String) =
    System.err.println("ScenarioService: " + msg)

  override def runScenario(
      req: RunScenarioRequest,
      respObs: StreamObserver[RunScenarioResponse],
  ): Unit = {
    val scenarioId = req.getScenarioId
    val contextId = req.getContextId
    //log(s"runScenario[$contextId]: $scenarioId")
    val response =
      contexts
        .get(contextId)
        .flatMap { context =>
          val packageId = scenarioId.getPackage.getSumCase match {
            case PackageIdentifier.SumCase.SELF =>
              context.homePackageId
            case PackageIdentifier.SumCase.PACKAGE_ID =>
              scenarioId.getPackage.getPackageId
            case PackageIdentifier.SumCase.SUM_NOT_SET =>
              throw new RuntimeException(
                s"Package id not set when running scenario, context id $contextId",
              )
          }
          context
            .interpretScenario(packageId, scenarioId.getName)
            .map {
              case (ledger, machine, errOrValue) =>
                val builder = RunScenarioResponse.newBuilder
                machine.withOnLedger("runScenario") { onLedger =>
                  errOrValue match {
                    case Left(err) =>
                      builder.setError(
                        new Conversions(
                          context.homePackageId,
                          ledger,
                          onLedger.ptx,
                          machine.traceLog,
                          onLedger.commitLocation,
                          machine.stackTrace())
                          .convertScenarioError(err),
                      )
                    case Right(value) =>
                      builder.setResult(
                        new Conversions(
                          context.homePackageId,
                          ledger,
                          onLedger.ptx,
                          machine.traceLog,
                          onLedger.commitLocation,
                          machine.stackTrace())
                          .convertScenarioResult(value))
                  }
                }
                builder.build
            }
        }

    response match {
      case None =>
        log(s"runScenario[$contextId]: $scenarioId not found")
        respObs.onError(notFoundContextError(req.getContextId))
      case Some(resp) =>
        respObs.onNext(resp)
        respObs.onCompleted()
    }
  }

  override def runScript(
      req: RunScenarioRequest,
      respObs: StreamObserver[RunScenarioResponse],
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
                s"Package id not set when running scenario, context id $contextId",
              )
          }
          context
            .interpretScript(packageId, scenarioId.getName)
            .map(_.map {
              case (ledger, (clientMachine, ledgerMachine), errOrValue) =>
                val builder = RunScenarioResponse.newBuilder
                ledgerMachine.withOnLedger("runScript") {
                  onLedger =>
                    errOrValue match {
                      case Left(err) =>
                        builder.setError(
                          new Conversions(
                            context.homePackageId,
                            ledger,
                            onLedger.ptx,
                            clientMachine.traceLog,
                            onLedger.commitLocation,
                            ledgerMachine.stackTrace())
                            .convertScenarioError(err),
                        )
                      case Right(value) =>
                        builder.setResult(
                          new Conversions(
                            context.homePackageId,
                            ledger,
                            onLedger.ptx,
                            clientMachine.traceLog,
                            onLedger.commitLocation,
                            ledgerMachine.stackTrace())
                            .convertScenarioResult(value))
                    }
                }
                builder.build
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
        respObs.onError(err)
    }
  }

  override def newContext(
      req: NewContextRequest,
      respObs: StreamObserver[NewContextResponse],
  ): Unit = {
    val lfVersion = LanguageVersion(
      LanguageVersion.Major.V1,
      LanguageVersion.Minor(req.getLfMinor)
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
          case e: ParseError =>
            respObs.onError(Status.INVALID_ARGUMENT.withDescription(e.error).asRuntimeException())
          case NonFatal(e) =>
            respObs.onError(Status.INTERNAL.withDescription(e.toString).asRuntimeException())
        }
    }
  }

}
