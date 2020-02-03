// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.scenario

import java.util.logging.{Level, Logger}

import com.digitalasset.daml.lf.archive.Decode.ParseError
import com.digitalasset.daml.lf.scenario.api.v1.{Map => _, _}
import io.grpc.stub.StreamObserver
import io.grpc.{ServerBuilder, Status, StatusRuntimeException}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object ScenarioServiceMain extends App {
  // default to 128MB
  val maxMessageSize = args.headOption.map(_.toInt).getOrElse(128 * 1024 * 1024)
  val server =
    ServerBuilder
      .forPort(0) // any free port
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

class ScenarioService extends ScenarioServiceGrpc.ScenarioServiceImplBase {

  import ScenarioService._

  @volatile private var contexts = Map.empty[Context.ContextId, Context]

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
                errOrValue match {
                  case Left(err) =>
                    builder.setError(
                      Conversions(context.homePackageId)
                        .convertScenarioError(ledger, machine, err),
                    )
                  case Right(value) =>
                    val conv = Conversions(context.homePackageId)
                    builder.setResult(conv.convertScenarioResult(ledger, machine, value))
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

  override def newContext(
      req: NewContextRequest,
      respObs: StreamObserver[NewContextResponse],
  ): Unit = {
    val ctx = Context.newContext()
    contexts = contexts + (ctx.contextId -> ctx)
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
        contexts = contexts + (clonedCtx.contextId -> clonedCtx)
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
      contexts = contexts - ctxId
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
    val ctxIds = req.getContextIdsList.asScala.toSet
    contexts = contexts.filterKeys(ctxId => ctxIds.contains(ctxId));
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
            else
              Seq.empty

          val loadModules =
            if (req.hasUpdateModules)
              req.getUpdateModules.getLoadModulesList.asScala
            else
              Seq.empty

          val unloadPackages =
            if (req.hasUpdatePackages)
              req.getUpdatePackages.getUnloadPackagesList.asScala
            else
              Seq.empty

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
