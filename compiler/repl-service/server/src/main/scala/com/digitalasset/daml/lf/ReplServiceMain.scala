// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.repl

import akka.actor.ActorSystem
import akka.stream._
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.script._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.{SValue, SExpr}
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.services.commands.CommandUpdater
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.{Files, Paths}
import java.util.logging.{Level, Logger}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}
import scalaz.syntax.tag._

object ReplServiceMain extends App {
  val portFile = args(0)
  val ledgerHost = args(1)
  val ledgerPort = args(2).toInt
  val maxMessageSize = 128 * 1024 * 1024

  val system = ActorSystem("Repl")
  implicit val sequencer: ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool("ReplPool")(system)
  implicit val materializer: Materializer = Materializer(system)
  implicit val ec: ExecutionContext = system.dispatcher

  val participantParams =
    Participants(Some(ApiParameters(ledgerHost, ledgerPort)), Map.empty, Map.empty)
  val applicationId = ApplicationId("daml repl")
  val clientConfig = LedgerClientConfiguration(
    applicationId = applicationId.unwrap,
    ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
    token = None,
  )
  val clients = Await.result(Runner.connect(participantParams, clientConfig), 30.seconds)

  val server =
    NettyServerBuilder
      .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
      .addService(new ReplService(clients, ec, materializer))
      .maxInboundMessageSize(maxMessageSize)
      .build
  server.start()
  Files.write(Paths.get(portFile), Seq(server.getPort.toString).asJava)

  // Bump up the log level
  Logger.getLogger("io.grpc").setLevel(Level.ALL)
  server.awaitTermination()

}

class ReplService(val clients: Participants[LedgerClient], ec: ExecutionContext, mat: Materializer)
    extends ReplServiceGrpc.ReplServiceImplBase {
  var packages: Map[PackageId, Package] = Map.empty
  var results: Seq[SValue] = Seq()
  implicit val ec_ = ec
  implicit val mat_ = mat

  private val homePackageId: PackageId = PackageId.assertFromString("-homePackageId-")

  override def loadPackage(
      req: LoadPackageRequest,
      respObs: StreamObserver[LoadPackageResponse]): Unit = {
    val (pkgId, pkg) = Decode.decodeArchiveFromInputStream(req.getPackage.newInput)
    packages = packages + (pkgId -> pkg)
    respObs.onNext(LoadPackageResponse.newBuilder.build)
    respObs.onCompleted()
  }

  override def runScript(
      req: RunScriptRequest,
      respObs: StreamObserver[RunScriptResponse]): Unit = {
    val lfVer = LanguageVersion(
      LanguageVersion.Major.V1,
      LanguageVersion.Minor fromProtoIdentifier req.getMinor)
    val dop: Decode.OfPackage[_] = Decode.decoders
      .lift(lfVer)
      .getOrElse(throw new RuntimeException(s"No decode support for LF ${lfVer.pretty}"))
      .decoder
    val lfScenarioModule =
      dop.protoScenarioModule(Decode.damlLfCodedInputStream(req.getDamlLf1.newInput))
    val mod: Ast.Module = dop.decodeScenarioModule(homePackageId, lfScenarioModule)
    // For now we only include the module of the current line
    // we probably need to extend this to merge the
    // modules from each line.
    val pkg = Package(Seq(mod), Seq(), None)
    val dar = Dar((homePackageId, pkg), packages.toList)
    val commandUpdater = new CommandUpdater(
      timeProviderO = Some(TimeProvider.UTC),
      ttl = java.time.Duration.ofSeconds(30),
      overrideTtl = true)
    val runner = new Runner(dar, ApplicationId("daml repl"), commandUpdater, TimeProvider.UTC)
    var scriptExpr: SExpr = SEVal(
      LfDefRef(
        Identifier(homePackageId, QualifiedName(mod.name, DottedName.assertFromString("expr")))),
      None)
    if (!results.isEmpty) {
      scriptExpr = SEApp(scriptExpr, results.map(SEValue(_)).toArray)
    }
    runner.runExpr(clients, scriptExpr).onComplete {
      case Failure(e) => respObs.onError(e)
      case Success(v) =>
        results = results ++ Seq(v)
        respObs.onNext(RunScriptResponse.newBuilder.build)
        respObs.onCompleted
    }
  }
}
