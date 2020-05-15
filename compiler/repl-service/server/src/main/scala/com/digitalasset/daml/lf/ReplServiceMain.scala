// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.repl

import akka.actor.ActorSystem
import akka.stream._
import com.daml.api.util.TimeProvider
import com.daml.auth.TokenHolder
import com.daml.lf.PureCompiledPackages
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script._
import com.daml.lf.language.Ast._
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.{Compiler, SValue, SExpr, SError}
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import java.net.{InetAddress, InetSocketAddress}
import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.logging.{Level, Logger}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}
import scalaz.syntax.tag._

object ReplServiceMain extends App {
  case class Config(
      portFile: Path,
      ledgerHost: String,
      ledgerPort: Int,
      accessTokenFile: Option[Path],
      maxInboundMessageSize: Int,
      tlsConfig: Option[TlsConfiguration],
  )
  object Config {
    private def validatePath(path: String, message: String): Either[String, Unit] = {
      val readable = Try(Paths.get(path).toFile.canRead).getOrElse(false)
      if (readable) Right(()) else Left(message)
    }
    private val parser = new scopt.OptionParser[Config]("repl-service") {
      opt[String]("port-file")
        .required()
        .action((portFile, c) => c.copy(portFile = Paths.get(portFile)))

      opt[String]("ledger-host")
        .required()
        .action((host, c) => c.copy(ledgerHost = host))

      opt[Int]("ledger-port")
        .required()
        .action((port, c) => c.copy(ledgerPort = port))

      opt[String]("access-token-file")
        .optional()
        .action { (tokenFile, c) =>
          c.copy(accessTokenFile = Some(Paths.get(tokenFile)))
        }

      opt[String]("pem")
        .optional()
        .text("TLS: The pem file to be used as the private key.")
        .validate(path => validatePath(path, "The file specified via --pem does not exist"))
        .action((path, arguments) =>
          arguments.copy(tlsConfig = arguments.tlsConfig.fold(
            Some(TlsConfiguration(true, None, Some(new File(path)), None)))(c =>
            Some(c.copy(keyFile = Some(new File(path)))))))

      opt[String]("crt")
        .optional()
        .text("TLS: The crt file to be used as the cert chain. Required for client authentication.")
        .validate(path => validatePath(path, "The file specified via --crt does not exist"))
        .action((path, arguments) =>
          arguments.copy(tlsConfig = arguments.tlsConfig.fold(
            Some(TlsConfiguration(true, None, Some(new File(path)), None)))(c =>
            Some(c.copy(keyFile = Some(new File(path)))))))

      opt[String]("cacrt")
        .optional()
        .text("TLS: The crt file to be used as the the trusted root CA.")
        .validate(path => validatePath(path, "The file specified via --cacrt does not exist"))
        .action((path, arguments) =>
          arguments.copy(tlsConfig = arguments.tlsConfig.fold(
            Some(TlsConfiguration(true, None, None, Some(new File(path)))))(c =>
            Some(c.copy(trustCertCollectionFile = Some(new File(path)))))))

      opt[Unit]("tls")
        .optional()
        .text("TLS: Enable tls. This is redundant if --pem, --crt or --cacrt are set")
        .action((path, arguments) =>
          arguments.copy(tlsConfig =
            arguments.tlsConfig.fold(Some(TlsConfiguration(true, None, None, None)))(Some(_))))

      opt[Int]("max-inbound-message-size")
        .action((x, c) => c.copy(maxInboundMessageSize = x))
        .optional()
        .text(
          s"Optional max inbound message size in bytes. Defaults to ${RunnerConfig.DefaultMaxInboundMessageSize}")
    }
    def parse(args: Array[String]): Option[Config] =
      parser.parse(
        args,
        Config(
          portFile = null,
          ledgerHost = null,
          ledgerPort = 0,
          accessTokenFile = None,
          tlsConfig = None,
          maxInboundMessageSize = RunnerConfig.DefaultMaxInboundMessageSize,
        )
      )
  }

  val config = Config.parse(args) match {
    case Some(conf) => conf
    case None =>
      println("Internal error: invalid command line arguments passed to repl service.")
      sys.exit(1)
  }
  val maxMessageSize = 128 * 1024 * 1024

  val system = ActorSystem("Repl")
  implicit val sequencer: ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool("ReplPool")(system)
  implicit val materializer: Materializer = Materializer(system)
  implicit val ec: ExecutionContext = system.dispatcher

  val participantParams =
    Participants(Some(ApiParameters(config.ledgerHost, config.ledgerPort)), Map.empty, Map.empty)
  val applicationId = ApplicationId("daml repl")
  val clientConfig = LedgerClientConfiguration(
    applicationId = applicationId.unwrap,
    ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
    commandClient = CommandClientConfiguration.default,
    sslContext = config.tlsConfig.flatMap(_.client),
    token = config.accessTokenFile.map(new TokenHolder(_)).flatMap(_.token),
  )
  val clients = Await.result(
    Runner.connect(participantParams, clientConfig, config.maxInboundMessageSize),
    30.seconds)

  val server =
    NettyServerBuilder
      .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
      .addService(new ReplService(clients, ec, materializer))
      .maxInboundMessageSize(maxMessageSize)
      .build
  server.start()
  Files.write(config.portFile, Seq(server.getPort.toString).asJava)

  // Bump up the log level
  Logger.getLogger("io.grpc").setLevel(Level.ALL)
  server.awaitTermination()

}

class ReplService(
    val clients: Participants[ScriptLedgerClient],
    ec: ExecutionContext,
    mat: Materializer)
    extends ReplServiceGrpc.ReplServiceImplBase {
  var packages: Map[PackageId, Package] = Map.empty
  var compiledDefinitions: Map[SDefinitionRef, SExpr] = Map.empty
  var results: Seq[SValue] = Seq()
  implicit val ec_ = ec
  implicit val mat_ = mat

  private val homePackageId: PackageId = PackageId.assertFromString("-homePackageId-")

  override def loadPackage(
      req: LoadPackageRequest,
      respObs: StreamObserver[LoadPackageResponse]): Unit = {
    val (pkgId, pkg) = Decode.decodeArchiveFromInputStream(req.getPackage.newInput)
    packages = packages + (pkgId -> pkg)
    compiledDefinitions = compiledDefinitions ++ Compiler(packages, Compiler.NoProfile)
      .unsafeCompilePackage(pkgId)
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
    // TODO[AH] Provide daml-script package id from REPL client.
    val (scriptPackageId, _) = packages.find {
      case (pkgId, pkg) => pkg.modules.contains(DottedName.assertFromString("Daml.Script"))
    }.get

    var scriptExpr: SExpr = SEVal(
      LfDefRef(
        Identifier(homePackageId, QualifiedName(mod.name, DottedName.assertFromString("expr")))))
    if (!results.isEmpty) {
      scriptExpr = SEApp(scriptExpr, results.map(SEValue(_)).toArray)
    }

    val allPkgs = packages + (homePackageId -> pkg)
    val defs = Compiler(allPkgs, Compiler.NoProfile).unsafeCompilePackage(homePackageId)
    val compiledPackages =
      PureCompiledPackages(allPkgs, compiledDefinitions ++ defs, Compiler.NoProfile)
    val runner = new Runner(
      compiledPackages,
      Script.Action(scriptExpr, ScriptIds(scriptPackageId)),
      ApplicationId("daml repl"),
      TimeProvider.UTC)
    runner.runWithClients(clients).onComplete {
      case Failure(e: SError.SError) =>
        // The error here is already printed by the logger in stepToValue.
        // No need to print anything here.
        respObs.onError(e)
      case Failure(e) =>
        println(s"$e")
        respObs.onError(e)
      case Success(v) =>
        results = results ++ Seq(v)
        respObs.onNext(RunScriptResponse.newBuilder.build)
        respObs.onCompleted
    }
  }

  override def clearResults(
      req: ClearResultsRequest,
      respObs: StreamObserver[ClearResultsResponse]): Unit = {
    results = Seq()
    respObs.onNext(ClearResultsResponse.newBuilder.build)
    respObs.onCompleted
  }
}
