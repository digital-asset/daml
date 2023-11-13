// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy.repl

import akka.actor.ActorSystem
import akka.stream._
import com.daml.auth.TokenHolder
import com.daml.lf.PureCompiledPackages
import com.daml.lf.data.assertRight
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script._
import com.daml.lf.language.Ast._
import com.daml.lf.language.{
  Ast,
  LanguageMajorVersion,
  LanguageVersion,
  PackageInterface,
  Util => AstUtil,
}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.{Compiler, SDefinition, SError, SValue}
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.tls.{TlsConfiguration, TlsConfigurationCli}
import com.daml.scalautil.Statement.discard
import com.typesafe.scalalogging.StrictLogging
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver

import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.{Files, Path, Paths}
import java.util.logging.{Level, Logger}
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.engine.script.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.speedy.iterable.SExprIterable

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

object ReplServiceMain extends App {
  final class NonSerializableValue extends RuntimeException {
    override def toString = "Cannot convert non-serializable value to JSON"
  }

  case class Config(
      portFile: Path,
      ledgerHost: Option[String],
      ledgerPort: Option[Int],
      accessTokenFile: Option[Path],
      applicationId: Option[ApplicationId],
      maxInboundMessageSize: Int,
      tlsConfig: TlsConfiguration,
      // optional so we can detect if both --static-time and --wall-clock-time are passed.
      timeMode: Option[ScriptTimeMode],
      majorLanguageVersion: LanguageMajorVersion,
  )
  object Config {
    private def setTimeMode(
        config: Config,
        timeMode: ScriptTimeMode,
    ): Config = {
      if (config.timeMode.exists(_ != timeMode)) {
        throw new IllegalStateException(
          "Static time mode (`-s`/`--static-time`) and wall-clock time mode (`-w`/`--wall-clock-time`) are mutually exclusive. The time mode must be unambiguous."
        )
      }
      config.copy(timeMode = Some(timeMode))
    }
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements")) // scopt builders
    private val parser = new scopt.OptionParser[Config]("repl-service") {
      opt[String]("port-file")
        .required()
        .action((portFile, c) => c.copy(portFile = Paths.get(portFile)))

      opt[String]("ledger-host")
        .optional()
        .action((host, c) => c.copy(ledgerHost = Some(host)))

      opt[Int]("ledger-port")
        .optional()
        .action((port, c) => c.copy(ledgerPort = Some(port)))

      opt[String]("access-token-file")
        .optional()
        .action { (tokenFile, c) =>
          c.copy(accessTokenFile = Some(Paths.get(tokenFile)))
        }

      opt[String]("application-id")
        .optional()
        .action { (appId, c) =>
          c.copy(applicationId = Some(ApplicationId(appId)))
        }

      TlsConfigurationCli.parse(this, colSpacer = "        ")((f, c) =>
        c copy (tlsConfig = f(c.tlsConfig))
      )

      opt[Int]("max-inbound-message-size")
        .action((x, c) => c.copy(maxInboundMessageSize = x))
        .optional()
        .text(
          s"Optional max inbound message size in bytes. Defaults to ${RunnerMainConfig.DefaultMaxInboundMessageSize}"
        )

      opt[Unit]('w', "wall-clock-time")
        .action { (_, c) =>
          setTimeMode(c, ScriptTimeMode.WallClock)
        }
        .text("Use wall clock time (UTC).")

      opt[Unit]('s', "static-time")
        .action { (_, c) =>
          setTimeMode(c, ScriptTimeMode.Static)
        }
        .text("Use static time.")

      implicit val majorLanguageVersionRead: scopt.Read[LanguageMajorVersion] =
        scopt.Read.reads(s =>
          LanguageMajorVersion.fromString(s) match {
            case Some(v) => v
            case None => throw new IllegalArgumentException(s"$s is not a valid major LF version")
          }
        )
      opt[LanguageMajorVersion]('v', "major-lf-version")
        .optional()
        .valueName("version")
        .text("the major version of LF to use")
        .action((v, c) => c.copy(majorLanguageVersion = v))

      checkConfig(c =>
        (c.ledgerHost, c.ledgerPort) match {
          case (Some(_), None) =>
            failure("Must specified either both --ledger-host and --ledger-port or neither")
          case (None, Some(_)) =>
            failure("Must specified either both --ledger-host and --ledger-port or neither")
          case _ => success
        }
      )
    }
    def parse(args: Array[String]): Option[Config] =
      parser.parse(
        args,
        Config(
          portFile = null,
          ledgerHost = None,
          ledgerPort = None,
          accessTokenFile = None,
          tlsConfig = TlsConfiguration(false, None, None, None),
          maxInboundMessageSize = RunnerMainConfig.DefaultMaxInboundMessageSize,
          timeMode = None,
          applicationId = None,
          majorLanguageVersion = LanguageMajorVersion.V1,
        ),
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

  val tokenHolder = config.accessTokenFile.map(new TokenHolder(_))
  val defaultParticipant = (config.ledgerHost, config.ledgerPort) match {
    case (Some(host), Some(port)) =>
      Some(ApiParameters(host, port, tokenHolder.flatMap(_.token), config.applicationId))
    case _ => None
  }
  val participantParams =
    Participants(defaultParticipant, Map.empty, Map.empty)
  val clients = Await.result(
    Runner
      .connect(participantParams, config.tlsConfig, config.maxInboundMessageSize),
    30.seconds,
  )
  val timeMode = config.timeMode.getOrElse(ScriptTimeMode.WallClock)

  val server =
    NettyServerBuilder
      .forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
      .addService(
        new ReplService(clients, timeMode, ec, sequencer, materializer, config.majorLanguageVersion)
      )
      .maxInboundMessageSize(maxMessageSize)
      .build
      .start
  discard[Path](Files.write(config.portFile, Seq(server.getPort.toString).asJava))

  // Bump up the log level
  Logger.getLogger("io.grpc").setLevel(Level.ALL)
  server.awaitTermination()

}

object ReplService {
  private val homePackageId: PackageId = PackageId.assertFromString("-homePackageId-")

  private def moduleRefs(v: SValue): Set[ModuleName] = {
    def moduleRefs(acc: Set[ModuleName], e: SExpr): Set[ModuleName] =
      e match {
        case SEVal(ref) =>
          if (ref.packageId == homePackageId) {
            acc + ref.modName
          } else {
            acc
          }
        case e => SExprIterable(e).foldLeft(acc)(moduleRefs)
      }

    SExprIterable(v).foldLeft(Set.empty[ModuleName])(moduleRefs)
  }
}

class ReplService(
    val clients: Participants[ScriptLedgerClient],
    timeMode: ScriptTimeMode,
    ec: ExecutionContext,
    esf: ExecutionSequencerFactory,
    mat: Materializer,
    val majorLanguageVersion: LanguageMajorVersion,
) extends ReplServiceGrpc.ReplServiceImplBase
    with StrictLogging {
  var signatures: Map[PackageId, PackageSignature] = Map.empty
  var compiledDefinitions: Map[SDefinitionRef, SDefinition] = Map.empty
  var results: Seq[SValue] = Seq()
  var mainModules: Map[ModuleName, Ast.Module] = Map.empty
  implicit val _ec: ExecutionContext = ec
  implicit val _esf: ExecutionSequencerFactory = esf
  implicit val _mat: Materializer = mat
  import ReplService._

  private val compilerConfig = Compiler.Config
    .Default(majorLanguageVersion)
    .copy(stacktracing = Compiler.FullStackTrace)

  private[this] def validatePackagesLanguageVersion(pkgMap: Map[PackageId, Package]): Unit = {
    val invalidPackages =
      pkgMap.view.mapValues(_.languageVersion.major).filter(_._2 != majorLanguageVersion)
    if (invalidPackages.nonEmpty) {
      throw new IllegalArgumentException(
        "the following packages don't have expected major LF version "
          + s"${majorLanguageVersion}: ${invalidPackages}"
      )
    }
  }

  override def loadPackages(
      req: LoadPackagesRequest,
      respObs: StreamObserver[LoadPackagesResponse],
  ): Unit = {
    val pkgMap =
      req.getPackagesList.asScala.view.map(archive.ArchiveDecoder.assertFromByteString).toMap
    validatePackagesLanguageVersion(pkgMap)
    val newSignatures = signatures ++ AstUtil.toSignatures(pkgMap)
    val newCompiledDefinitions = compiledDefinitions ++
      assertRight(
        Compiler.compilePackages(new PackageInterface(newSignatures), pkgMap, compilerConfig)
      )
    signatures = newSignatures
    compiledDefinitions = newCompiledDefinitions
    respObs.onNext(LoadPackagesResponse.newBuilder.build)
    respObs.onCompleted()
  }

  override def runScript(
      req: RunScriptRequest,
      respObs: StreamObserver[RunScriptResponse],
  ): Unit = {
    val lfVer = LanguageVersion(majorLanguageVersion, LanguageVersion.Minor(req.getMinor))
    val mod = archive.moduleDecoder(lfVer, homePackageId).assertFromByteString(req.getDamlLf1)
    val pkg = Package(mainModules.updated(mod.name, mod), Set.empty, lfVer, None)
    // TODO[AH] Provide daml-script package id from REPL client.
    val scriptPackageId = this.signatures
      .collectFirst {
        case (pkgId, pkg) if pkg.modules.contains(DottedName.assertFromString("Daml.Script")) =>
          pkgId
      }
      .getOrElse(sys.error("Cannot find Daml.Script module"))

    var scriptExpr: SExpr = SEVal(
      LfDefRef(
        Identifier(homePackageId, QualifiedName(mod.name, DottedName.assertFromString("expr")))
      )
    )
    if (!results.isEmpty) {
      scriptExpr = SEApp(scriptExpr, results.toArray)
    }

    val signatures = this.signatures.updated(homePackageId, AstUtil.toSignature(pkg))
    val interface = new language.PackageInterface(signatures)
    val defs =
      new Compiler(interface, compilerConfig).unsafeCompilePackage(homePackageId, pkg)
    val compiledPackages =
      PureCompiledPackages(signatures, compiledDefinitions ++ defs, compilerConfig)
    val runner =
      new Runner(compiledPackages, Script.Action(scriptExpr, ScriptIds(scriptPackageId)), timeMode)
    runner
      .runWithClients(clients)
      ._1
      .map { v =>
        (
          v,
          req.getFormat match {
            case RunScriptRequest.Format.TEXT_ONLY =>
              v match {
                case SValue.SText(t) => t
                case _ => ""
              }
            case RunScriptRequest.Format.JSON =>
              try {
                LfValueCodec.apiValueToJsValue(v.toUnnormalizedValue).compactPrint
              } catch {
                case SError.SErrorCrash(_, _) => throw new ReplServiceMain.NonSerializableValue()

              }
            case RunScriptRequest.Format.UNRECOGNIZED =>
              throw new RuntimeException("Unrecognized response format")
          },
        )
      }
      .onComplete {
        case Failure(originalE) =>
          val e = originalE match {
            // For now, don’t show stack traces in Daml Repl. They look fairly confusing
            // since they refer to the internal names we use.
            case e: Script.FailedCmd => e.cause
            case _ => originalE
          }
          respObs.onNext(
            RunScriptResponse.newBuilder
              .setError(ScriptError.newBuilder.setError(e.toString).build)
              .build
          )
          respObs.onCompleted
        case Success((v, result)) =>
          results = results :+ v
          if (moduleRefs(v).contains(mod.name)) {
            // If the result is a closure and contains a reference to the
            // current module, we have to keep that module around.
            mainModules += mod.name -> mod
          }
          respObs.onNext(
            RunScriptResponse.newBuilder
              .setSuccess(ScriptSuccess.newBuilder.setResult(result).build)
              .build
          )
          respObs.onCompleted
      }
  }

  override def clearResults(
      req: ClearResultsRequest,
      respObs: StreamObserver[ClearResultsResponse],
  ): Unit = {
    results = Seq()
    mainModules = Map.empty
    respObs.onNext(ClearResultsResponse.newBuilder.build)
    respObs.onCompleted
  }
}
