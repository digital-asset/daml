// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package scenario

import java.util.concurrent.atomic.AtomicLong

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.archive.Decode
import com.daml.lf.archive.Decode.ParseError
import com.daml.lf.data.assertRight
import com.daml.lf.data.Ref.{DottedName, Identifier, ModuleName, PackageId, QualifiedName}
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.scenario.api.v1.{ScenarioModule => ProtoScenarioModule}
import com.daml.lf.speedy.Compiler
import com.daml.lf.speedy.ScenarioRunner
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.Speedy
import com.daml.lf.speedy.SExpr
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SExpr.{LfDefRef, SDefinitionRef}
import com.daml.lf.validation.Validation
import com.google.protobuf.ByteString
import com.daml.lf.engine.script.{
  Runner,
  Script,
  ScriptIds,
  ScriptTimeMode,
  IdeClient,
  Participants
}
import com.daml.lf.transaction.VersionTimeline

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.collection.immutable.HashMap
import scala.util.{Failure, Success}

/**
  * Scenario interpretation context: maintains a set of modules and external packages, with which
  * scenarios can be interpreted.
  */
object Context {
  type ContextId = Long
  case class ContextException(err: String) extends RuntimeException(err)

  private val contextCounter = new AtomicLong()

  def newContext(lfVerion: LanguageVersion): Context =
    new Context(contextCounter.incrementAndGet(), lfVerion)

  private val compilerConfig =
    Compiler.Config(
      allowedLanguageVersions = VersionTimeline.devLanguageVersions,
      packageValidation = Compiler.FullPackageValidation,
      profiling = Compiler.NoProfile,
      stacktracing = Compiler.FullStackTrace,
    )
}

class Context(val contextId: Context.ContextId, languageVersion: LanguageVersion) {

  import Context._

  /**
    * The package identifier to use for modules added to the context.
    * When decoding LF modules this package identifier should be used to rewrite
    * self-references. We only care that the identifier is disjunct from the package ids
    * in extPackages.
    */
  val homePackageId: PackageId = PackageId.assertFromString("-homePackageId-")

  private var extPackages: Map[PackageId, Ast.Package] = HashMap.empty
  private var extDefns: Map[SDefinitionRef, SExpr] = HashMap.empty
  private var modules: Map[ModuleName, Ast.Module] = HashMap.empty
  private var modDefns: Map[ModuleName, Map[SDefinitionRef, SExpr]] = HashMap.empty
  private var defns: Map[SDefinitionRef, SExpr] = HashMap.empty

  def loadedModules(): Iterable[ModuleName] = modules.keys
  def loadedPackages(): Iterable[PackageId] = extPackages.keys

  def cloneContext(): Context = synchronized {
    val newCtx = Context.newContext(languageVersion)
    newCtx.extPackages = extPackages
    newCtx.extDefns = extDefns
    newCtx.modules = modules
    newCtx.modDefns = modDefns
    newCtx.defns = defns
    newCtx
  }

  private[this] val dop: Decode.OfPackage[_] = Decode.decoders
    .lift(languageVersion)
    .getOrElse(
      throw Context.ContextException(s"No decode support for LF ${languageVersion.pretty}"))
    .decoder

  private def decodeModule(bytes: ByteString): Ast.Module = {
    val lfScenarioModule = dop.protoScenarioModule(Decode.damlLfCodedInputStream(bytes.newInput))
    dop.decodeScenarioModule(homePackageId, lfScenarioModule)
  }

  @throws[ParseError]
  def update(
      unloadModules: Set[ModuleName],
      loadModules: Seq[ProtoScenarioModule],
      unloadPackages: Set[PackageId],
      loadPackages: Seq[ByteString],
      omitValidation: Boolean,
  ): Unit = synchronized {

    val newModules = loadModules.map(module => decodeModule(module.getDamlLf1))
    modules --= unloadModules
    newModules.foreach(mod => modules += mod.name -> mod)

    val newPackages =
      loadPackages.map { archive =>
        Decode.decodeArchiveFromInputStream(archive.newInput)
      }.toMap

    val modulesToCompile =
      if (unloadPackages.nonEmpty || newPackages.nonEmpty) {
        // if any change we recompile everything
        extPackages --= unloadPackages
        extPackages ++= newPackages
        extDefns = assertRight(Compiler.compilePackages(extPackages, compilerConfig))
        modDefns = HashMap.empty
        modules.values
      } else {
        modDefns --= unloadModules
        newModules
      }

    val pkgs = allPackages
    val compiler = new Compiler(pkgs, compilerConfig)

    modulesToCompile.foreach { mod =>
      if (!omitValidation)
        assertRight(Validation.checkModule(pkgs, homePackageId, mod.name).left.map(_.pretty))
      modDefns += mod.name -> mod.definitions.flatMap {
        case (defName, defn) =>
          compiler
            .unsafeCompileDefn(Identifier(homePackageId, QualifiedName(mod.name, defName)), defn)
      }
    }

    defns = extDefns
    modDefns.values.foreach(defns ++= _)
  }

  def allPackages: Map[PackageId, Ast.Package] = synchronized {
    extPackages + (homePackageId -> Ast.Package(modules, extPackages.keySet, languageVersion, None))
  }

  // We use a fix Hash and fix time to seed the contract id, so we get reproducible run.
  private val txSeeding =
    crypto.Hash.hashPrivateKey(s"scenario-service")

  private def buildMachine(identifier: Identifier): Option[Speedy.Machine] = {
    val defns = this.defns
    val compiledPackages = PureCompiledPackages(allPackages, defns, compilerConfig)
    for {
      defn <- defns.get(LfDefRef(identifier))
    } yield
      Speedy.Machine.fromScenarioSExpr(
        compiledPackages,
        txSeeding,
        defn,
        value.ValueVersions.DevOutputVersions,
        transaction.TransactionVersions.DevOutputVersions,
      )
  }

  def interpretScenario(
      pkgId: String,
      name: String,
  ): Option[(ScenarioLedger, Speedy.Machine, Either[SError, SValue])] = {
    buildMachine(
      Identifier(PackageId.assertFromString(pkgId), QualifiedName.assertFromString(name)),
    ).map { machine =>
      ScenarioRunner(machine).run() match {
        case Right((diff @ _, steps @ _, ledger, value)) =>
          (ledger, machine, Right(value))
        case Left((err, ledger)) =>
          (ledger, machine, Left(err))
      }
    }
  }

  def interpretScript(
      pkgId: String,
      name: String,
  )(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory, mat: Materializer)
    : Future[Option[(ScenarioLedger, (Speedy.Machine, Speedy.Machine), Either[SError, SValue])]] = {
    val defns = this.defns
    val compiledPackages = PureCompiledPackages(allPackages, defns, compilerConfig)
    val expectedScriptId = DottedName.assertFromString("Daml.Script")
    val Some(scriptPackageId) = allPackages.collectFirst {
      case (pkgId, pkg) if pkg.modules contains expectedScriptId => pkgId
    }
    val scriptExpr = SExpr.SEVal(
      LfDefRef(Identifier(PackageId.assertFromString(pkgId), QualifiedName.assertFromString(name))))
    val runner = new Runner(
      compiledPackages,
      Script.Action(scriptExpr, ScriptIds(scriptPackageId)),
      ScriptTimeMode.Static
    )
    val ledgerClient = new IdeClient(compiledPackages)
    val participants = Participants(Some(ledgerClient), Map.empty, Map.empty)
    val (clientMachine, resultF) = runner.runWithClients(participants)
    resultF.transform {
      case Success(v) =>
        Success(
          Some(
            (ledgerClient.scenarioRunner.ledger, (clientMachine, ledgerClient.machine), Right(v))))
      case Failure(e: SError) =>
        // SError are the errors that should be handled and displayed as
        // failed partial transactions.
        Success(
          Some(
            (ledgerClient.scenarioRunner.ledger, (clientMachine, ledgerClient.machine), Left(e))))
      case Failure(e) => Failure(e)
    }
  }
}
