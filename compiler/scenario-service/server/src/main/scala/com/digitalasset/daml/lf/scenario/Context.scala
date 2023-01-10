// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package scenario

import java.util.concurrent.atomic.AtomicLong

import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.archive
import com.daml.lf.data.{assertRight, ImmArray}
import com.daml.lf.data.Ref.{Identifier, ModuleName, PackageId, QualifiedName}
import com.daml.lf.engine.script.ledgerinteraction.{IdeLedgerClient, ScriptTimeMode}
import com.daml.lf.language.{Ast, LanguageVersion, Util => AstUtil}
import com.daml.lf.scenario.api.v1.{ScenarioModule => ProtoScenarioModule}
import com.daml.lf.speedy.{Compiler, SDefinition, Speedy}
import com.daml.lf.speedy.SExpr.{LfDefRef, SDefinitionRef}
import com.daml.lf.validation.Validation
import com.google.protobuf.ByteString
import com.daml.lf.engine.script.{Participants, Runner, ScriptF}
import com.daml.logging.LoggingContext

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.collection.immutable.HashMap
import scala.util.{Failure, Success}

/** Scenario interpretation context: maintains a set of modules and external packages, with which
  * scenarios can be interpreted.
  */
object Context {
  type ContextId = Long
  case class ContextException(err: String) extends RuntimeException(err)

  private val contextCounter = new AtomicLong()

  def newContext(lfVerion: LanguageVersion)(implicit loggingContext: LoggingContext): Context =
    new Context(contextCounter.incrementAndGet(), lfVerion)

  private val compilerConfig =
    Compiler.Config(
      allowedLanguageVersions = LanguageVersion.DevVersions,
      packageValidation = Compiler.FullPackageValidation,
      profiling = Compiler.NoProfile,
      stacktracing = Compiler.FullStackTrace,
    )
}

class Context(val contextId: Context.ContextId, languageVersion: LanguageVersion)(implicit
    loggingContext: LoggingContext
) {

  import Context._

  /** The package identifier to use for modules added to the context.
    * When decoding LF modules this package identifier should be used to rewrite
    * self-references. We only care that the identifier is disjunct from the package ids
    * in extSignature.
    */
  val homePackageId: PackageId = PackageId.assertFromString("-homePackageId-")

  private var extSignatures: Map[PackageId, Ast.PackageSignature] = HashMap.empty
  private var extDefns: Map[SDefinitionRef, SDefinition] = HashMap.empty
  private var modules: Map[ModuleName, Ast.Module] = HashMap.empty
  private var modDefns: Map[ModuleName, Map[SDefinitionRef, SDefinition]] = HashMap.empty
  private var defns: Map[SDefinitionRef, SDefinition] = HashMap.empty

  def loadedModules(): Iterable[ModuleName] = modules.keys
  def loadedPackages(): Iterable[PackageId] = extSignatures.keys

  def cloneContext(): Context = synchronized {
    val newCtx = Context.newContext(languageVersion)
    newCtx.extSignatures = extSignatures
    newCtx.extDefns = extDefns
    newCtx.modules = modules
    newCtx.modDefns = modDefns
    newCtx.defns = defns
    newCtx
  }

  @throws[archive.Error]
  def update(
      unloadModules: Set[ModuleName],
      loadModules: collection.Seq[ProtoScenarioModule],
      unloadPackages: Set[PackageId],
      loadPackages: collection.Seq[ByteString],
      omitValidation: Boolean,
  ): Unit = synchronized {

    val newModules = loadModules.map(module =>
      archive.moduleDecoder(languageVersion, homePackageId).assertFromByteString(module.getDamlLf1)
    )
    modules --= unloadModules
    newModules.foreach(mod => modules += mod.name -> mod)

    val newPackages =
      loadPackages.map(archive.ArchiveDecoder.assertFromByteString).toMap

    val modulesToCompile =
      if (unloadPackages.nonEmpty || newPackages.nonEmpty) {
        val invalidPackages = unloadModules ++ newPackages.keys
        val newExtSignature = extSignatures -- unloadPackages ++ AstUtil.toSignatures(newPackages)
        val interface = new language.PackageInterface(newExtSignature)
        val newExtDefns = extDefns.view.filterKeys(sdef => !invalidPackages(sdef.packageId)) ++
          assertRight(Compiler.compilePackages(interface, newPackages, compilerConfig))
        // we update only if we manage to compile the new packages
        extSignatures = newExtSignature
        extDefns = newExtDefns.toMap
        modDefns = HashMap.empty
        modules.values
      } else {
        modDefns --= unloadModules
        newModules
      }

    val interface = new language.PackageInterface(this.allSignatures)
    val compiler = new Compiler(interface, compilerConfig)

    modulesToCompile.foreach { mod =>
      if (!omitValidation)
        assertRight(Validation.checkModule(interface, homePackageId, mod).left.map(_.pretty))
      modDefns +=
        mod.name -> compiler.unsafeCompileModule(homePackageId, mod).toMap
    }

    defns = extDefns
    modDefns.values.foreach(defns ++= _)
  }

  def allSignatures: Map[PackageId, Ast.PackageSignature] = {
    val extSignatures = this.extSignatures
    extSignatures.updated(
      homePackageId,
      AstUtil.toSignature(Ast.Package(modules, extSignatures.keySet, languageVersion, None)),
    )
  }

  // We use a fix Hash and fix time to seed the contract id, so we get reproducible run.
  private val txSeeding =
    crypto.Hash.hashPrivateKey(s"scenario-service")

  private[this] def buildMachine(defn: SDefinition): Speedy.ScenarioMachine =
    Speedy.Machine.fromScenarioSExpr(
      PureCompiledPackages(allSignatures, defns, compilerConfig),
      defn.body,
    )

  def interpretScenario(
      pkgId: String,
      name: String,
  ): Option[ScenarioRunner.ScenarioResult] = {
    val id = Identifier(PackageId.assertFromString(pkgId), QualifiedName.assertFromString(name))
    defns.get(LfDefRef(id)).map(defn => ScenarioRunner.run(() => buildMachine(defn), txSeeding))
  }

  def interpretScript(
      pkgId: String,
      name: String,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[ScenarioRunner.ScenarioResult]] = {
    val defns = this.defns
    val compiledPackages = PureCompiledPackages(allSignatures, defns, compilerConfig)
    val scriptId =
      Identifier(PackageId.assertFromString(pkgId), QualifiedName.assertFromString(name))
    val traceLog = Speedy.Machine.newTraceLog
    val warningLog = Speedy.Machine.newWarningLog
    val ledgerClient: IdeLedgerClient = new IdeLedgerClient(compiledPackages, traceLog, warningLog)
    val participants = Participants(Some(ledgerClient), Map.empty, Map.empty)
    val (clientMachine, resultF) = Runner.run(
      compiledPackages = compiledPackages,
      scriptId = scriptId,
      convertInputValue = None,
      inputValue = None,
      initialClients = participants,
      timeMode = ScriptTimeMode.Static,
      traceLog = traceLog,
      warningLog = warningLog,
    )

    def handleFailure(e: Error) =
      // SError are the errors that should be handled and displayed as
      // failed partial transactions.
      Success(
        Some(
          ScenarioRunner.ScenarioError(
            ledgerClient.ledger,
            clientMachine.traceLog,
            clientMachine.warningLog,
            ledgerClient.currentSubmission,
            // TODO (MK) https://github.com/digital-asset/daml/issues/7276
            ImmArray.Empty,
            e,
          )
        )
      )

    val dummyDuration: Double = 0
    val dummySteps: Int = 0

    resultF.transform {
      case Success(v) =>
        Success(
          Some(
            ScenarioRunner.ScenarioSuccess(
              ledgerClient.ledger,
              clientMachine.traceLog,
              clientMachine.warningLog,
              dummyDuration,
              dummySteps,
              v,
            )
          )
        )
      case Failure(e: Error) => handleFailure(e)
      case Failure(e: Runner.InterpretationError) => {
        handleFailure(Error.RunnerException(e.error))
      }
      case Failure(e: ScriptF.FailedCmd) =>
        e.cause match {
          case e: Error => handleFailure(e)
          case e: speedy.SError.SError => handleFailure(Error.RunnerException(e))
          case _ => Failure(e)
        }
      case Failure(e) => Failure(e)
    }
  }
}
