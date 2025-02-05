// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package script

import java.util.concurrent.atomic.AtomicLong
import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.daml.lf.data.{ImmArray, assertRight}
import com.digitalasset.daml.lf.data.Ref.{
  Identifier,
  ModuleName,
  PackageId,
  PackageName,
  PackageVersion,
  QualifiedName,
}
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.engine.script.ledgerinteraction.IdeLedgerClient
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, Util => AstUtil}
import com.digitalasset.daml.lf.script.api.v1.{ScriptModule => ProtoScriptModule}
import com.digitalasset.daml.lf.speedy.{Compiler, SDefinition, Speedy}
import com.digitalasset.daml.lf.speedy.SExpr.{SDefinitionRef}
import com.digitalasset.daml.lf.validation.Validation
import com.daml.script.converter
import com.google.protobuf.ByteString
import com.digitalasset.daml.lf.engine.script.{Runner, Script}
import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.stablepackages.StablePackages
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.collection.immutable.HashMap
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/** Script interpretation context: maintains a set of modules and external packages, with which
  * scripts can be interpreted.
  */
object Context {
  type ContextId = Long
  case class ContextException(err: String) extends RuntimeException(err)

  private val contextCounter = new AtomicLong()

  def newContext(lfVerion: LanguageVersion, timeout: Duration)(implicit
      loggingContext: LoggingContext
  ): Context =
    new Context(contextCounter.incrementAndGet(), lfVerion, timeout)
}

class Context(
    val contextId: Context.ContextId,
    languageVersion: LanguageVersion,
    timeout: Duration,
)(implicit
    loggingContext: LoggingContext
) {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  def devMode: Boolean = languageVersion.isDevVersion

  private val compilerConfig =
    Compiler.Config(
      allowedLanguageVersions = LanguageVersion.AllVersions(languageVersion.major),
      packageValidation = Compiler.FullPackageValidation,
      profiling = Compiler.NoProfile,
      stacktracing = Compiler.FullStackTrace,
    )

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
    val newCtx = Context.newContext(languageVersion, timeout)
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
      loadModules: collection.Seq[ProtoScriptModule],
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

    val pkgInterface = new language.PackageInterface(this.allSignatures)
    val compiler = new Compiler(pkgInterface, compilerConfig)

    modulesToCompile.foreach { mod =>
      if (!omitValidation)
        assertRight(
          Validation
            .checkModule(StablePackages(languageVersion.major), pkgInterface, homePackageId, mod)
            .left
            .map(_.pretty)
        )
      modDefns +=
        mod.name -> compiler.unsafeCompileModule(homePackageId, mod).toMap
    }

    defns = extDefns
    modDefns.values.foreach(defns ++= _)
  }

  // TODO: https://github.com/digital-asset/daml/issues/17995
  //  Get the package name and package version from the daml.yaml
  private[this] val dummyMetadata =
    Ast.PackageMetadata(
      name = PackageName.assertFromString("-dummy-package-name-"),
      version = PackageVersion.assertFromString("0.0.0"),
      upgradedPackageId = None,
    )

  def allSignatures: Map[PackageId, Ast.PackageSignature] = {
    val extSignatures = this.extSignatures
    extSignatures.updated(
      homePackageId,
      AstUtil.toSignature(
        Ast.Package(modules, extSignatures.keySet, languageVersion, dummyMetadata)
      ),
    )
  }

  def interpretScript(
      pkgId: String,
      name: String,
      canceledByRequest: () => Boolean = () => false,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[IdeLedgerRunner.ScriptResult]] = {
    val defns = this.defns
    val compiledPackages = PureCompiledPackages(allSignatures, defns, compilerConfig)
    val scriptId =
      Identifier(PackageId.assertFromString(pkgId), QualifiedName.assertFromString(name))
    val traceLog = Speedy.Machine.newTraceLog
    val warningLog = Speedy.Machine.newWarningLog
    val profile = Speedy.Machine.newProfile
    val timeBomb = TimeBomb(timeout.toMillis)
    val isOverdue = timeBomb.hasExploded
    val ledgerClient = new IdeLedgerClient(compiledPackages, traceLog, warningLog, isOverdue)
    val timeBombCanceller = timeBomb.start()
    val (resultF, ideLedgerContext) = Runner.runIdeLedgerClient(
      compiledPackages = compiledPackages,
      scriptId = scriptId,
      convertInputValue = None,
      inputValue = None,
      initialClient = ledgerClient,
      timeMode = ScriptTimeMode.Static,
      traceLog = traceLog,
      warningLog = warningLog,
      profile = profile,
      canceled = () => {
        if (timeBombCanceller()) Some(Runner.TimedOut)
        else if (canceledByRequest()) Some(Runner.CanceledByRequest)
        else None
      },
    )

    def handleFailure(e: Error) =
      // SError are the errors that should be handled and displayed as
      // failed partial transactions.
      Success(
        Some(
          IdeLedgerRunner.ScriptError(
            ideLedgerContext.ledger,
            traceLog,
            warningLog,
            ideLedgerContext.currentSubmission,
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
            IdeLedgerRunner.ScriptSuccess(
              ideLedgerContext.ledger,
              traceLog,
              warningLog,
              profile,
              dummyDuration,
              dummySteps,
              v,
            )
          )
        )
      case Failure(e: Error) => handleFailure(e)
      case Failure(e: Runner.InterpretationError) => handleFailure(Error.RunnerException(e.error))
      case Failure(e: engine.free.InterpretationError) =>
        handleFailure(Error.RunnerException(e.error))
      case Failure(Runner.CanceledByRequest) =>
        handleFailure(Error.CanceledByRequest())
      case Failure(Runner.TimedOut) =>
        handleFailure(Error.Timeout(timeout))
      case Failure(e: Script.FailedCmd) =>
        e.cause match {
          case e: Error => handleFailure(e)
          case e: speedy.SError.SError => handleFailure(Error.RunnerException(e))
          case _ =>
            // We can't send _everything_ over without changing internal, we log and wrap the error in t.
            logger.warn("Script.FailedCmd unexpected cause: " + e.getMessage)
            logger.debug(e.getStackTrace.mkString("\n"))
            handleFailure(Error.Internal("Script.FailedCmd unexpected cause: " + e.getMessage))
        }
      case Failure(e: converter.ConverterException) =>
        handleFailure(Error.Internal("Unexpected conversion exception: " + e.getMessage))
      case Failure(e: com.digitalasset.daml.lf.engine.free.ConversionError) =>
        handleFailure(Error.Internal("Unexpected conversion exception: " + e.getMessage))
      case Failure(e) =>
        // something bad happened, we log and fail
        logger.error("Unexpected error type from script runner: " + e.getMessage)
        logger.debug(e.getStackTrace.mkString("\n"))
        Failure(e)
    }
  }
}
