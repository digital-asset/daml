// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package script

import java.util.concurrent.atomic.AtomicLong
import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.daml.lf.data.{ImmArray, assertRight}
import com.digitalasset.daml.lf.data.Ref.{Identifier, ModuleName, PackageId, QualifiedName}
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.engine.script.ledgerinteraction.IdeLedgerClient
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, Util => AstUtil}
import com.digitalasset.daml.lf.script.api.v1.{ScriptModule => ProtoScriptModule}
import com.digitalasset.daml.lf.speedy.{Compiler, SDefinition, Speedy}
import com.digitalasset.daml.lf.speedy.SExpr.SDefinitionRef
import com.digitalasset.daml.lf.validation.Validation
import com.daml.script.converter
import com.google.protobuf.ByteString
import com.digitalasset.daml.lf.engine.script.{Runner, Script}
import com.daml.logging.LoggingContext
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
      allowedLanguageVersions = LanguageVersion.allLfVersionsRange,
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

  /* To run a script on a daml project, we load the project as modules (daml files), and we load
   * its dependencies as packages (DALF files). The loaded packages are also called the external
   * packages. The home package is built dynamically from the loaded modules.
   * In the case of `test --all`, to run the scripts from a dependency, there should not be any
   * module to load, and packageId should be the id of the package in which we run the scripts, not
   * the `homePackageId`.
   */
  private var extSignatures: Map[PackageId, Ast.PackageSignature] = HashMap.empty
  private var extDefns: Map[SDefinitionRef, SDefinition] = HashMap.empty
  private var modules: Map[ModuleName, Ast.Module] = HashMap.empty
  private var modDefns: Map[ModuleName, Map[SDefinitionRef, SDefinition]] = HashMap.empty
  // the id of the package that contains the scripts to run
  // can be different from the home package id if we run scripts from an external package
  private var packageId: PackageId = homePackageId
  // Contains all the signatures, of the external packages and the home package.
  private var allSignatures: Map[PackageId, Ast.PackageSignature] = HashMap.empty

  def loadedModules(): Iterable[ModuleName] = modules.keys
  def loadedPackages(): Iterable[PackageId] = extSignatures.keys

  def cloneContext(): Context = synchronized {
    val newCtx = Context.newContext(languageVersion, timeout)
    newCtx.extSignatures = extSignatures
    newCtx.extDefns = extDefns
    newCtx.modules = modules
    newCtx.modDefns = modDefns
    newCtx.packageId = packageId
    newCtx.allSignatures = allSignatures
    newCtx
  }

  @throws[archive.Error]
  def update(
      unloadModules: Set[ModuleName],
      loadModules: collection.Seq[ProtoScriptModule],
      unloadPackages: Set[PackageId],
      loadPackages: collection.Seq[ByteString],
      omitValidation: Boolean,
      packageMetadata: Ast.PackageMetadata,
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

    if (modules.nonEmpty) {
      // we don't have a package id for these modules, so we use the default homePackageId
      packageId = homePackageId
      allSignatures = extSignatures.updated(
        packageId,
        AstUtil.toSignature(
          Ast.Package(
            modules,
            extSignatures.keySet,
            languageVersion,
            packageMetadata,
            Ast.GeneratedImports(
              reason = "package made in com.digitalasset.daml.lf.script.Context",
              pkgIds = Set.empty,
            ),
          )
        ),
      )
      val pkgInterface = new language.PackageInterface(allSignatures)
      val compiler = new Compiler(pkgInterface, compilerConfig)
      modulesToCompile.foreach { mod =>
        if (!omitValidation)
          assertRight(
            Validation
              .checkModule(pkgInterface, packageId, mod)
              .left
              .map(_.pretty)
          )
        modDefns +=
          mod.name -> compiler.unsafeCompileModule(packageId, mod).toMap
      }
    } else {
      // the context's package is either an already loaded package or an empty package that has no module
      val packageSig = extSignatures.find { case (_, sig) =>
        sig.metadata.name == packageMetadata.name && sig.metadata.version == packageMetadata.version
      }
      packageSig match {
        case Some((pkgId, _)) =>
          this.packageId = pkgId
          this.allSignatures = extSignatures
        case None =>
          this.packageId = homePackageId
          this.allSignatures = extSignatures.updated(
            packageId,
            Ast.PackageSignature(
              Map.empty,
              extSignatures.keySet,
              languageVersion,
              packageMetadata,
              Ast.GeneratedImports(
                reason = "package made in com.digitalasset.daml.lf.script.Context",
                pkgIds = Set.empty,
              ),
            ),
          )
      }
    }
  }

  def interpretScript(
      name: String,
      canceledByRequest: () => Boolean,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[IdeLedgerRunner.ScriptResult] = {
    val defns = extDefns ++ modDefns.values.flatten
    val compiledPackages = PureCompiledPackages(allSignatures, defns, compilerConfig)
    val scriptId = Identifier(packageId, QualifiedName.assertFromString(name))
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

    val dummyDuration: Double = 0
    val dummySteps: Int = 0

    resultF.transform {
      case Success(v) =>
        Success(
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
