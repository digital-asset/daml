// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package scenario

import java.util.concurrent.atomic.AtomicLong

import com.daml.lf.archive.Decode
import com.daml.lf.archive.Decode.ParseError
import com.daml.lf.data.Ref.{Identifier, ModuleName, PackageId, QualifiedName}
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.scenario.api.v1.{ScenarioModule => ProtoScenarioModule}
import com.daml.lf.speedy.Compiler
import com.daml.lf.speedy.ScenarioRunner
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.Speedy
import com.daml.lf.speedy.SExpr
import com.daml.lf.speedy.SValue
import com.daml.lf.types.Ledger.Ledger
import com.daml.lf.speedy.SExpr.{LfDefRef, SDefinitionRef}
import com.daml.lf.validation.Validation
import com.google.protobuf.ByteString

import scala.collection.immutable.HashMap

/**
  * Scenario interpretation context: maintains a set of modules and external packages, with which
  * scenarios can be interpreted.
  */
object Context {
  type ContextId = Long
  case class ContextException(err: String) extends RuntimeException(err)

  private val contextCounter = new AtomicLong()

  def newContext: Context = new Context(contextCounter.incrementAndGet())

  private def assert[X](either: Either[String, X]): X =
    either.fold(e => throw new ParseError(e), identity)
}

class Context(val contextId: Context.ContextId) {

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
    val newCtx = Context.newContext
    newCtx.extPackages = extPackages
    newCtx.extDefns = extDefns
    newCtx.modules = modules
    newCtx.modDefns = modDefns
    newCtx.defns = defns
    newCtx
  }

  private def decodeModule(
      major: LanguageVersion.Major,
      minor: String,
      bytes: ByteString,
  ): Ast.Module = {
    val lfVer = LanguageVersion(major, LanguageVersion.Minor fromProtoIdentifier minor)
    val dop: Decode.OfPackage[_] = Decode.decoders
      .lift(lfVer)
      .getOrElse(throw Context.ContextException(s"No decode support for LF ${lfVer.pretty}"))
      .decoder
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

    val newModules = loadModules.map(module =>
      decodeModule(LanguageVersion.Major.V1, module.getMinor, module.getDamlLf1))
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
        extDefns = assert(Compiler.compilePackages(extPackages, Compiler.NoProfile))
        modDefns = HashMap.empty
        modules.values
      } else {
        modDefns --= unloadModules
        newModules
      }

    val pkgs = allPackages
    val compiler = Compiler(pkgs, Compiler.NoProfile)

    modulesToCompile.foreach { mod =>
      if (!omitValidation)
        assert(Validation.checkModule(pkgs, homePackageId, mod.name).left.map(_.pretty))
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
    extPackages + (homePackageId -> Ast.Package(modules, extPackages.keySet, None))
  }

  // We use a fix Hash and fix time to seed the contract id, so we get reproducible run.
  private val submissionTime =
    data.Time.Timestamp.MinValue
  private val initialSeeding =
    speedy.InitialSeeding.TransactionSeed(crypto.Hash.hashPrivateKey(s"scenario-service"))

  private def buildMachine(identifier: Identifier): Option[Speedy.Machine] = {
    val defns = this.defns
    for {
      defn <- defns.get(LfDefRef(identifier))
    } yield
      Speedy.Machine
        .build(
          sexpr = defn,
          compiledPackages = PureCompiledPackages(allPackages, defns, Compiler.NoProfile),
          submissionTime,
          initialSeeding,
          Set.empty,
        )
  }

  def interpretScenario(
      pkgId: String,
      name: String,
  ): Option[(Ledger, Speedy.Machine, Either[SError, SValue])] =
    buildMachine(
      Identifier(assert(PackageId.fromString(pkgId)), assert(QualifiedName.fromString(name))),
    ).map { machine =>
      ScenarioRunner(machine).run() match {
        case Right((diff @ _, steps @ _, ledger, value)) =>
          (ledger, machine, Right(value))
        case Left((err, ledger)) =>
          (ledger, machine, Left(err))
      }
    }

}
