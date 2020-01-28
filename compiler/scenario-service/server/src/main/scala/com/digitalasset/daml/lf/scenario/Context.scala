// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.scenario

import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.archive.Decode.ParseError
import com.digitalasset.daml.lf.data.Ref.{Identifier, ModuleName, PackageId, QualifiedName}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.scenario.api.v1.{ScenarioModule => ProtoScenarioModule}
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.speedy.ScenarioRunner
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.Speedy
import com.digitalasset.daml.lf.speedy.SExpr
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.types.Ledger.Ledger
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.speedy.SExpr.{LfDefRef, SDefinitionRef}
import com.digitalasset.daml.lf.validation.Validation
import com.google.protobuf.ByteString
import com.digitalasset.daml.lf.transaction.VersionTimeline

/**
  * Scenario interpretation context: maintains a set of modules and external packages, with which
  * scenarios can be interpreted.
  */
object Context {
  type ContextId = Long
  case class ContextException(err: String) extends RuntimeException(err, null, true, false)

  var nextContextId: ContextId = 0

  def newContext(): Context = {
    this.synchronized {
      nextContextId += 1
      new Context(nextContextId)
    }
  }

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
  val homePackageId: PackageId =
    PackageId.assertFromString("-homePackageId-")

  private var modules: Map[ModuleName, Ast.Module] = Map.empty
  private var extPackages: Map[PackageId, Ast.Package] = Map.empty
  private var defns: Map[SDefinitionRef, (LanguageVersion, SExpr)] = Map.empty

  def loadedModules(): Iterable[ModuleName] = modules.keys
  def loadedPackages(): Iterable[PackageId] = extPackages.keys

  def cloneContext(): Context = this.synchronized {
    val newCtx = Context.newContext
    newCtx.modules = modules
    newCtx.extPackages = extPackages
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

  private def validate(pkgIds: Traversable[PackageId]): Unit =
    pkgIds.foreach(
      Validation.checkPackage(allPackages, _).left.foreach(e => throw ParseError(e.pretty)),
    )

  @throws[ParseError]
  def update(
      unloadModules: Seq[String],
      loadModules: Seq[ProtoScenarioModule],
      unloadPackages: Seq[String],
      loadPackages: Seq[ByteString],
      forScenarioService: Boolean,
  ): Unit = this.synchronized {

    // First we unload modules and packages
    unloadModules.foreach { moduleId =>
      val lfModuleId = assert(ModuleName.fromString(moduleId))
      modules -= lfModuleId
      defns = defns.filterKeys(ref => ref.packageId != homePackageId || ref.modName != lfModuleId)
    }
    unloadPackages.foreach { pkgId =>
      val lfPkgId = assert(PackageId.fromString(pkgId))
      extPackages -= lfPkgId
      defns = defns.filterKeys(ref => ref.packageId != lfPkgId)
    }
    // Now we can load the new packages.
    val newPackages =
      loadPackages.map { archive =>
        Decode.decodeArchiveFromInputStream(archive.newInput)
      }.toMap
    extPackages ++= newPackages
    defns ++= Compiler(extPackages).compilePackages(extPackages.keys).map {
      case (defRef, defn) =>
        val module = extPackages(defRef.packageId).modules(defRef.modName)
        (defRef, (module.languageVersion, defn))
    }

    // And now the new modules can be loaded.
    val lfModules = loadModules.map(module =>
      decodeModule(LanguageVersion.Major.V1, module.getMinor, module.getDamlLf1),
    )

    modules ++= lfModules.map(m => m.name -> m)
    if (!forScenarioService)
      validate(newPackages.keys ++ Iterable(homePackageId))

    // At this point 'allPackages' is consistent and we can
    // compile the new modules.
    val compiler = Compiler(allPackages)
    defns = lfModules.foldLeft(defns)((newDefns, m) =>
      newDefns.filterKeys(ref => ref.packageId != homePackageId || ref.modName != m.name)
        ++ m.definitions.flatMap {
          case (defName, defn) =>
            compiler
              .compileDefn(Identifier(homePackageId, QualifiedName(m.name, defName)), defn)
              .map {
                case (defRef, compiledDefn) => (defRef, (m.languageVersion, compiledDefn))
              }

        },
    )
  }

  def allPackages: Map[PackageId, Ast.Package] =
    extPackages + (homePackageId -> Ast.Package(modules, extPackages.keySet))

  private def buildMachine(identifier: Identifier): Option[Speedy.Machine] = {
    for {
      res <- defns.get(LfDefRef(identifier))
      (lfVer, defn) = res
    } yield
    // note that the use of `Map#mapValues` here is intentional: we lazily project the
    // definition out rather than rebuilding the map.
    Speedy.Machine
      .build(
        checkSubmitterInMaintainers = VersionTimeline.checkSubmitterInMaintainers(lfVer),
        sexpr = defn,
        compiledPackages = PureCompiledPackages(allPackages, defns.mapValues(_._2)).right.get,
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
        case Right((diff @ _, steps @ _, ledger)) =>
          (ledger, machine, Right(machine.toSValue))
        case Left((err, ledger)) =>
          (ledger, machine, Left(err))
      }
    }

}
