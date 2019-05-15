// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.scenario

import com.digitalasset.daml_lf.DamlLf1
import com.digitalasset.daml.lf.data.Ref.{Identifier, ModuleName, PackageId, QualifiedName}
import com.digitalasset.daml.lf.archive.LanguageVersion
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.daml.lf.lfpackage.{Decode, DecodeV1}
import com.digitalasset.daml.lf.scenario.api.v1.{Module => ProtoModule}
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.speedy.ScenarioRunner
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.Speedy
import com.digitalasset.daml.lf.speedy.SExpr
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.types.LedgerForScenarios.Ledger
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.lfpackage.Decode.ParseError
import com.digitalasset.daml.lf.speedy.SExpr.{LfDefRef, SDefinitionRef}
import com.digitalasset.daml.lf.validation.{Validation, ValidationError}
import com.google.protobuf.ByteString

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
  private var defns: Map[SDefinitionRef, SExpr] = Map.empty

  def loadedModules(): Iterable[ModuleName] = modules.keys
  def loadedPackages(): Iterable[PackageId] = extPackages.keys

  def cloneContext(): Context = this.synchronized {
    val newCtx = Context.newContext
    newCtx.modules = modules
    newCtx.extPackages = extPackages
    newCtx.defns = defns
    newCtx
  }

  private def validate(pkgIds: Traversable[PackageId], forScenarioService: Boolean): Unit = {
    val validator: PackageId => Either[ValidationError, Unit] =
      if (forScenarioService)
        Validation.checkPackageForScenarioService(allPackages, _)
      else
        Validation.checkPackage(allPackages, _)

    pkgIds.foreach(validator(_).left.foreach(e => throw ParseError(e.pretty)))
  }

  @throws[ParseError]
  def update(
      unloadModules: Seq[String],
      loadModules: Seq[ProtoModule],
      unloadPackages: Seq[String],
      loadPackages: Seq[ByteString],
      forScenarioService: Boolean
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
    defns ++= Compiler(extPackages).compilePackages(extPackages.keys)

    // And now the new modules can be loaded.
    val lfModules = loadModules.map(module =>
      module.getModuleCase match {
        case ProtoModule.ModuleCase.DAML_LF_1 =>
          // TODO this duplicates/skips the similar logic and extra version
          // support check from `Decode`'s functions; will improperly accept
          // too-new versions as a result
          val lfMod = DamlLf1.Module
            .parser()
            .parseFrom(
              Decode.damlLfCodedInputStream(module.getDamlLf1.newInput)
            )
          new DecodeV1(LanguageVersion.Minor fromProtoIdentifier module.getMinor)
            .ModuleDecoder(homePackageId, lfMod)
            .decode()
        case ProtoModule.ModuleCase.DAML_LF_DEV | ProtoModule.ModuleCase.MODULE_NOT_SET =>
          throw Context.ContextException("Module.MODULE_NOT_SET")
    })
    modules ++= lfModules.map(m => m.name -> m)

    validate(newPackages.keys ++ Iterable(homePackageId), forScenarioService)

    // At this point 'allPackages' is consistent and we can
    // compile the new modules.
    val compiler = Compiler(allPackages)
    defns = lfModules.foldLeft(defns)(
      (newDefns, m) =>
        newDefns.filterKeys(ref => ref.packageId != homePackageId || ref.modName != m.name)
          ++ m.definitions.flatMap {
            case (defName, defn) =>
              compiler.compileDefn(Identifier(homePackageId, QualifiedName(m.name, defName)), defn)

        }
    )
  }

  def allPackages: Map[PackageId, Ast.Package] =
    extPackages + (homePackageId -> Ast.Package(modules))

  private def buildMachine(identifier: Identifier): Option[Speedy.Machine] = {
    for {
      defn <- defns.get(LfDefRef(identifier))
    } yield Speedy.Machine.build(defn, PureCompiledPackages(allPackages, defns).right.get)
  }

  def interpretScenario(
      pkgId: String,
      name: String
  ): Option[(Ledger, Speedy.Machine, Either[SError, SValue])] =
    buildMachine(
      Identifier(assert(PackageId.fromString(pkgId)), assert(QualifiedName.fromString(name))))
      .map { machine =>
        ScenarioRunner(machine).run() match {
          case Right((diff @ _, steps @ _, ledger)) =>
            (ledger, machine, Right(machine.toSValue))
          case Left((err, ledger)) =>
            (ledger, machine, Left(err))
        }
      }

}
