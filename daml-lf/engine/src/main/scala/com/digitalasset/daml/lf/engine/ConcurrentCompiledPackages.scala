// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import java.util.concurrent.ConcurrentHashMap

import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.ConcurrentCompiledPackages.AddPackageState
import com.digitalasset.daml.lf.lfpackage.Ast.Package
import com.digitalasset.daml.lf.speedy.Compiler.PackageNotFound
import com.digitalasset.daml.lf.speedy.SExpr.SDefinitionRef
import com.digitalasset.daml.lf.speedy.{Compiler, SExpr}

/** Thread-safe class that can be used when you need to maintain a shared, mutable collection of
  * packages.
  */
final class ConcurrentCompiledPackages extends CompiledPackages {
  private[this] val _packages: ConcurrentHashMap[PackageId, Package] =
    new ConcurrentHashMap()
  private[this] val _defns: ConcurrentHashMap[SDefinitionRef, SExpr] =
    new ConcurrentHashMap()

  def getPackage(pId: PackageId): Option[Package] = Option(_packages.get(pId))
  def getDefinition(dref: SDefinitionRef): Option[SExpr] = Option(_defns.get(dref))

  /** Might ask for a package if the package you're trying to add references it.
    *
    * Note that when resuming from a [[Result]] the continuation will modify the
    * [[ConcurrentCompiledPackages]] that originated it.
    */
  def addPackage(pkgId: PackageId, pkg: Package): Result[Unit] =
    addPackageInternal(
      AddPackageState(
        packages = Map(pkgId -> pkg),
        seenDependencies = Set.empty,
        toCompile = List(pkgId)))

  private def addPackageInternal(state: AddPackageState): Result[Unit] =
    this.synchronized {
      var toCompile = state.toCompile

      while (toCompile.nonEmpty) {
        val pkgId: PackageId = toCompile.head
        toCompile = toCompile.tail

        if (!_packages.contains(pkgId)) {

          val pkg = state.packages.get(pkgId) match {
            case None => return ResultError(Error(s"Could not find package $pkgId"))
            case Some(pkg_) => pkg_
          }

          val defns =
            try {
              Compiler(packages orElse state.packages).compilePackage(pkgId)
            } catch {
              // if we have a missing package, ask for it and then compile that one, too.
              case PackageNotFound(dependency) =>
                if (state.seenDependencies.contains(dependency)) {
                  return ResultError(Error(s"Cyclical packages, stumbled upon $dependency twice"))
                }
                return ResultNeedPackage(
                  dependency, {
                    case None => ResultError(Error(s"Could not find package $dependency"))
                    case Some(dependencyPkg) =>
                      addPackageInternal(
                        AddPackageState(
                          packages = state.packages + (dependency -> dependencyPkg),
                          seenDependencies = state.seenDependencies + dependency,
                          toCompile = dependency :: pkgId :: toCompile))
                  }
                )
            }

          // if we made it this far, update
          _packages.put(pkgId, pkg)
          for ((defnId, defn) <- defns) {
            _defns.put(defnId, defn)
          }
        }
      }

      ResultDone(())
    }

  def clear(): Unit = this.synchronized[Unit] {
    _packages.clear()
    _defns.clear()
  }
}

object ConcurrentCompiledPackages {
  def apply(): ConcurrentCompiledPackages = new ConcurrentCompiledPackages()

  private case class AddPackageState(
      packages: Map[PackageId, Package], // the packages we're currently compiling
      seenDependencies: Set[PackageId], // the dependencies we've found so far
      toCompile: List[PackageId])
}
