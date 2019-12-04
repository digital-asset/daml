// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import java.util.concurrent.ConcurrentHashMap

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.ConcurrentCompiledPackages.AddPackageState
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.speedy
import scala.collection.JavaConverters._

/** Thread-safe class that can be used when you need to maintain a shared, mutable collection of
  * packages.
  */
final class ConcurrentCompiledPackages extends MutableCompiledPackages {
  private[this] val _packages: ConcurrentHashMap[PackageId, Package] =
    new ConcurrentHashMap()
  private[this] val _defns: ConcurrentHashMap[speedy.SExpr.SDefinitionRef, speedy.SExpr] =
    new ConcurrentHashMap()
  private[this] val _packageDeps: ConcurrentHashMap[PackageId, Set[PackageId]] =
    new ConcurrentHashMap()

  def getPackage(pId: PackageId): Option[Package] = Option(_packages.get(pId))
  def getDefinition(dref: speedy.SExpr.SDefinitionRef): Option[speedy.SExpr] =
    Option(_defns.get(dref))

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

  // TODO SC remove 'return', notwithstanding a love of unhandled exceptions
  @SuppressWarnings(Array("org.wartremover.warts.Return"))
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

          // Load dependencies of this package and transitively its dependencies.
          for (dependency <- pkg.directDeps) {
            if (!_packages.contains(dependency) && !state.seenDependencies.contains(dependency)) {
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
          }

          // Compile the speedy definitions for this package.
          val defns =
            try {
              speedy.Compiler(packages orElse state.packages).compilePackage(pkgId)
            } catch {
              // A missing package during compilation is means that the dependencies declared
              // in the package are wrong.
              case speedy.Compiler.PackageNotFound(dependency) =>
                return ResultError(Error(
                  s"Internal error: Invalid direct dependencies, package $dependency found missing during compilation"))
            }

          // if we made it this far, update
          _packages.put(pkgId, pkg)
          for ((defnId, defn) <- defns) {
            _defns.put(defnId, defn)
          }

          // Compute the transitive dependencies of the new package. Since we are adding
          // packages in dependency order we can just union the dependencies of the
          // direct dependencies to get the complete transitive dependencies.
          val deps = pkg.directDeps.foldLeft(pkg.directDeps) {
            case (deps, dependency) =>
              deps union _packageDeps.get(dependency)
          }
          _packageDeps.put(pkgId, deps)
        }
      }

      ResultDone(())
    }

  def clear(): Unit = this.synchronized[Unit] {
    _packages.clear()
    _defns.clear()
  }

  override def packageIds: Set[PackageId] =
    _packages.keySet.asScala.toSet

  def getPackageDependencies(pkgId: PackageId): Option[Set[PackageId]] =
    Option(_packageDeps.get(pkgId))
}

object ConcurrentCompiledPackages {
  def apply(): ConcurrentCompiledPackages = new ConcurrentCompiledPackages()

  private case class AddPackageState(
      packages: Map[PackageId, Package], // the packages we're currently compiling
      seenDependencies: Set[PackageId], // the dependencies we've found so far
      toCompile: List[PackageId])
}
