// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.util.concurrent.ConcurrentHashMap

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.ConcurrentCompiledPackages.AddPackageState
import com.daml.lf.language.Ast.Package
import com.daml.lf.speedy.Compiler

import scala.collection.JavaConverters._
import scala.collection.concurrent.{Map => ConcurrentMap}

/** Thread-safe class that can be used when you need to maintain a shared, mutable collection of
  * packages.
  */
final class ConcurrentCompiledPackages extends MutableCompiledPackages {
  private[this] val _packages: ConcurrentMap[PackageId, Package] =
    new ConcurrentHashMap().asScala
  private[this] val _defns: ConcurrentHashMap[speedy.SExpr.SDefinitionRef, speedy.SExpr] =
    new ConcurrentHashMap()
  private[this] val _packageDeps: ConcurrentHashMap[PackageId, Set[PackageId]] =
    new ConcurrentHashMap()
  private[this] var _profilingMode: speedy.Compiler.ProfilingMode = speedy.Compiler.NoProfile
  private[this] var _stackTraceMode: speedy.Compiler.StackTraceMode = speedy.Compiler.FullStackTrace

  override def getPackage(pId: PackageId): Option[Package] = _packages.get(pId)
  override def getDefinition(dref: speedy.SExpr.SDefinitionRef): Option[speedy.SExpr] =
    Option(_defns.get(dref))

  override def profilingMode: Compiler.ProfilingMode = _profilingMode

  def profilingMode_=(profilingMode: speedy.Compiler.ProfilingMode) = {
    _profilingMode = profilingMode
  }

  override def stackTraceMode = _stackTraceMode

  def stackTraceMode_=(stackTraceMode: speedy.Compiler.StackTraceMode) = {
    _stackTraceMode = stackTraceMode
  }

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

          // At this point all dependencies have been loaded. Update the packages
          // map using 'computeIfAbsent' which will ensure we only compile the
          // package once. Other concurrent calls to add this package will block
          // waiting for the first one to finish.
          if (!_packages.contains(pkgId)) {
            // Compile the speedy definitions for this package.
            val defns =
              speedy
                .Compiler(packages orElse state.packages, stackTraceMode, profilingMode)
                .unsafeCompilePackage(pkgId)
            defns.foreach {
              case (defnId, defn) => _defns.put(defnId, defn)
            }
            // Compute the transitive dependencies of the new package. Since we are adding
            // packages in dependency order we can just union the dependencies of the
            // direct dependencies to get the complete transitive dependencies.
            val deps = pkg.directDeps.foldLeft(pkg.directDeps) {
              case (deps, dependency) =>
                deps union _packageDeps.get(dependency)
            }
            _packageDeps.put(pkgId, deps)
            _packages.put(
              pkgId,
              pkg
            )
          }
        }
      }

      ResultDone.Unit
    }

  def clear(): Unit = this.synchronized[Unit] {
    _packages.clear()
    _packageDeps.clear()
    _defns.clear()
  }

  override def packageIds: Set[PackageId] =
    _packages.keySet.toSet

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
