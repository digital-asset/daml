// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.util.concurrent.ConcurrentHashMap

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.ConcurrentCompiledPackages.AddPackageState
import com.daml.lf.language.Ast.{Package, PackageSignature}
import com.daml.lf.language.{Util => AstUtil}
import com.daml.lf.speedy.Compiler
import com.daml.lf.speedy.Compiler.CompilationError

import scala.collection.JavaConverters._
import scala.collection.concurrent.{Map => ConcurrentMap}

/** Thread-safe class that can be used when you need to maintain a shared, mutable collection of
  * packages.
  */
private[lf] final class ConcurrentCompiledPackages(compilerConfig: Compiler.Config)
    extends MutableCompiledPackages(compilerConfig) {
  private[this] val _signatures: ConcurrentMap[PackageId, PackageSignature] =
    new ConcurrentHashMap().asScala
  private[this] val _defns: ConcurrentHashMap[speedy.SExpr.SDefinitionRef, speedy.SDefinition] =
    new ConcurrentHashMap()
  private[this] val _packageDeps: ConcurrentHashMap[PackageId, Set[PackageId]] =
    new ConcurrentHashMap()

  override def getSignature(pId: PackageId): Option[PackageSignature] = _signatures.get(pId)
  override def getDefinition(dref: speedy.SExpr.SDefinitionRef): Option[speedy.SDefinition] =
    Option(_defns.get(dref))

  /** Might ask for a package if the package you're trying to add references it.
    *
    * Note that when resuming from a [[Result]] the continuation will modify the
    * [[ConcurrentCompiledPackages]] that originated it.
    */
  override def addPackage(pkgId: PackageId, pkg: Package): Result[Unit] =
    addPackageInternal(
      AddPackageState(
        packages = Map(pkgId -> pkg),
        seenDependencies = Set.empty,
        toCompile = List(pkgId)
      )
    )

  // TODO SC remove 'return', notwithstanding a love of unhandled exceptions
  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def addPackageInternal(state: AddPackageState): Result[Unit] =
    this.synchronized {
      var toCompile = state.toCompile

      while (toCompile.nonEmpty) {
        val pkgId: PackageId = toCompile.head
        toCompile = toCompile.tail

        if (!_signatures.contains(pkgId)) {

          val pkg = state.packages.get(pkgId) match {
            case None => return ResultError(Error(s"Could not find package $pkgId"))
            case Some(pkg_) => pkg_
          }

          // Load dependencies of this package and transitively its dependencies.
          for (dependency <- pkg.directDeps) {
            if (!_signatures.contains(dependency) && !state.seenDependencies.contains(dependency)) {
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
          if (!_signatures.contains(pkgId)) {
            val signature = AstUtil.toSignature(pkg)
            val signatureLookup: PartialFunction[PackageId, PackageSignature] = {
              case `pkgId` => signature
            }
            // Compile the speedy definitions for this package.
            val defns = try {
              new speedy.Compiler(signatureLookup orElse _signatures, compilerConfig)
                .unsafeCompilePackage(pkgId, pkg)
            } catch {
              case CompilationError(msg) =>
                return ResultError(Error(s"Compilation Error: $msg"))
              case e: validation.ValidationError =>
                return ResultError(Error(s"Validation Error: ${e.pretty}"))
            }
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
            _signatures.put(pkgId, signature)
          }
        }
      }

      ResultDone.Unit
    }

  def clear(): Unit = this.synchronized[Unit] {
    _signatures.clear()
    _packageDeps.clear()
    _defns.clear()
  }

  override def packageIds: Set[PackageId] =
    _signatures.keySet.toSet

  def getPackageDependencies(pkgId: PackageId): Option[Set[PackageId]] =
    Option(_packageDeps.get(pkgId))
}

object ConcurrentCompiledPackages {
  def apply(compilerConfig: Compiler.Config = Compiler.Config.Default): ConcurrentCompiledPackages =
    new ConcurrentCompiledPackages(compilerConfig)

  private case class AddPackageState(
      packages: Map[PackageId, Package], // the packages we're currently compiling
      seenDependencies: Set[PackageId], // the dependencies we've found so far
      toCompile: List[PackageId])
}
