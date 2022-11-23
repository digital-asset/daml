// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.util.concurrent.ConcurrentHashMap

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.ConcurrentCompiledPackages.AddPackageState
import com.daml.lf.language.Ast.{Package, PackageSignature}
import com.daml.lf.language.{PackageInterface, Util => AstUtil}
import com.daml.lf.speedy.Compiler
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard

import scala.jdk.CollectionConverters._
import scala.collection.concurrent.{Map => ConcurrentMap}
import scala.util.control.NonFatal

/** Thread-safe class that can be used when you need to maintain a shared, mutable collection of
  * packages.
  */
private[lf] final class ConcurrentCompiledPackages(compilerConfig: Compiler.Config)
    extends MutableCompiledPackages(compilerConfig) {
  private[this] val signatures: ConcurrentMap[PackageId, PackageSignature] =
    new ConcurrentHashMap().asScala
  private[this] val definitionsByReference
      : ConcurrentHashMap[speedy.SExpr.SDefinitionRef, speedy.SDefinition] =
    new ConcurrentHashMap()
  private[this] val packageDeps: ConcurrentHashMap[PackageId, Set[PackageId]] =
    new ConcurrentHashMap()

  override def packageIds: scala.collection.Set[PackageId] = signatures.keySet
  override def pkgInterface: PackageInterface = new PackageInterface(signatures)
  override def getDefinition(dref: speedy.SExpr.SDefinitionRef): Option[speedy.SDefinition] =
    Option(definitionsByReference.get(dref))

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
        toCompile = List(pkgId),
      )
    )

  // TODO SC remove 'return', notwithstanding a love of unhandled exceptions
  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  @scala.annotation.nowarn("msg=return statement uses an exception to pass control to the caller")
  private def addPackageInternal(state: AddPackageState): Result[Unit] =
    this.synchronized {
      var toCompile = state.toCompile

      while (toCompile.nonEmpty) {
        val pkgId: PackageId = toCompile.head
        toCompile = toCompile.tail

        if (!signatures.contains(pkgId)) {

          val pkg = state.packages.get(pkgId) match {
            case None =>
              return ResultError(
                Error.Package.Internal(
                  NameOf.qualifiedNameOfCurrentFunc,
                  s"broken invariant: Could not find package $pkgId",
                  None,
                )
              )
            case Some(pkg_) => pkg_
          }

          // Load dependencies of this package and transitively its dependencies.
          for (dependency <- pkg.directDeps) {
            if (!signatures.contains(dependency) && !state.seenDependencies.contains(dependency)) {
              return ResultNeedPackage(
                dependency,
                {
                  case None =>
                    ResultError(Error.Package.MissingPackage(dependency))
                  case Some(dependencyPkg) =>
                    addPackageInternal(
                      AddPackageState(
                        packages = state.packages + (dependency -> dependencyPkg),
                        seenDependencies = state.seenDependencies + dependency,
                        toCompile = dependency :: pkgId :: toCompile,
                      )
                    )
                },
              )
            }
          }

          // At this point all dependencies have been loaded. Update the packages
          // map using 'computeIfAbsent' which will ensure we only compile the
          // package once. Other concurrent calls to add this package will block
          // waiting for the first one to finish.
          if (!signatures.contains(pkgId)) {
            val pkgSignature = AstUtil.toSignature(pkg)
            val extendedSignatures =
              new language.PackageInterface(Map(pkgId -> pkgSignature) orElse signatures)

            // Compile the speedy definitions for this package.
            val defns =
              try {
                new speedy.Compiler(extendedSignatures, compilerConfig)
                  .unsafeCompilePackage(pkgId, pkg)
              } catch {
                case e: validation.ValidationError =>
                  return ResultError(Error.Package.Validation(e))
                case Compiler.LanguageVersionError(
                      packageId,
                      languageVersion,
                      allowedLanguageVersions,
                    ) =>
                  return ResultError(
                    Error.Package
                      .AllowedLanguageVersion(packageId, languageVersion, allowedLanguageVersions)
                  )
                case err @ Compiler.CompilationError(msg) =>
                  return ResultError(
                    // compilation errors are internal since typechecking should
                    // catch any errors arising during compilation
                    Error.Package.Internal(
                      NameOf.qualifiedNameOfCurrentFunc,
                      s"Compilation Error: $msg",
                      Some(err),
                    )
                  )
                case NonFatal(err) =>
                  return ResultError(
                    Error.Package.Internal(
                      NameOf.qualifiedNameOfCurrentFunc,
                      s"Unexpected ${err.getClass.getSimpleName} Exception",
                      Some(err),
                    )
                  )
              }
            defns.foreach { case (defnId, defn) =>
              definitionsByReference.put(defnId, defn)
            }
            // Compute the transitive dependencies of the new package. Since we are adding
            // packages in dependency order we can just union the dependencies of the
            // direct dependencies to get the complete transitive dependencies.
            val deps = pkg.directDeps.foldLeft(pkg.directDeps) { case (deps, dependency) =>
              deps union packageDeps.get(dependency)
            }
            discard(packageDeps.put(pkgId, deps))
            signatures.put(pkgId, pkgSignature)
          }
        }
      }

      ResultDone.Unit
    }

  def clear(): Unit = this.synchronized[Unit] {
    signatures.clear()
    packageDeps.clear()
    definitionsByReference.clear()
  }

  def getPackageDependencies(pkgId: PackageId): Option[Set[PackageId]] =
    Option(packageDeps.get(pkgId))
}

object ConcurrentCompiledPackages {
  def apply(compilerConfig: Compiler.Config = Compiler.Config.Default): ConcurrentCompiledPackages =
    new ConcurrentCompiledPackages(compilerConfig)

  private case class AddPackageState(
      packages: Map[PackageId, Package], // the packages we're currently compiling
      seenDependencies: Set[PackageId], // the dependencies we've found so far
      toCompile: List[PackageId],
  ) {
    // Invariant
    // assert(toCompile.forall(packages.contains))
  }
}
