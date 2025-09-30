// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import java.util.concurrent.ConcurrentHashMap
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.ConcurrentCompiledPackages.AddPackageState
import com.digitalasset.daml.lf.language.Ast.{Package, PackageSignature}
import com.digitalasset.daml.lf.language.{Util => AstUtil}
import com.digitalasset.daml.lf.speedy.Compiler
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
  val signatures: ConcurrentMap[PackageId, PackageSignature] =
    new ConcurrentHashMap().asScala
  val definitions: ConcurrentMap[speedy.SExpr.SDefinitionRef, speedy.SDefinition] =
    new ConcurrentHashMap().asScala
  private[this] val packageDeps: ConcurrentMap[PackageId, Set[PackageId]] =
    new ConcurrentHashMap().asScala

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
      var toCompile = stableIds ++: state.toCompile
      // discard(stableIds)
      // var toCompile = state.toCompile

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
            // for (dependency <- pkg.importsFromEither(false).toList) {
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
              definitions.put(defnId, defn)
            }
            // Compute the transitive dependencies of the new package. Since we are adding
            // packages in dependency order we can just union the dependencies of the
            // direct dependencies to get the complete transitive dependencies.
            // val deps = pkg.importsFromEither(false).foldLeft(pkg.importsFromEither(false)) { case (deps, dependency) =>
            val deps = pkg.directDeps.foldLeft(pkg.directDeps) { case (deps, dependency) =>
              deps union packageDeps(dependency)
            }
            discard(packageDeps.put(pkgId, deps))
            signatures.put(pkgId, pkgSignature)
          }
        }
      }

      ResultDone.Unit
    }

  private val stableIds: Seq[PackageId] =
    Seq(
      "54f85ebfc7dfae18f7d70370015dcc6c6792f60135ab369c44ae52c6fc17c274", // daml-prim
      "ee33fb70918e7aaa3d3fc44d64a399fb2bf5bcefc54201b1690ecd448551ba88", // daml-prim-DA-Exception-ArithmeticError
      "6da1f43a10a179524e840e7288b47bda213339b0552d92e87ae811e52f59fc0e", // daml-prim-DA-Exception-AssertionFailed
      "f181cd661f7af3a60bdaae4b0285a2a67beb55d6910fc8431dbae21a5825ec0f", // daml-prim-DA-Exception-GeneralError
      "91e167fa7a256f21f990c526a0a0df840e99aeef0e67dc1f5415b0309486de74", // daml-prim-DA-Exception-PreconditionFailed
      "0e4a572ab1fb94744abb02243a6bbed6c78fc6e3c8d3f60c655f057692a62816", // daml-prim-DA-Internal-Erased
      "e5411f3d75f072b944bd88e652112a14a3d409c491fd9a51f5f6eede6d3a3348", // daml-prim-DA-Internal-NatSyn-
      "ab068e2f920d0e06347975c2a342b71f8b8e3b4be0f02ead9442caac51aa8877", // daml-prim-DA-Internal-PromotedText
      "5aee9b21b8e9a4c4975b5f4c4198e6e6e8469df49e2010820e792f393db870f4", // daml-prim-DA-Types
      "fcee8dfc1b81c449b421410edd5041c16ab59c45bbea85bcb094d1b17c3e9df7", // daml-prim-GHC-Prim
      "19f0df5fdaf5a96e137b6ea885fdb378f37bd3166bd9a47ee11518e33fa09a20", // daml-prim-GHC-Tuple
      "e7e0adfa881e7dbbb07da065ae54444da7c4bccebcb8872ab0cb5dcf9f3761ce", // daml-prim-GHC-Types
      "a1fa18133ae48cbb616c4c148e78e661666778c3087d099067c7fe1868cbb3a1", // daml-stdlib-DA-Action-State-Type
      "fa79192fe1cce03d7d8db36471dde4cf6c96e6d0f07e1c391dd49e355af9b38c", // daml-stdlib-DA-Date-Types
      "6f8e6085f5769861ae7a40dccd618d6f747297d59b37cab89b93e2fa80b0c024", // daml-stdlib-DA-Internal-Any
      "86d888f34152dae8729900966b44abcb466b9c111699678de58032de601d2b04", // daml-stdlib-DA-Internal-Down
      "7adc4c2d07fa3a51173c843cba36e610c1168b2dbbf53076e20c0092eae8763d", // daml-stdlib-DA-Internal-Fail-Types
      "c280cc3ef501d237efa7b1120ca3ad2d196e089ad596b666bed59a85f3c9a074", // daml-stdlib-DA-Internal-Interface-AnyView-Types
      "9e70a8b3510d617f8a136213f33d6a903a10ca0eeec76bb06ba55d1ed9680f69", // daml-stdlib-DA-Internal-Template
      "cae345b5500ef6f84645c816f88b9f7a85a9f3c71697984abdf6849f81e80324", // daml-stdlib-DA-Logic-Types
      "52854220dc199884704958df38befd5492d78384a032fd7558c38f00e3d778a2", // daml-stdlib-DA-Monoid-Types
      "bde4bd30749e99603e5afa354706608601029e225d4983324d617825b634253a", // daml-stdlib-DA-NonEmpty-Types
      "bfda48f9aa2c89c895cde538ec4b4946c7085959e031ad61bde616b9849155d7", // daml-stdlib-DA-Random-Types
      "d095a2ccf6dd36b2415adc4fa676f9191ba63cd39828dc5207b36892ec350cbc", // daml-stdlib-DA-Semigroup-Types
      "c3bb0c5d04799b3f11bad7c3c102963e115cf53da3e4afcbcfd9f06ebd82b4ff", // daml-stdlib-DA-Set-Types
      "60c61c542207080e97e378ab447cc355ecc47534b3a3ebbff307c4fb8339bc4d", // daml-stdlib-DA-Stack-Types
      "b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946", // daml-stdlib-DA-Time-Types
      "3cde94fe9be5c700fc1d9a8ad2277e2c1214609f8c52a5b4db77e466875b8cb7", // daml-stdlib-DA-Validation-Types
    ).map(s => eitherToParseError(PackageId.fromString(s)))

  private def eitherToParseError[A](x: Either[String, A]): A =
    x.fold(err => throw new RuntimeException(err), identity)

  def clear(): Unit = this.synchronized[Unit] {
    signatures.clear()
    definitions.clear()
    packageDeps.clear()
  }

  def getPackageDependencies(pkgId: PackageId): Option[Set[PackageId]] =
    packageDeps.get(pkgId)
}

object ConcurrentCompiledPackages {
  def apply(compilerConfig: Compiler.Config): ConcurrentCompiledPackages =
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
