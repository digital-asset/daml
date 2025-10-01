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
              definitions.put(defnId, defn)
            }
            // Compute the transitive dependencies of the new package. Since we are adding
            // packages in dependency order we can just union the dependencies of the
            // direct dependencies to get the complete transitive dependencies.
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

// info on the breakage
/*
 PreprocessorSpecV2:
FAIL: //daml-lf/engine:tests_test_suite_src_test_scala_com_digitalasset_daml_lf_engine_PreprocessorSpec.scala (see /home/roger.bosman/.cache/bazel/_bazel_roger.bosman/d2bc5e364f3f82cbea0c650e3e424d0d/execroot/com_github_digital_asset_daml/bazel-out/k8-opt/testlogs/daml-lf/engine/tests_test_suite_src_test_scala_com_digitalasset_daml_lf_engine_PreprocessorSpec.scala/test.log)
INFO: From Testing //daml-lf/engine:tests_test_suite_src_test_scala_com_digitalasset_daml_lf_engine_PreprocessorSpec.scala:
==================== Test output for //daml-lf/engine:tests_test_suite_src_test_scala_com_digitalasset_daml_lf_engine_PreprocessorSpec.scala:
Discovery starting.
SLF4J: No SLF4J providers were found.
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See https://www.slf4j.org/codes.html#noProviders for further details.
Discovery completed in 2 seconds, 859 milliseconds.
Run starting. Expected test count is: 11
PreprocessorSpecV2:
preprocessor
- should returns correct result when resuming *** FAILED *** (99 milliseconds)
  Left(Package(MissingPackage(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946,Package(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946)))) was not an instance of scala.util.Right, but an instance of scala.util.Left (PreprocessorSpec.scala:67)
  org.scalatest.exceptions.TestFailedException:
  at org.scalatest.matchers.MatchersHelper$.newTestFailedException(MatchersHelper.scala:143)
  at org.scalatest.matchers.TypeMatcherHelper$.assertAType(TypeMatcherHelper.scala:162)
  at com.digitalasset.daml.lf.engine.PreprocessorSpec.$anonfun$new$2(PreprocessorSpec.scala:67)
  at org.scalatest.OutcomeOf.outcomeOf(OutcomeOf.scala:85)
  at org.scalatest.OutcomeOf.outcomeOf$(OutcomeOf.scala:83)
  at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
  at org.scalatest.Transformer.apply(Transformer.scala:22)
  at org.scalatest.Transformer.apply(Transformer.scala:20)
  at org.scalatest.wordspec.AnyWordSpecLike$$anon$3.apply(AnyWordSpecLike.scala:1227)
  at org.scalatest.TestSuite.withFixture(TestSuite.scala:196)
  at org.scalatest.TestSuite.withFixture$(TestSuite.scala:195)
  at org.scalatest.wordspec.AnyWordSpec.withFixture(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.invokeWithFixture$1(AnyWordSpecLike.scala:1225)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTest$1(AnyWordSpecLike.scala:1237)
  at org.scalatest.SuperEngine.runTestImpl(Engine.scala:306)
  at org.scalatest.wordspec.AnyWordSpecLike.runTest(AnyWordSpecLike.scala:1237)
  at org.scalatest.wordspec.AnyWordSpecLike.runTest$(AnyWordSpecLike.scala:1219)
  at org.scalatest.wordspec.AnyWordSpec.runTest(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTests$1(AnyWordSpecLike.scala:1296)
  at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:413)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
  at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
  at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
  at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:396)
  at org.scalatest.SuperEngine.runTestsImpl(Engine.scala:475)
  at org.scalatest.wordspec.AnyWordSpecLike.runTests(AnyWordSpecLike.scala:1296)
  at org.scalatest.wordspec.AnyWordSpecLike.runTests$(AnyWordSpecLike.scala:1295)
  at org.scalatest.wordspec.AnyWordSpec.runTests(AnyWordSpec.scala:1879)
  at org.scalatest.Suite.run(Suite.scala:1112)
  at org.scalatest.Suite.run$(Suite.scala:1094)
  at org.scalatest.wordspec.AnyWordSpec.org$scalatest$wordspec$AnyWordSpecLike$$super$run(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$run$1(AnyWordSpecLike.scala:1341)
  at org.scalatest.SuperEngine.runImpl(Engine.scala:535)
  at org.scalatest.wordspec.AnyWordSpecLike.run(AnyWordSpecLike.scala:1341)
  at org.scalatest.wordspec.AnyWordSpecLike.run$(AnyWordSpecLike.scala:1339)
  at org.scalatest.wordspec.AnyWordSpec.run(AnyWordSpec.scala:1879)
  at org.scalatest.Suite.callExecuteOnSuite$1(Suite.scala:1175)
  at org.scalatest.Suite.$anonfun$runNestedSuites$1(Suite.scala:1222)
  at scala.collection.ArrayOps$.foreach$extension(ArrayOps.scala:1324)
  at org.scalatest.Suite.runNestedSuites(Suite.scala:1220)
  at org.scalatest.Suite.runNestedSuites$(Suite.scala:1154)
  at org.scalatest.tools.DiscoverySuite.runNestedSuites(DiscoverySuite.scala:30)
  at org.scalatest.Suite.run(Suite.scala:1109)
  at org.scalatest.Suite.run$(Suite.scala:1094)
  at org.scalatest.tools.DiscoverySuite.run(DiscoverySuite.scala:30)
  at org.scalatest.tools.SuiteRunner.run(SuiteRunner.scala:45)
  at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13(Runner.scala:1322)
  at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13$adapted(Runner.scala:1316)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.tools.Runner$.doRunRunRunDaDoRunRun(Runner.scala:1316)
  at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24(Runner.scala:993)
  at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24$adapted(Runner.scala:971)
  at org.scalatest.tools.Runner$.withClassLoaderAndDispatchReporter(Runner.scala:1482)
  at org.scalatest.tools.Runner$.runOptionallyWithPassFailReporter(Runner.scala:971)
  at org.scalatest.tools.Runner$.main(Runner.scala:775)
  at org.scalatest.tools.Runner.main(Runner.scala)
  at io.bazel.rulesscala.scala_test.Runner.main(Runner.java:33)
- should returns correct error when resuming *** FAILED *** (4 milliseconds)
  The partial function passed as the second parameter to inside was not defined at the value passed as the first parameter to inside, which was: Left(Package(MissingPackage(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946,Package(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946)))) (PreprocessorSpec.scala:82)
  org.scalatest.exceptions.TestFailedException:
  at org.scalatest.Inside$.insideWithPos(Inside.scala:181)
  at org.scalatest.Inside.inside(Inside.scala:105)
  at org.scalatest.Inside.inside$(Inside.scala:104)
  at com.digitalasset.daml.lf.engine.PreprocessorSpec.inside(PreprocessorSpec.scala:45)
  at com.digitalasset.daml.lf.engine.PreprocessorSpec.$anonfun$new$3(PreprocessorSpec.scala:82)
  at org.scalatest.OutcomeOf.outcomeOf(OutcomeOf.scala:85)
  at org.scalatest.OutcomeOf.outcomeOf$(OutcomeOf.scala:83)
  at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
  at org.scalatest.Transformer.apply(Transformer.scala:22)
  at org.scalatest.Transformer.apply(Transformer.scala:20)
  at org.scalatest.wordspec.AnyWordSpecLike$$anon$3.apply(AnyWordSpecLike.scala:1227)
  at org.scalatest.TestSuite.withFixture(TestSuite.scala:196)
  at org.scalatest.TestSuite.withFixture$(TestSuite.scala:195)
  at org.scalatest.wordspec.AnyWordSpec.withFixture(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.invokeWithFixture$1(AnyWordSpecLike.scala:1225)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTest$1(AnyWordSpecLike.scala:1237)
  at org.scalatest.SuperEngine.runTestImpl(Engine.scala:306)
  at org.scalatest.wordspec.AnyWordSpecLike.runTest(AnyWordSpecLike.scala:1237)
  at org.scalatest.wordspec.AnyWordSpecLike.runTest$(AnyWordSpecLike.scala:1219)
  at org.scalatest.wordspec.AnyWordSpec.runTest(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTests$1(AnyWordSpecLike.scala:1296)
  at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:413)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
  at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
  at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
  at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:396)
  at org.scalatest.SuperEngine.runTestsImpl(Engine.scala:475)
  at org.scalatest.wordspec.AnyWordSpecLike.runTests(AnyWordSpecLike.scala:1296)
  at org.scalatest.wordspec.AnyWordSpecLike.runTests$(AnyWordSpecLike.scala:1295)
  at org.scalatest.wordspec.AnyWordSpec.runTests(AnyWordSpec.scala:1879)
  at org.scalatest.Suite.run(Suite.scala:1112)
  at org.scalatest.Suite.run$(Suite.scala:1094)
  at org.scalatest.wordspec.AnyWordSpec.org$scalatest$wordspec$AnyWordSpecLike$$super$run(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$run$1(AnyWordSpecLike.scala:1341)
  at org.scalatest.SuperEngine.runImpl(Engine.scala:535)
  at org.scalatest.wordspec.AnyWordSpecLike.run(AnyWordSpecLike.scala:1341)
  at org.scalatest.wordspec.AnyWordSpecLike.run$(AnyWordSpecLike.scala:1339)
  at org.scalatest.wordspec.AnyWordSpec.run(AnyWordSpec.scala:1879)
  at org.scalatest.Suite.callExecuteOnSuite$1(Suite.scala:1175)
  at org.scalatest.Suite.$anonfun$runNestedSuites$1(Suite.scala:1222)
  at scala.collection.ArrayOps$.foreach$extension(ArrayOps.scala:1324)
  at org.scalatest.Suite.runNestedSuites(Suite.scala:1220)
  at org.scalatest.Suite.runNestedSuites$(Suite.scala:1154)
  at org.scalatest.tools.DiscoverySuite.runNestedSuites(DiscoverySuite.scala:30)
  at org.scalatest.Suite.run(Suite.scala:1109)
  at org.scalatest.Suite.run$(Suite.scala:1094)
  at org.scalatest.tools.DiscoverySuite.run(DiscoverySuite.scala:30)
  at org.scalatest.tools.SuiteRunner.run(SuiteRunner.scala:45)
  at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13(Runner.scala:1322)
  at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13$adapted(Runner.scala:1316)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.tools.Runner$.doRunRunRunDaDoRunRun(Runner.scala:1316)
  at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24(Runner.scala:993)
  at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24$adapted(Runner.scala:971)
  at org.scalatest.tools.Runner$.withClassLoaderAndDispatchReporter(Runner.scala:1482)
  at org.scalatest.tools.Runner$.runOptionallyWithPassFailReporter(Runner.scala:971)
  at org.scalatest.tools.Runner$.main(Runner.scala:775)
  at org.scalatest.tools.Runner.main(Runner.scala)
  at io.bazel.rulesscala.scala_test.Runner.main(Runner.java:33)
- should resolve package name *** FAILED *** (36 milliseconds)
  tableDrivenForEveryFailed:   org.scalatest.exceptions.TableDrivenPropertyCheckFailedException: Lookup was thrown during property evaluation. (PreprocessorSpec.scala:130)
      Message: "None"
      Occurred at table row 0 (zero based, not counting headings), which had values (
      commands = Create(#my-nice-package:Mod:WithoutKey,ValueRecord(None,ImmArray((None,ValueList(FrontStack(ValueParty(Alice)))),(None,ValueInt64(42)))))  ),
    org.scalatest.exceptions.TableDrivenPropertyCheckFailedException: Lookup was thrown during property evaluation. (PreprocessorSpec.scala:130)
      Message: "None"
      Occurred at table row 1 (zero based, not counting headings), which had values (
      commands = Exercise(#my-nice-package:Mod:WithoutKey,ContractId(002dcb1ec55082e743153fb6ad3a012cb9e706433b50e5b8a56c9e8693134f3c7cdeadbeef),Noop,ValueUnit)  ),
    org.scalatest.exceptions.TableDrivenPropertyCheckFailedException: Lookup was thrown during property evaluation. (PreprocessorSpec.scala:130)
      Message: "None"
      Occurred at table row 2 (zero based, not counting headings), which had values (
      commands = ExerciseByKey(#my-nice-package:Mod:WithKey,ValueList(FrontStack(ValueParty(Alice))),Noop,ValueUnit)  ),
    org.scalatest.exceptions.TableDrivenPropertyCheckFailedException: Lookup was thrown during property evaluation. (PreprocessorSpec.scala:130)
      Message: "None"
      Occurred at table row 3 (zero based, not counting headings), which had values (
      commands = CreateAndExercise(#my-nice-package:Mod:WithoutKey,ValueRecord(None,ImmArray((None,ValueList(FrontStack(ValueParty(Alice)))),(None,ValueInt64(42)))),Noop,ValueUnit)  ) (PreprocessorSpec.scala:130)
  org.scalatest.exceptions.TestFailedException:
  at org.scalatest.enablers.TableAsserting$$anon$2.indicateFailure(TableAsserting.scala:5403)
  at org.scalatest.enablers.TableAsserting$$anon$2.indicateFailure(TableAsserting.scala:5385)
  at org.scalatest.enablers.UnitTableAsserting$TableAssertingImpl.doForEvery(TableAsserting.scala:2029)
  at org.scalatest.enablers.UnitTableAsserting$TableAssertingImpl.forEvery(TableAsserting.scala:1764)
  at org.scalatest.prop.TableFor1.forEvery(TableFor1.scala:194)
  at org.scalatest.prop.TableDrivenPropertyChecks.forEvery(TableDrivenPropertyChecks.scala:649)
  at org.scalatest.prop.TableDrivenPropertyChecks.forEvery$(TableDrivenPropertyChecks.scala:648)
  at com.digitalasset.daml.lf.engine.PreprocessorSpec.forEvery(PreprocessorSpec.scala:45)
  at com.digitalasset.daml.lf.engine.PreprocessorSpec.$anonfun$new$4(PreprocessorSpec.scala:130)
  at org.scalatest.OutcomeOf.outcomeOf(OutcomeOf.scala:85)
  at org.scalatest.OutcomeOf.outcomeOf$(OutcomeOf.scala:83)
  at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
  at org.scalatest.Transformer.apply(Transformer.scala:22)
  at org.scalatest.Transformer.apply(Transformer.scala:20)
  at org.scalatest.wordspec.AnyWordSpecLike$$anon$3.apply(AnyWordSpecLike.scala:1227)
  at org.scalatest.TestSuite.withFixture(TestSuite.scala:196)
  at org.scalatest.TestSuite.withFixture$(TestSuite.scala:195)
  at org.scalatest.wordspec.AnyWordSpec.withFixture(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.invokeWithFixture$1(AnyWordSpecLike.scala:1225)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTest$1(AnyWordSpecLike.scala:1237)
  at org.scalatest.SuperEngine.runTestImpl(Engine.scala:306)
  at org.scalatest.wordspec.AnyWordSpecLike.runTest(AnyWordSpecLike.scala:1237)
  at org.scalatest.wordspec.AnyWordSpecLike.runTest$(AnyWordSpecLike.scala:1219)
  at org.scalatest.wordspec.AnyWordSpec.runTest(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTests$1(AnyWordSpecLike.scala:1296)
  at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:413)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
  at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
  at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
  at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:396)
  at org.scalatest.SuperEngine.runTestsImpl(Engine.scala:475)
  at org.scalatest.wordspec.AnyWordSpecLike.runTests(AnyWordSpecLike.scala:1296)
  at org.scalatest.wordspec.AnyWordSpecLike.runTests$(AnyWordSpecLike.scala:1295)
  at org.scalatest.wordspec.AnyWordSpec.runTests(AnyWordSpec.scala:1879)
  at org.scalatest.Suite.run(Suite.scala:1112)
  at org.scalatest.Suite.run$(Suite.scala:1094)
  at org.scalatest.wordspec.AnyWordSpec.org$scalatest$wordspec$AnyWordSpecLike$$super$run(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$run$1(AnyWordSpecLike.scala:1341)
  at org.scalatest.SuperEngine.runImpl(Engine.scala:535)
  at org.scalatest.wordspec.AnyWordSpecLike.run(AnyWordSpecLike.scala:1341)
  at org.scalatest.wordspec.AnyWordSpecLike.run$(AnyWordSpecLike.scala:1339)
  at org.scalatest.wordspec.AnyWordSpec.run(AnyWordSpec.scala:1879)
  at org.scalatest.Suite.callExecuteOnSuite$1(Suite.scala:1175)
  at org.scalatest.Suite.$anonfun$runNestedSuites$1(Suite.scala:1222)
  at scala.collection.ArrayOps$.foreach$extension(ArrayOps.scala:1324)
  at org.scalatest.Suite.runNestedSuites(Suite.scala:1220)
  at org.scalatest.Suite.runNestedSuites$(Suite.scala:1154)
  at org.scalatest.tools.DiscoverySuite.runNestedSuites(DiscoverySuite.scala:30)
  at org.scalatest.Suite.run(Suite.scala:1109)
  at org.scalatest.Suite.run$(Suite.scala:1094)
  at org.scalatest.tools.DiscoverySuite.run(DiscoverySuite.scala:30)
  at org.scalatest.tools.SuiteRunner.run(SuiteRunner.scala:45)
  at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13(Runner.scala:1322)
  at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13$adapted(Runner.scala:1316)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.tools.Runner$.doRunRunRunDaDoRunRun(Runner.scala:1316)
  at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24(Runner.scala:993)
  at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24$adapted(Runner.scala:971)
  at org.scalatest.tools.Runner$.withClassLoaderAndDispatchReporter(Runner.scala:1482)
  at org.scalatest.tools.Runner$.runOptionallyWithPassFailReporter(Runner.scala:971)
  at org.scalatest.tools.Runner$.main(Runner.scala:775)
  at org.scalatest.tools.Runner.main(Runner.scala)
  at io.bazel.rulesscala.scala_test.Runner.main(Runner.java:33)
  Cause: org.scalatest.exceptions.TableDrivenPropertyCheckFailedException: Lookup was thrown during property evaluation. (PreprocessorSpec.scala:130)
  Message: "None"
  Occurred at table row 0 (zero based, not counting headings), which had values (
  commands = Create(#my-nice-package:Mod:WithoutKey,ValueRecord(None,ImmArray((None,ValueList(FrontStack(ValueParty(Alice)))),(None,ValueInt64(42)))))  )
  at org.scalatest.enablers.UnitTableAsserting$TableAssertingImpl.innerRunAndCollectResult$1(TableAsserting.scala:2006)
  at org.scalatest.enablers.UnitTableAsserting$TableAssertingImpl.runAndCollectResult(TableAsserting.scala:2017)
  at org.scalatest.enablers.UnitTableAsserting$TableAssertingImpl.doForEvery(TableAsserting.scala:2022)
  at org.scalatest.enablers.UnitTableAsserting$TableAssertingImpl.forEvery(TableAsserting.scala:1764)
  at org.scalatest.prop.TableFor1.forEvery(TableFor1.scala:194)
  at org.scalatest.prop.TableDrivenPropertyChecks.forEvery(TableDrivenPropertyChecks.scala:649)
  at org.scalatest.prop.TableDrivenPropertyChecks.forEvery$(TableDrivenPropertyChecks.scala:648)
  at com.digitalasset.daml.lf.engine.PreprocessorSpec.forEvery(PreprocessorSpec.scala:45)
  at com.digitalasset.daml.lf.engine.PreprocessorSpec.$anonfun$new$4(PreprocessorSpec.scala:130)
  at org.scalatest.OutcomeOf.outcomeOf(OutcomeOf.scala:85)
  at org.scalatest.OutcomeOf.outcomeOf$(OutcomeOf.scala:83)
  at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
  at org.scalatest.Transformer.apply(Transformer.scala:22)
  at org.scalatest.Transformer.apply(Transformer.scala:20)
  at org.scalatest.wordspec.AnyWordSpecLike$$anon$3.apply(AnyWordSpecLike.scala:1227)
  at org.scalatest.TestSuite.withFixture(TestSuite.scala:196)
  at org.scalatest.TestSuite.withFixture$(TestSuite.scala:195)
  at org.scalatest.wordspec.AnyWordSpec.withFixture(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.invokeWithFixture$1(AnyWordSpecLike.scala:1225)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTest$1(AnyWordSpecLike.scala:1237)
  at org.scalatest.SuperEngine.runTestImpl(Engine.scala:306)
  at org.scalatest.wordspec.AnyWordSpecLike.runTest(AnyWordSpecLike.scala:1237)
  at org.scalatest.wordspec.AnyWordSpecLike.runTest$(AnyWordSpecLike.scala:1219)
  at org.scalatest.wordspec.AnyWordSpec.runTest(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTests$1(AnyWordSpecLike.scala:1296)
  at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:413)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
  at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
  at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
  at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:396)
  at org.scalatest.SuperEngine.runTestsImpl(Engine.scala:475)
  at org.scalatest.wordspec.AnyWordSpecLike.runTests(AnyWordSpecLike.scala:1296)
  at org.scalatest.wordspec.AnyWordSpecLike.runTests$(AnyWordSpecLike.scala:1295)
  at org.scalatest.wordspec.AnyWordSpec.runTests(AnyWordSpec.scala:1879)
  at org.scalatest.Suite.run(Suite.scala:1112)
  at org.scalatest.Suite.run$(Suite.scala:1094)
  at org.scalatest.wordspec.AnyWordSpec.org$scalatest$wordspec$AnyWordSpecLike$$super$run(AnyWordSpec.scala:1879)
  at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$run$1(AnyWordSpecLike.scala:1341)
  at org.scalatest.SuperEngine.runImpl(Engine.scala:535)
  at org.scalatest.wordspec.AnyWordSpecLike.run(AnyWordSpecLike.scala:1341)
  at org.scalatest.wordspec.AnyWordSpecLike.run$(AnyWordSpecLike.scala:1339)
  at org.scalatest.wordspec.AnyWordSpec.run(AnyWordSpec.scala:1879)
  at org.scalatest.Suite.callExecuteOnSuite$1(Suite.scala:1175)
  at org.scalatest.Suite.$anonfun$runNestedSuites$1(Suite.scala:1222)
  at scala.collection.ArrayOps$.foreach$extension(ArrayOps.scala:1324)
  at org.scalatest.Suite.runNestedSuites(Suite.scala:1220)
  at org.scalatest.Suite.runNestedSuites$(Suite.scala:1154)
  at org.scalatest.tools.DiscoverySuite.runNestedSuites(DiscoverySuite.scala:30)
  at org.scalatest.Suite.run(Suite.scala:1109)
  at org.scalatest.Suite.run$(Suite.scala:1094)
  at org.scalatest.tools.DiscoverySuite.run(DiscoverySuite.scala:30)
  at org.scalatest.tools.SuiteRunner.run(SuiteRunner.scala:45)
  at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13(Runner.scala:1322)
  at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13$adapted(Runner.scala:1316)
  at scala.collection.immutable.List.foreach(List.scala:334)
  at org.scalatest.tools.Runner$.doRunRunRunDaDoRunRun(Runner.scala:1316)
  at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24(Runner.scala:993)
  at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24$adapted(Runner.scala:971)
  at org.scalatest.tools.Runner$.withClassLoaderAndDispatchReporter(Runner.scala:1482)
  at org.scalatest.tools.Runner$.runOptionallyWithPassFailReporter(Runner.scala:971)
  at org.scalatest.tools.Runner$.main(Runner.scala:775)
  at org.scalatest.tools.Runner.main(Runner.scala)
  at io.bazel.rulesscala.scala_test.Runner.main(Runner.java:33)
  Cause: com.digitalasset.daml.lf.engine.Error$Preprocessing$Lookup:
- should reject command with unknown package name (8 milliseconds)
  should build package resolution map
  - when provided with consistent package-map and preference set inputs (31 milliseconds)
  should return correct error message
  - when provided with a package-id that does not have a package-name counterpart in the package map (2 milliseconds)
  - when provided with multiple package-ids referencing the same package-name (4 milliseconds)
  should prefetchContractIdsAndKeys
  - should extract the keys from ExerciseByKey commands *** FAILED *** (8 milliseconds)
    scala.MatchError: Left(Package(MissingPackage(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946,Package(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946)))) (of class scala.util.Left)
    at com.digitalasset.daml.lf.engine.PreprocessorSpec.$anonfun$new$19(PreprocessorSpec.scala:290)
    at org.scalatest.OutcomeOf.outcomeOf(OutcomeOf.scala:85)
    at org.scalatest.OutcomeOf.outcomeOf$(OutcomeOf.scala:83)
    at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
    at org.scalatest.Transformer.apply(Transformer.scala:22)
    at org.scalatest.Transformer.apply(Transformer.scala:20)
    at org.scalatest.wordspec.AnyWordSpecLike$$anon$3.apply(AnyWordSpecLike.scala:1227)
    at org.scalatest.TestSuite.withFixture(TestSuite.scala:196)
    at org.scalatest.TestSuite.withFixture$(TestSuite.scala:195)
    at org.scalatest.wordspec.AnyWordSpec.withFixture(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.invokeWithFixture$1(AnyWordSpecLike.scala:1225)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTest$1(AnyWordSpecLike.scala:1237)
    at org.scalatest.SuperEngine.runTestImpl(Engine.scala:306)
    at org.scalatest.wordspec.AnyWordSpecLike.runTest(AnyWordSpecLike.scala:1237)
    at org.scalatest.wordspec.AnyWordSpecLike.runTest$(AnyWordSpecLike.scala:1219)
    at org.scalatest.wordspec.AnyWordSpec.runTest(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTests$1(AnyWordSpecLike.scala:1296)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:413)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:396)
    at org.scalatest.SuperEngine.runTestsImpl(Engine.scala:475)
    at org.scalatest.wordspec.AnyWordSpecLike.runTests(AnyWordSpecLike.scala:1296)
    at org.scalatest.wordspec.AnyWordSpecLike.runTests$(AnyWordSpecLike.scala:1295)
    at org.scalatest.wordspec.AnyWordSpec.runTests(AnyWordSpec.scala:1879)
    at org.scalatest.Suite.run(Suite.scala:1112)
    at org.scalatest.Suite.run$(Suite.scala:1094)
    at org.scalatest.wordspec.AnyWordSpec.org$scalatest$wordspec$AnyWordSpecLike$$super$run(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$run$1(AnyWordSpecLike.scala:1341)
    at org.scalatest.SuperEngine.runImpl(Engine.scala:535)
    at org.scalatest.wordspec.AnyWordSpecLike.run(AnyWordSpecLike.scala:1341)
    at org.scalatest.wordspec.AnyWordSpecLike.run$(AnyWordSpecLike.scala:1339)
    at org.scalatest.wordspec.AnyWordSpec.run(AnyWordSpec.scala:1879)
    at org.scalatest.Suite.callExecuteOnSuite$1(Suite.scala:1175)
    at org.scalatest.Suite.$anonfun$runNestedSuites$1(Suite.scala:1222)
    at scala.collection.ArrayOps$.foreach$extension(ArrayOps.scala:1324)
    at org.scalatest.Suite.runNestedSuites(Suite.scala:1220)
    at org.scalatest.Suite.runNestedSuites$(Suite.scala:1154)
    at org.scalatest.tools.DiscoverySuite.runNestedSuites(DiscoverySuite.scala:30)
    at org.scalatest.Suite.run(Suite.scala:1109)
    at org.scalatest.Suite.run$(Suite.scala:1094)
    at org.scalatest.tools.DiscoverySuite.run(DiscoverySuite.scala:30)
    at org.scalatest.tools.SuiteRunner.run(SuiteRunner.scala:45)
    at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13(Runner.scala:1322)
    at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13$adapted(Runner.scala:1316)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.tools.Runner$.doRunRunRunDaDoRunRun(Runner.scala:1316)
    at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24(Runner.scala:993)
    at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24$adapted(Runner.scala:971)
    at org.scalatest.tools.Runner$.withClassLoaderAndDispatchReporter(Runner.scala:1482)
    at org.scalatest.tools.Runner$.runOptionallyWithPassFailReporter(Runner.scala:971)
    at org.scalatest.tools.Runner$.main(Runner.scala:775)
    at org.scalatest.tools.Runner.main(Runner.scala)
    at io.bazel.rulesscala.scala_test.Runner.main(Runner.java:33)
  - should include explicitly specified keys *** FAILED *** (8 milliseconds)
    scala.MatchError: Left(Package(MissingPackage(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946,Package(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946)))) (of class scala.util.Left)
    at com.digitalasset.daml.lf.engine.PreprocessorSpec.$anonfun$new$20(PreprocessorSpec.scala:313)
    at org.scalatest.OutcomeOf.outcomeOf(OutcomeOf.scala:85)
    at org.scalatest.OutcomeOf.outcomeOf$(OutcomeOf.scala:83)
    at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
    at org.scalatest.Transformer.apply(Transformer.scala:22)
    at org.scalatest.Transformer.apply(Transformer.scala:20)
    at org.scalatest.wordspec.AnyWordSpecLike$$anon$3.apply(AnyWordSpecLike.scala:1227)
    at org.scalatest.TestSuite.withFixture(TestSuite.scala:196)
    at org.scalatest.TestSuite.withFixture$(TestSuite.scala:195)
    at org.scalatest.wordspec.AnyWordSpec.withFixture(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.invokeWithFixture$1(AnyWordSpecLike.scala:1225)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTest$1(AnyWordSpecLike.scala:1237)
    at org.scalatest.SuperEngine.runTestImpl(Engine.scala:306)
    at org.scalatest.wordspec.AnyWordSpecLike.runTest(AnyWordSpecLike.scala:1237)
    at org.scalatest.wordspec.AnyWordSpecLike.runTest$(AnyWordSpecLike.scala:1219)
    at org.scalatest.wordspec.AnyWordSpec.runTest(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTests$1(AnyWordSpecLike.scala:1296)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:413)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:396)
    at org.scalatest.SuperEngine.runTestsImpl(Engine.scala:475)
    at org.scalatest.wordspec.AnyWordSpecLike.runTests(AnyWordSpecLike.scala:1296)
    at org.scalatest.wordspec.AnyWordSpecLike.runTests$(AnyWordSpecLike.scala:1295)
    at org.scalatest.wordspec.AnyWordSpec.runTests(AnyWordSpec.scala:1879)
    at org.scalatest.Suite.run(Suite.scala:1112)
    at org.scalatest.Suite.run$(Suite.scala:1094)
    at org.scalatest.wordspec.AnyWordSpec.org$scalatest$wordspec$AnyWordSpecLike$$super$run(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$run$1(AnyWordSpecLike.scala:1341)
    at org.scalatest.SuperEngine.runImpl(Engine.scala:535)
    at org.scalatest.wordspec.AnyWordSpecLike.run(AnyWordSpecLike.scala:1341)
    at org.scalatest.wordspec.AnyWordSpecLike.run$(AnyWordSpecLike.scala:1339)
    at org.scalatest.wordspec.AnyWordSpec.run(AnyWordSpec.scala:1879)
    at org.scalatest.Suite.callExecuteOnSuite$1(Suite.scala:1175)
    at org.scalatest.Suite.$anonfun$runNestedSuites$1(Suite.scala:1222)
    at scala.collection.ArrayOps$.foreach$extension(ArrayOps.scala:1324)
    at org.scalatest.Suite.runNestedSuites(Suite.scala:1220)
    at org.scalatest.Suite.runNestedSuites$(Suite.scala:1154)
    at org.scalatest.tools.DiscoverySuite.runNestedSuites(DiscoverySuite.scala:30)
    at org.scalatest.Suite.run(Suite.scala:1109)
    at org.scalatest.Suite.run$(Suite.scala:1094)
    at org.scalatest.tools.DiscoverySuite.run(DiscoverySuite.scala:30)
    at org.scalatest.tools.SuiteRunner.run(SuiteRunner.scala:45)
    at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13(Runner.scala:1322)
    at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13$adapted(Runner.scala:1316)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.tools.Runner$.doRunRunRunDaDoRunRun(Runner.scala:1316)
    at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24(Runner.scala:993)
    at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24$adapted(Runner.scala:971)
    at org.scalatest.tools.Runner$.withClassLoaderAndDispatchReporter(Runner.scala:1482)
    at org.scalatest.tools.Runner$.runOptionallyWithPassFailReporter(Runner.scala:971)
    at org.scalatest.tools.Runner$.main(Runner.scala:775)
    at org.scalatest.tools.Runner.main(Runner.scala)
    at io.bazel.rulesscala.scala_test.Runner.main(Runner.java:33)
  - should extract contract IDs from commands *** FAILED *** (4 milliseconds)
    scala.MatchError: Left(Package(MissingPackage(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946,Package(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946)))) (of class scala.util.Left)
    at com.digitalasset.daml.lf.engine.PreprocessorSpec.$anonfun$new$21(PreprocessorSpec.scala:373)
    at org.scalatest.OutcomeOf.outcomeOf(OutcomeOf.scala:85)
    at org.scalatest.OutcomeOf.outcomeOf$(OutcomeOf.scala:83)
    at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
    at org.scalatest.Transformer.apply(Transformer.scala:22)
    at org.scalatest.Transformer.apply(Transformer.scala:20)
    at org.scalatest.wordspec.AnyWordSpecLike$$anon$3.apply(AnyWordSpecLike.scala:1227)
    at org.scalatest.TestSuite.withFixture(TestSuite.scala:196)
    at org.scalatest.TestSuite.withFixture$(TestSuite.scala:195)
    at org.scalatest.wordspec.AnyWordSpec.withFixture(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.invokeWithFixture$1(AnyWordSpecLike.scala:1225)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTest$1(AnyWordSpecLike.scala:1237)
    at org.scalatest.SuperEngine.runTestImpl(Engine.scala:306)
    at org.scalatest.wordspec.AnyWordSpecLike.runTest(AnyWordSpecLike.scala:1237)
    at org.scalatest.wordspec.AnyWordSpecLike.runTest$(AnyWordSpecLike.scala:1219)
    at org.scalatest.wordspec.AnyWordSpec.runTest(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTests$1(AnyWordSpecLike.scala:1296)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:413)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:396)
    at org.scalatest.SuperEngine.runTestsImpl(Engine.scala:475)
    at org.scalatest.wordspec.AnyWordSpecLike.runTests(AnyWordSpecLike.scala:1296)
    at org.scalatest.wordspec.AnyWordSpecLike.runTests$(AnyWordSpecLike.scala:1295)
    at org.scalatest.wordspec.AnyWordSpec.runTests(AnyWordSpec.scala:1879)
    at org.scalatest.Suite.run(Suite.scala:1112)
    at org.scalatest.Suite.run$(Suite.scala:1094)
    at org.scalatest.wordspec.AnyWordSpec.org$scalatest$wordspec$AnyWordSpecLike$$super$run(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$run$1(AnyWordSpecLike.scala:1341)
    at org.scalatest.SuperEngine.runImpl(Engine.scala:535)
    at org.scalatest.wordspec.AnyWordSpecLike.run(AnyWordSpecLike.scala:1341)
    at org.scalatest.wordspec.AnyWordSpecLike.run$(AnyWordSpecLike.scala:1339)
    at org.scalatest.wordspec.AnyWordSpec.run(AnyWordSpec.scala:1879)
    at org.scalatest.Suite.callExecuteOnSuite$1(Suite.scala:1175)
    at org.scalatest.Suite.$anonfun$runNestedSuites$1(Suite.scala:1222)
    at scala.collection.ArrayOps$.foreach$extension(ArrayOps.scala:1324)
    at org.scalatest.Suite.runNestedSuites(Suite.scala:1220)
    at org.scalatest.Suite.runNestedSuites$(Suite.scala:1154)
    at org.scalatest.tools.DiscoverySuite.runNestedSuites(DiscoverySuite.scala:30)
    at org.scalatest.Suite.run(Suite.scala:1109)
    at org.scalatest.Suite.run$(Suite.scala:1094)
    at org.scalatest.tools.DiscoverySuite.run(DiscoverySuite.scala:30)
    at org.scalatest.tools.SuiteRunner.run(SuiteRunner.scala:45)
    at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13(Runner.scala:1322)
    at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13$adapted(Runner.scala:1316)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.tools.Runner$.doRunRunRunDaDoRunRun(Runner.scala:1316)
    at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24(Runner.scala:993)
    at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24$adapted(Runner.scala:971)
    at org.scalatest.tools.Runner$.withClassLoaderAndDispatchReporter(Runner.scala:1482)
    at org.scalatest.tools.Runner$.runOptionallyWithPassFailReporter(Runner.scala:971)
    at org.scalatest.tools.Runner$.main(Runner.scala:775)
    at org.scalatest.tools.Runner.main(Runner.scala)
    at io.bazel.rulesscala.scala_test.Runner.main(Runner.java:33)
  - should fail on contract IDs in keys *** FAILED *** (1 millisecond)
    scala.MatchError: Left(Package(MissingPackage(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946,Package(b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946)))) (of class scala.util.Left)
    at com.digitalasset.daml.lf.engine.PreprocessorSpec.$anonfun$new$23(PreprocessorSpec.scala:405)
    at org.scalatest.OutcomeOf.outcomeOf(OutcomeOf.scala:85)
    at org.scalatest.OutcomeOf.outcomeOf$(OutcomeOf.scala:83)
    at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
    at org.scalatest.Transformer.apply(Transformer.scala:22)
    at org.scalatest.Transformer.apply(Transformer.scala:20)
    at org.scalatest.wordspec.AnyWordSpecLike$$anon$3.apply(AnyWordSpecLike.scala:1227)
    at org.scalatest.TestSuite.withFixture(TestSuite.scala:196)
    at org.scalatest.TestSuite.withFixture$(TestSuite.scala:195)
    at org.scalatest.wordspec.AnyWordSpec.withFixture(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.invokeWithFixture$1(AnyWordSpecLike.scala:1225)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTest$1(AnyWordSpecLike.scala:1237)
    at org.scalatest.SuperEngine.runTestImpl(Engine.scala:306)
    at org.scalatest.wordspec.AnyWordSpecLike.runTest(AnyWordSpecLike.scala:1237)
    at org.scalatest.wordspec.AnyWordSpecLike.runTest$(AnyWordSpecLike.scala:1219)
    at org.scalatest.wordspec.AnyWordSpec.runTest(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$runTests$1(AnyWordSpecLike.scala:1296)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:413)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:390)
    at org.scalatest.SuperEngine.$anonfun$runTestsInBranch$1(Engine.scala:427)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    at org.scalatest.SuperEngine.runTestsInBranch(Engine.scala:396)
    at org.scalatest.SuperEngine.runTestsImpl(Engine.scala:475)
    at org.scalatest.wordspec.AnyWordSpecLike.runTests(AnyWordSpecLike.scala:1296)
    at org.scalatest.wordspec.AnyWordSpecLike.runTests$(AnyWordSpecLike.scala:1295)
    at org.scalatest.wordspec.AnyWordSpec.runTests(AnyWordSpec.scala:1879)
    at org.scalatest.Suite.run(Suite.scala:1112)
    at org.scalatest.Suite.run$(Suite.scala:1094)
    at org.scalatest.wordspec.AnyWordSpec.org$scalatest$wordspec$AnyWordSpecLike$$super$run(AnyWordSpec.scala:1879)
    at org.scalatest.wordspec.AnyWordSpecLike.$anonfun$run$1(AnyWordSpecLike.scala:1341)
    at org.scalatest.SuperEngine.runImpl(Engine.scala:535)
    at org.scalatest.wordspec.AnyWordSpecLike.run(AnyWordSpecLike.scala:1341)
    at org.scalatest.wordspec.AnyWordSpecLike.run$(AnyWordSpecLike.scala:1339)
    at org.scalatest.wordspec.AnyWordSpec.run(AnyWordSpec.scala:1879)
    at org.scalatest.Suite.callExecuteOnSuite$1(Suite.scala:1175)
    at org.scalatest.Suite.$anonfun$runNestedSuites$1(Suite.scala:1222)
    at scala.collection.ArrayOps$.foreach$extension(ArrayOps.scala:1324)
    at org.scalatest.Suite.runNestedSuites(Suite.scala:1220)
    at org.scalatest.Suite.runNestedSuites$(Suite.scala:1154)
    at org.scalatest.tools.DiscoverySuite.runNestedSuites(DiscoverySuite.scala:30)
    at org.scalatest.Suite.run(Suite.scala:1109)
    at org.scalatest.Suite.run$(Suite.scala:1094)
    at org.scalatest.tools.DiscoverySuite.run(DiscoverySuite.scala:30)
    at org.scalatest.tools.SuiteRunner.run(SuiteRunner.scala:45)
    at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13(Runner.scala:1322)
    at org.scalatest.tools.Runner$.$anonfun$doRunRunRunDaDoRunRun$13$adapted(Runner.scala:1316)
    at scala.collection.immutable.List.foreach(List.scala:334)
    at org.scalatest.tools.Runner$.doRunRunRunDaDoRunRun(Runner.scala:1316)
    at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24(Runner.scala:993)
    at org.scalatest.tools.Runner$.$anonfun$runOptionallyWithPassFailReporter$24$adapted(Runner.scala:971)
    at org.scalatest.tools.Runner$.withClassLoaderAndDispatchReporter(Runner.scala:1482)
    at org.scalatest.tools.Runner$.runOptionallyWithPassFailReporter(Runner.scala:971)
    at org.scalatest.tools.Runner$.main(Runner.scala:775)
    at org.scalatest.tools.Runner.main(Runner.scala)
 at io.bazel.rulesscala.scala_test.Runner.main(Runner.java:33)
 */
