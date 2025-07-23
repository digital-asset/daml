// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.testing.parser
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EngineValidatePackagesTestV2 extends EngineValidatePackagesTest(LanguageMajorVersion.V2)

class EngineValidatePackagesTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with Inside {

  val langVersion = LanguageVersion.defaultOrLatestStable(majorLanguageVersion)

  val pkgId = Ref.PackageId.assertFromString("-pkg-")
  val extraPkgId = Ref.PackageId.assertFromString("-extra-")
  val missingPkgId = Ref.PackageId.assertFromString("-missing-")
  val utilityPkgId = Ref.PackageId.assertFromString("-utility-")
  val altUtilityPkgId = Ref.PackageId.assertFromString("-alt-utility-")
  val (stablePkgId, stablePkg) = StablePackages(majorLanguageVersion).packagesMap.head

  implicit val parserParameters: parser.ParserParameters[this.type] =
    parser.ParserParameters(pkgId, langVersion)

  val fakeDamlPrimPkg =
    p"""
      metadata ( 'daml-prim' : '1.0.0' )
      module Mod {
        val string: Text = "fake-daml-prim";
      }
    """
  val fakeDamlStdlibPkg =
    p"""
      metadata ( 'daml-stdlib' : '1.0.0' )
      module Mod {
        val string: Text = "fake-daml-stdlib";
      }
    """
  val utilityPkgChoices = Seq(
    ("no utility packages", Set.empty, Seq.empty),
    ("daml-prim utility package", Set(utilityPkgId), Seq(utilityPkgId -> fakeDamlPrimPkg)),
    (
      "daml-stdlib utility package",
      Set(altUtilityPkgId),
      Seq(altUtilityPkgId -> fakeDamlStdlibPkg),
    ),
    (
      "daml-prim and daml-stdlib utility package",
      Set(utilityPkgId, altUtilityPkgId),
      Seq(utilityPkgId -> fakeDamlPrimPkg, altUtilityPkgId -> fakeDamlStdlibPkg),
    ),
  )
  val stablePkgChoices = Seq(
    ("no stable packages", Set.empty, Seq.empty),
    ("stable package", Set(stablePkgId), Seq(stablePkgId -> stablePkg)),
  )

  private def newEngine = new Engine(
    EngineConfig(LanguageVersion.AllVersions(majorLanguageVersion))
  )

  private def darFromPackageMap(
      mainPkg: (Ref.PackageId, Package),
      dependentPkgs: (Ref.PackageId, Package)*
  ): Dar[(Ref.PackageId, Package)] =
    Dar(mainPkg, dependentPkgs.toList)

  "Engine.validateDar" should {
    val pkg =
      p"""
        metadata ( 'pkg' : '1.0.0' )
        module Mod {
          val string: Text = "pkg";
        }
      """

    "accept valid package" should {
      utilityPkgChoices.foreach { case (utilityLabel, utilityDirectDeps, utilityDependencies) =>
        stablePkgChoices.foreach { case (stableLabel, stableDirectDeps, stableDependencies) =>
          s"with $utilityLabel and $stableLabel" in {
            newEngine.validateDar(
              darFromPackageMap(
                pkgId -> pkg.copy(directDeps = utilityDirectDeps ++ stableDirectDeps),
                utilityDependencies ++ stableDependencies: _*
              )
            ) shouldBe Right(())
          }
        }
      }
    }

    "reject ill-typed packages" should {
      val illTypedPackage =
        p"""
        metadata ( 'pkg' : '1.0.0' )
        module Mod {
          val string: Text = 1;
        }
      """

      utilityPkgChoices.foreach { case (utilityLabel, utilityDirectDeps, utilityDependencies) =>
        stablePkgChoices.foreach { case (stableLabel, stableDirectDeps, stableDependencies) =>
          s"with $utilityLabel and $stableLabel" in {
            inside(
              newEngine.validateDar(
                darFromPackageMap(
                  pkgId -> illTypedPackage.copy(directDeps = utilityDirectDeps ++ stableDirectDeps),
                  utilityDependencies ++ stableDependencies: _*
                )
              )
            ) { case Left(_: Error.Package.Validation) =>
            }
          }
        }
      }
    }

    "reject non self-consistent sets of packages" should {
      val extraPkg =
        p"""
           metadata ( 'extra' : '1.0.0' )
           module Mod {
             val string: Text = "e";
           }
         """

      "with missing dependencies only" should {
        val dependentPackage =
          p"""
              metadata ( 'pkg' : '1.0.0' )
              module Mod {
                val string: Text = '-missing-':Mod:Text;
              }
            """

        utilityPkgChoices.foreach { case (utilityLabel, utilityDirectDeps, utilityDependencies) =>
          stablePkgChoices.foreach { case (stableLabel, stableDirectDeps, stableDependencies) =>
            s"with $utilityLabel and $stableLabel" in {
              inside(
                newEngine.validateDar(
                  darFromPackageMap(
                    pkgId -> dependentPackage.copy(directDeps =
                      Set(missingPkgId) ++ utilityDirectDeps ++ stableDirectDeps
                    ),
                    utilityDependencies ++ stableDependencies: _*
                  )
                )
              ) {
                case Left(
                      err @ Error.Package.DarSelfConsistency(
                        mainPkgId,
                        transitiveDeps,
                        missingDeps,
                        extraDeps,
                      )
                    ) =>
                  mainPkgId shouldBe pkgId
                  transitiveDeps shouldBe utilityDirectDeps ++ stableDirectDeps
                  missingDeps shouldBe Set(missingPkgId)
                  extraDeps shouldBe Set.empty
                  err.logReportingEnabled shouldBe false
              }
            }
          }
        }
      }

      "with extra dependencies only" should {
        val dependentPackage =
          p"""
              metadata ( 'pkg' : '1.0.0' )
              module Mod {
                val string: Text = "t";
              }
            """

        utilityPkgChoices.foreach { case (utilityLabel, utilityDirectDeps, utilityDependencies) =>
          stablePkgChoices.foreach { case (stableLabel, stableDirectDeps, stableDependencies) =>
            s"with $utilityLabel and $stableLabel" in {
              inside(
                newEngine.validateDar(
                  darFromPackageMap(
                    pkgId -> dependentPackage.copy(directDeps =
                      utilityDirectDeps ++ stableDirectDeps
                    ),
                    Seq(extraPkgId -> extraPkg) ++ utilityDependencies ++ stableDependencies: _*
                  )
                )
              ) {
                case Left(
                      err @ Error.Package.DarSelfConsistency(
                        mainPkgId,
                        transitiveDeps,
                        missingDeps,
                        extraDeps,
                      )
                    ) =>
                  mainPkgId shouldBe pkgId
                  transitiveDeps shouldBe utilityDirectDeps ++ stableDirectDeps
                  missingDeps shouldBe Set.empty
                  extraDeps shouldBe Set(extraPkgId)
                  err.logReportingEnabled shouldBe true
              }
            }
          }
        }
      }

      "with both missing dependencies and extra dependencies" should {
        val dependentPackage =
          p"""
              metadata ( 'pkg' : '1.0.0' )
              module Mod {
                val string: Text = '-missing-':Mod:Text;
              }
            """

        utilityPkgChoices.foreach { case (utilityLabel, utilityDirectDeps, utilityDependencies) =>
          stablePkgChoices.foreach { case (stableLabel, stableDirectDeps, stableDependencies) =>
            s"with $utilityLabel and $stableLabel" in {
              inside(
                newEngine.validateDar(
                  darFromPackageMap(
                    pkgId -> dependentPackage.copy(directDeps =
                      Set(missingPkgId) ++ utilityDirectDeps ++ stableDirectDeps
                    ),
                    Seq(extraPkgId -> extraPkg) ++ utilityDependencies ++ stableDependencies: _*
                  )
                )
              ) {
                case Left(
                      err @ Error.Package.DarSelfConsistency(
                        mainPkgId,
                        transitiveDeps,
                        missingDeps,
                        extraDeps,
                      )
                    ) =>
                  mainPkgId shouldBe pkgId
                  transitiveDeps shouldBe utilityDirectDeps ++ stableDirectDeps
                  missingDeps shouldBe Set(missingPkgId)
                  extraDeps shouldBe Set(extraPkgId)
                  err.logReportingEnabled shouldBe false
              }
            }
          }
        }
      }
    }
  }
}
