// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import java.util.concurrent.atomic.AtomicInteger

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.ConcurrentCompiledPackages
import com.daml.lf.language.Ast.Package
import com.daml.lf.language.{Ast, LanguageVersion, Util => AstUtil}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

final class CompiledPackageSpec extends WordSpec with Matchers with TableDrivenPropertyChecks {

  val Seq(v1_6, v1_7, v1_8) =
    Seq("6", "7", "8").map(minor =>
      LanguageVersion(LanguageVersion.Major.V1, LanguageVersion.Minor.Stable(minor)))

  private[this] final class ModBuilder(languageVersion: LanguageVersion) {

    import ModBuilder._

    val name = Ref.ModuleName.assertFromString(s"module${counter.incrementAndGet()}")

    private[this] var _deps = Set.empty[PackageId]
    private[this] var _body: Ast.Expr = AstUtil.EUnit
    private[this] var _built = false

    def addDeps(deps: (Ref.PackageId, Ref.ModuleName)): this.type = {
      assert(!_built)
      val (pkgId, modName) = deps
      val id = Ref.Identifier(pkgId, Ref.QualifiedName(modName, noOpName))
      _deps = _deps + id.packageId
      _body = Ast.ELet(Ast.Binding(None, AstUtil.TUnit, Ast.EVal(id)), _body)
      this
    }

    def build(): (Ast.Module, Set[PackageId]) = {
      _built = true
      Ast.Module(
        name,
        Map(noOpName -> Ast.DValue(AstUtil.TUnit, false, _body, false)),
        languageVersion,
        Ast.FeatureFlags.default
      ) -> _deps
    }
  }

  private[this] object ModBuilder {
    private val counter = new AtomicInteger()

    private def noOpName = Ref.DottedName.assertFromString("noOp")

    def apply(
        languageVersion: LanguageVersion,
        deps: (Ref.PackageId, Ref.ModuleName)*,
    ): ModBuilder =
      deps.foldLeft(new ModBuilder(languageVersion))(_.addDeps(_))
  }

  private[this] final class PkgBuilder {

    import PkgBuilder._

    val id = Ref.PackageId.assertFromString(s"package${counter.incrementAndGet()}")

    private[this] var _modules = List.empty[Ast.Module]
    private[this] var _deps = Set.empty[PackageId]
    private[this] var _built = false

    def addMod(modBuilder: ModBuilder): this.type = {
      assert(!_built)
      val (mod, deps) = modBuilder.build()
      _modules = mod :: _modules
      _deps = _deps | deps
      this
    }

    def build: Package = {
      _built = true
      Package(_modules, _deps, None)
    }

  }

  private[this] object PkgBuilder {
    private val counter = new AtomicInteger()

    def apply(modBuilders: ModBuilder*): PkgBuilder =
      modBuilders.foldLeft(new PkgBuilder())(_.addMod(_))
  }

  tests("PureCompiledPackages")(PureCompiledPackages(_).toOption.get)

  tests("ConcurrentCompiledPackages") { packages =>
    val compiledPackages = ConcurrentCompiledPackages()
    packages.foreach {
      case (pkgId, pkg) =>
        compiledPackages
          .addPackage(pkgId, pkg)
          .consume(
            _ => sys.error("unexpected error"),
            packages.get,
            _ => sys.error("unexpected error"),
          )
          .toOption
          .get
    }
    compiledPackages
  }

  def tests(name: String)(_compile: Map[Ref.PackageId, Ast.Package] => CompiledPackages) =
    s"$name#getMaxLanguageVersione" should {

      def compile(packages: PkgBuilder*) =
        _compile(packages.map(b => b.id -> b.build).toMap)

      "return None for empty Package" in {
        val pkg1 = PkgBuilder()
        val compiledPackages = compile(pkg1)

        compiledPackages.packageLanguageVersion.lift(pkg1.id) shouldBe None
      }

      "return None for non defined Package" in {
        val compiledPackages = compile()
        compiledPackages.packageLanguageVersion.lift(PkgBuilder().id) shouldBe None
      }

      "return the newest language version of package with unique modules" in {
        val pkg = PkgBuilder(ModBuilder(v1_6))
        val compiledPackages = compile(pkg)
        compiledPackages.packageLanguageVersion.lift(pkg.id) shouldBe Some(v1_6)
      }

      "return the newest language version of package with several modules" in {
        val Seq(mod6, mod7, mod8) = Seq(v1_6, v1_7, v1_8).map(ModBuilder(_))

        Seq(mod6, mod7).permutations.foreach { mods =>
          val pkg = PkgBuilder(mods: _*)
          compile(pkg).packageLanguageVersion.lift(pkg.id) shouldBe Some(v1_7)
        }
        Seq(mod6, mod7, mod8).permutations.foreach { mods =>
          val pkg = PkgBuilder(mods: _*)
          compile(pkg).packageLanguageVersion.lift(pkg.id) shouldBe Some(v1_8)
        }
      }

      "return the newest language version of package with several package" in {
        val Seq(mod6, mod7, mod8) = Seq(v1_6, v1_7, v1_8).map(ModBuilder(_))
        val Seq(pkg6, pkg7, pkg8) = Seq(mod6, mod7, mod8).map(PkgBuilder(_))
        val Seq(dep6, dep7, dep8) =
          Seq(pkg6.id -> mod6.name, pkg7.id -> mod7.name, pkg8.id -> mod8.name)

        val testCases = Table(
          ("pkgVersion", "dependencies", "output"),
          (v1_6, Seq(dep6), v1_6),
          (v1_6, Seq(dep7), v1_7),
          (v1_6, Seq(dep8), v1_8),
          (v1_6, Seq(dep6, dep7), v1_7),
          (v1_6, Seq(dep7, dep8), v1_8),
          (v1_6, Seq(dep6, dep7, dep8), v1_8),
          (v1_7, Seq(dep6), v1_7),
          (v1_7, Seq(dep8), v1_8),
          (v1_7, Seq(dep7, dep8), v1_8),
          (v1_7, Seq(dep6, dep7, dep8), v1_8),
          (v1_8, Seq(dep6), v1_8),
          (v1_8, Seq(dep7), v1_8),
          (v1_8, Seq(dep6, dep7), v1_8),
        )

        testCases.forEvery {
          case (pkgVersion, dependencies, output) =>
            val pkg = PkgBuilder(ModBuilder(pkgVersion, dependencies: _*))
            compile(pkg, pkg6, pkg7, pkg8).packageLanguageVersion.lift(pkg.id) shouldBe Some(output)
        }
      }
    }

}
