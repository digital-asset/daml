// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package validation

import com.daml.lf.data.Ref.{ModuleName, PackageId}
import com.daml.lf.archive._
import com.daml.lf.language.Ast._
import com.daml.lf.language.PackageInterface
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.test.ModelTestDar
import com.daml.lf.archive.DarDecoder
import java.io.File
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class TypecheckingBench {

  @Param(Array(""))
  var darPath: String = _

  @Param(Array(""))
  var moduleName: String = _

  private var dar: Dar[(PackageId, Package)] = _
  private var darMap: Map[PackageId, Package] = _
  private var pkgInterface: PackageInterface = _
  private var module: Option[(PackageId, Module)] = None

  @Setup(Level.Trial)
  def setup(): Unit = {
    val darFile = new File(
      if (darPath.isEmpty)
        rlocation(ModelTestDar.path)
      else darPath
    )
    dar = DarDecoder.assertReadArchiveFromFile(darFile)
    darMap = dar.all.toMap
    pkgInterface = PackageInterface(darMap)

    if (!moduleName.isEmpty) {
      dar.all.foreach { case (pkgId, pkg) =>
        pkg.modules.foreach { case (name, m) =>
          if (name == ModuleName.assertFromString(moduleName)) {
            module = Some((pkgId, m))
          }
        }
      }
      if (module.isEmpty) {
        sys.error(s"Module name $moduleName could not be found in DAR")
      }
    }
  }

  @Benchmark
  def bench(): Unit = {
    val r = module match {
      case Some((pkgId, m)) =>
        Validation.checkModule(pkgInterface, pkgId, m)
      case None =>
        Validation.checkPackages(pkgInterface, darMap)
    }
    assert(r.isRight)
  }
}
