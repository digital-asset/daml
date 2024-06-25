// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine.script.test

import com.digitalasset.daml.lf.data.Ref

import java.nio.file.Path

final case class CompiledDar(
    mainPkg: Ref.PackageId,
    compiledPackages: PureCompiledPackages,
)

object CompiledDar {
  def read(
      path: Path,
      compilerConfig: speedy.Compiler.Config,
  ): CompiledDar = {
    val dar = archive.DarDecoder.assertReadArchiveFromFile(path.toFile)
    val pkgs = PureCompiledPackages.assertBuild(dar.all.toMap, compilerConfig)
    CompiledDar(dar.main._1, pkgs)
  }
}
