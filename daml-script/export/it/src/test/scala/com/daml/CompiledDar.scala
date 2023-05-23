// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine.script.test

import com.daml.lf.data.Ref

import java.nio.file.Path

final case class CompiledDar(
    mainPkg: Ref.PackageId,
    compiledPackages: PureCompiledPackages,
) {
  def id(module: Ref.DottedName, name: Ref.DottedName): Ref.Identifier =
    Ref.Identifier(mainPkg, Ref.QualifiedName(module, name))

  def id(names: (String, String)): Ref.Identifier = {
    val (module, name) = names
    id(Ref.DottedName.assertFromString(module), Ref.DottedName.assertFromString(name))
  }
}

object CompiledDar {
  def read(
      path: Path,
      compilerConfig: speedy.Compiler.Config = speedy.Compiler.Config.Dev,
  ): CompiledDar = {
    val dar = archive.DarDecoder.assertReadArchiveFromFile(path.toFile)
    val pkgs = PureCompiledPackages.assertBuild(dar.all.toMap, compilerConfig)
    CompiledDar(dar.main._1, pkgs)
  }
}
