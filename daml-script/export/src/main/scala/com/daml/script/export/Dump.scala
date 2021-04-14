// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import com.daml.ledger.api.refinements.ApiTypes.ContractId
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.google.protobuf.ByteString

object Export {

  def writeExport(
      sdkVersion: String,
      damlScriptLib: String,
      targetDir: Path,
      acs: Map[ContractId, CreatedEvent],
      trees: Seq[TransactionTree],
      pkgRefs: Set[PackageId],
      pkgs: Map[PackageId, (ByteString, Ast.Package)],
      acsBatchSize: Int,
  ) = {
    val dir = Files.createDirectories(targetDir)
    Files.write(
      dir.resolve("Export.daml"),
      Encode
        .encodeTransactionTreeStream(acs, trees, acsBatchSize)
        .render(80)
        .getBytes(StandardCharsets.UTF_8),
    )
    val exposedPackages: Seq[String] =
      pkgRefs.view.collect(Function.unlift(Dependencies.toPackages(_, pkgs))).toSeq
    val deps = Files.createDirectory(dir.resolve("deps"))
    val dalfFiles = pkgs.map { case (pkgId, (bs, pkg)) =>
      val prefix = pkg.metadata.map(md => s"${md.name}-${md.version}-").getOrElse("")
      val file = deps.resolve(s"$prefix$pkgId.dalf")
      Dependencies.writeDalf(file, pkgId, bs)
      file
    }.toSeq
    val lfTarget = Dependencies.targetLfVersion(pkgs.values.map(_._2.languageVersion))
    val targetFlag = lfTarget.fold("")(Dependencies.targetFlag(_))

    val buildOptions = targetFlag +: exposedPackages.map(pkgId => s"--package=$pkgId")

    Files.write(
      dir.resolve("daml.yaml"),
      s"""sdk-version: $sdkVersion
         |name: export
         |version: 1.0.0
         |source: .
         |dependencies: [daml-stdlib, daml-prim, $damlScriptLib]
         |data-dependencies: [${dalfFiles.mkString(",")}]
         |build-options: [${buildOptions.mkString(",")}]
         |""".stripMargin.getBytes(StandardCharsets.UTF_8),
    )
  }
}
