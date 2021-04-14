// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.io.FileOutputStream
import java.nio.file.Path

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.client.LedgerClient
import com.daml.lf.archive.Decode
import com.daml.lf.archive.Reader.damlLfCodedInputStreamFromBytes
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{Ast, LanguageVersion}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

object Dependencies {

  // Given a list of root package ids, download all packages transitively referenced by those roots.
  def fetchPackages(client: LedgerClient, references: List[PackageId])(implicit
      ec: ExecutionContext
  ): Future[Map[PackageId, (ByteString, Ast.Package)]] = {
    def go(
        todo: List[PackageId],
        acc: Map[PackageId, (ByteString, Ast.Package)],
    ): Future[Map[PackageId, (ByteString, Ast.Package)]] =
      todo match {
        case Nil => Future.successful(acc)
        case p :: todo if acc.contains(p) => go(todo, acc)
        case p :: todo =>
          client.packageClient.getPackage(p).flatMap { pkgResp =>
            val cos = damlLfCodedInputStreamFromBytes(
              pkgResp.archivePayload.toByteArray,
              Decode.PROTOBUF_RECURSION_LIMIT,
            )
            val pkgId = PackageId.assertFromString(pkgResp.hash)
            val pkg = Decode
              .readArchivePayloadAndVersion(pkgId, DamlLf.ArchivePayload.parser().parseFrom(cos))
              ._1
              ._2
            go(todo ++ pkg.directDeps, acc + (pkgId -> ((pkgResp.archivePayload, pkg))))
          }
      }
    go(references, Map.empty)
  }

  def targetLfVersion(dalfs: Iterable[LanguageVersion]): Option[LanguageVersion] = {
    if (dalfs.isEmpty) { None }
    else { Some(dalfs.max) }
  }

  def targetFlag(v: LanguageVersion): String =
    s"--target=${v.pretty}"

  def writeDalf(
      file: Path,
      pkgId: PackageId,
      bs: ByteString,
  ): Unit = {
    val os = new FileOutputStream(file.toFile)
    try {
      encodeDalf(pkgId, bs).writeTo(os)
    } finally {
      os.close()
    }
  }

  private def encodeDalf(pkgId: PackageId, bs: ByteString) =
    DamlLf.Archive
      .newBuilder()
      .setHash(pkgId)
      .setHashFunction(DamlLf.HashFunction.SHA256)
      .setPayload(bs)
      .build

  private val providedLibraries: Set[Ref.PackageName] =
    Set("daml-stdlib", "daml-prim", "daml-script").map(Ref.PackageName.assertFromString(_))

  private def isProvidedLibrary(pkg: Ast.Package): Boolean =
    pkg.metadata.exists(m => providedLibraries.contains(m.name))

  // Return the package-id appropriate for the --package flag if the package is not builtin.
  def toPackages(
      mainId: PackageId,
      pkgs: Map[PackageId, (ByteString, Ast.Package)],
  ): Option[String] = {
    for {
      main <- pkgs.get(mainId) if !isProvidedLibrary(main._2)
      md <- main._2.metadata
    } yield s"${md.name}-${md.version}"
  }
}
