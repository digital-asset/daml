// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import java.io.{ByteArrayOutputStream, FileOutputStream}
import java.nio.file.Path
import java.util.jar.{Attributes, Manifest}
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.client.LedgerClient
import com.daml.lf.archive.{Dar, Decode}
import com.daml.lf.archive.Reader.damlLfCodedInputStreamFromBytes
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.google.protobuf.ByteString

import scala.annotation.tailrec
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

  def writeDar(
      sdkVersion: String,
      file: Path,
      dar: Dar[(PackageId, ByteString, Ast.Package)],
  ): Unit = {
    def encode(pkgId: PackageId, bs: ByteString) = {
      DamlLf.Archive
        .newBuilder()
        .setHash(pkgId)
        .setHashFunction(DamlLf.HashFunction.SHA256)
        .setPayload(bs)
        .build()
    }

    val out = new ZipOutputStream(new FileOutputStream(file.toFile))
    out.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"))
    out.write(manifest(sdkVersion, dar))
    out.closeEntry()
    dar.all.foreach { case (pkgId, bs, _) =>
      out.putNextEntry(new ZipEntry(pkgId + ".dalf"))
      out.write(encode(pkgId, bs).toByteArray)
      out.closeEntry
    }
    out.close
  }

  // Given the pkg id of a main dalf and the map of all downloaded packages produce
  // a DAR or return None for builtin packages like daml-stdlib
  // that don’t need to be listed in data-dependencies.
  def toDar(
      pkgId: PackageId,
      pkgs: Map[PackageId, (ByteString, Ast.Package)],
  ): Option[Dar[(PackageId, ByteString, Ast.Package)]] = {
    def deps(pkgId: PackageId): Set[PackageId] = {
      @tailrec
      def go(todo: List[PackageId], acc: Set[PackageId]): Set[PackageId] =
        todo match {
          case Nil => acc
          case p :: todo if acc.contains(p) => go(todo, acc)
          case p :: todo =>
            go(todo ++ pkgs(p)._2.directDeps.toList, acc.union(pkgs(p)._2.directDeps))
        }
      go(List(pkgId), Set.empty) - pkgId
    }
    for {
      pkg <- pkgs.get(pkgId) if pkg._2.metadata.isDefined
      if !Seq("daml-stdlib", "daml-prim", "daml-script")
        .map(Ref.PackageName.assertFromString(_))
        .contains(pkg._2.metadata.get.name)
    } yield {
      Dar(
        (pkgId, pkg._1, pkg._2),
        deps(pkgId).toList.map(pkgId => (pkgId, pkgs(pkgId)._1, pkgs(pkgId)._2)),
      )
    }
  }

  private def manifest[A, B](sdkVersion: String, dar: Dar[(PackageId, A, B)]): Array[Byte] = {
    val manifest = new Manifest()
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0")
    manifest.getMainAttributes().put(new Attributes.Name("Format"), "daml-lf")
    manifest
      .getMainAttributes()
      .put(new Attributes.Name("Dalfs"), dar.all.map(pkg => pkg._1 + ".dalf").mkString(", "))
    manifest.getMainAttributes().put(new Attributes.Name("Main-Dalf"), dar.main._1 + ".dalf")
    manifest.getMainAttributes().put(new Attributes.Name("Sdk-Version"), sdkVersion)
    val bytes = new ByteArrayOutputStream()
    manifest.write(bytes)
    bytes.close
    bytes.toByteArray
  }

}
