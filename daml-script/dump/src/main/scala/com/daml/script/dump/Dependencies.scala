// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import java.nio.file.Path

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.client.LedgerClient
import com.daml.lf.archive.{Dar, DarWriter, Decode}
import com.daml.lf.archive.Reader.damlLfCodedInputStreamFromBytes
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.script.dump.TreeUtils.{treeRefs, valueRefs}
import com.google.protobuf.ByteString
import scalaz.std.iterable._
import scalaz.std.set._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scalaz.syntax.traverse._

object Dependencies {

  def contractsReferences(contracts: Iterable[CreatedEvent]): Set[PackageId] = {
    contracts
      .foldMap(ev => valueRefs(Sum.Record(ev.getCreateArguments)))
      .map(i => PackageId.assertFromString(i.packageId))
  }

  def treesReferences(transactions: Iterable[TransactionTree]): Set[PackageId] = {
    transactions
      .foldMap(treeRefs(_))
      .map(i => PackageId.assertFromString(i.packageId))
  }

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

  def targetLfVersion(dependencies: Seq[Dar[LanguageVersion]]): Option[LanguageVersion] = {
    val dalfs = dependencies.flatMap(_.all)
    if (dalfs.isEmpty) { None }
    else { Some(dalfs.max) }
  }

  def targetFlag(v: LanguageVersion): String =
    s"--target=${v.pretty}"

  def writeDar(
      sdkVersion: String,
      file: Path,
      dar: Dar[(PackageId, ByteString, Ast.Package)],
  ): Unit = {
    DarWriter.encode(
      sdkVersion,
      dar.map { case (pkgId, dalf, _) => (pkgId + ".dalf", encodeDalf(pkgId, dalf).toByteArray) },
      file,
    )
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
      pkg <- pkgs.get(pkgId) if !pkg._2.metadata.exists(m => providedLibraries.contains(m.name))
    } yield {
      Dar(
        (pkgId, pkg._1, pkg._2),
        deps(pkgId).toList.map(pkgId => (pkgId, pkgs(pkgId)._1, pkgs(pkgId)._2)),
      )
    }
  }
}
