// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.io.FileOutputStream
import java.nio.file.Path
import com.daml.daml_lf_dev.DamlLf
import com.digitalasset.canton.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.value
import com.digitalasset.canton.ledger.client.LedgerClient
import com.daml.lf.archive
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{Ast, LanguageVersion, StablePackages}
import com.google.protobuf.ByteString

import scala.collection.mutable
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
          client.v2.packageService.getPackage(p).flatMap { pkgResp =>
            val pkgId = PackageId.assertFromString(pkgResp.hash)
            val pkg =
              archive.archivePayloadDecoder(pkgId).assertFromByteString(pkgResp.archivePayload)._2
            go(todo ++ pkg.directDeps, acc + (pkgId -> ((pkgResp.archivePayload, pkg))))
          }
      }
    go(references, Map.empty)
  }

  /** Whether the given LF version is missing type-class instances.
    *
    * If so then we need to generate replacement instances for standard template and choice instances.
    */
  def lfMissingInstances(version: LanguageVersion): Boolean =
    LanguageVersion.Ordering.lt(version, LanguageVersion.v1_8)

  final case class ChoiceInstanceSpec(
      arg: Ast.Type,
      ret: Ast.Type,
  )
  final case class TemplateInstanceSpec(
      key: Option[Ast.Type],
      choices: Map[ApiTypes.Choice, ChoiceInstanceSpec],
  )

  /** Extract all templates that are missing instances due to their package's LF version.
    *
    * The exporter will need to create replacement type class instances
    * for the standard template and choice instances for these templates.
    */
  def templatesMissingInstances(
      pkgs: Map[PackageId, (ByteString, Ast.Package)]
  ): Map[ApiTypes.TemplateId, TemplateInstanceSpec] = {
    val map = mutable.HashMap.empty[ApiTypes.TemplateId, TemplateInstanceSpec]
    pkgs.foreach {
      case (pkgId, (_, pkg)) if lfMissingInstances(pkg.languageVersion) =>
        pkg.modules.foreach { case (modName, mod) =>
          mod.templates.foreach { case (tplName, tpl) =>
            val tplId = ApiTypes.TemplateId(
              value
                .Identifier()
                .withPackageId(pkgId)
                .withModuleName(modName.dottedName)
                .withEntityName(tplName.dottedName)
            )
            map += tplId -> TemplateInstanceSpec(
              key = tpl.key.map(_.typ),
              choices = tpl.choices.map { case (name, choice) =>
                ApiTypes.Choice(name: String) -> ChoiceInstanceSpec(
                  choice.argBinder._2,
                  choice.returnType,
                )
              },
            )
          }
        }
      case _ => ()
    }
    map.toMap
  }

  /** The Daml-LF version to target based on the DALF dependencies.
    *
    * Chooses the latest LF version among the DALFs but at least 1.14 as that is the minimum supported by damlc.
    * Returns None if no DALFs are given.
    */
  def targetLfVersion(dalfs: Iterable[LanguageVersion]): Option[LanguageVersion] = {
    if (dalfs.isEmpty) { None }
    else { Some((List(LanguageVersion.v1_14) ++ dalfs).max) }
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

  private def isProvidedLibrary(pkgId: PackageId, pkg: Ast.Package): Boolean = {
    val stablePackages =
      StablePackages.ids(LanguageVersion.AllVersions(LanguageVersion.default.major))
    providedLibraries.contains(pkg.metadata.name) || stablePackages.contains(pkgId)
  }

  // Return the package-id appropriate for the --package flag if the package is not builtin.
  def toPackages(
      mainId: PackageId,
      pkgs: Map[PackageId, (ByteString, Ast.Package)],
  ): Option[String] = {
    for {
      main <- pkgs.get(mainId) if !isProvidedLibrary(mainId, main._2)
      md = main._2.metadata
      pkg = s"${md.name}-${md.version}"
    } yield pkg
  }
}
