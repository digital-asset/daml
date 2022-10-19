// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party, TemplateId}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.daml.script.export.Dependencies.TemplateInstanceSpec
import com.daml.script.export.TreeUtils.{
  Action,
  CreatedContract,
  CreatedContractWithPath,
  SetTime,
  Submit,
  cmdReferencedCids,
  partiesInContracts,
  partiesInTree,
  topoSortAcs,
  treeCreatedCids,
  treeRefs,
  valueRefs,
}
import com.google.protobuf.ByteString
import scalaz.std.iterable._
import scalaz.std.set._
import scalaz.syntax.foldable._

import scala.collection.mutable

case class Export(
    partyMap: Map[Party, String],
    cidMap: Map[ContractId, String],
    unknownCids: Set[ContractId],
    cidRefs: Set[ContractId],
    missingInstances: Map[TemplateId, TemplateInstanceSpec],
    moduleRefs: Set[String],
    actions: Seq[Action],
)

object Export {

  def fromTransactionTrees(
      acs: Map[ContractId, CreatedEvent],
      trees: Seq[TransactionTree],
      missingInstances: Map[TemplateId, TemplateInstanceSpec],
      acsBatchSize: Int,
      setTime: Boolean,
  ): Export = {
    val sortedAcs = topoSortAcs(acs)
    val actions = Action.fromACS(sortedAcs, acsBatchSize) ++ Action.fromTrees(trees, setTime)
    val submits: Seq[Submit] = actions.collect { case submit: Submit => submit }
    val cidRefs = submits.foldMap(_.commands.foldMap(cmdReferencedCids))

    val acsCids =
      sortedAcs.map(ev =>
        CreatedContractWithPath(ContractId(ev.contractId), TemplateId(ev.getTemplateId), Nil)
      )
    val treeCids = trees.map(treeCreatedCids(_))
    val cidMap = cidMapping(acsCids +: treeCids, cidRefs)

    val refs =
      acs.values.foldMap(ev => valueRefs(Sum.Record(ev.getCreateArguments))) ++ trees.foldMap(
        treeRefs(_)
      )
    val usesSetTime = actions.any(_.isInstanceOf[SetTime])
    val timeRefs: Set[String] = if (usesSetTime) { Set("DA.Date", "DA.Time") }
    else { Set.empty }
    val partiesModuleRefs = Set[String]("DA.Traversable")
    val unknownContractModuleRefs = Set[String]("DA.Stack", "DA.TextMap")

    Export(
      partyMap = partyMapping(partiesInContracts(acs.values) ++ trees.foldMap(partiesInTree(_))),
      cidMap = cidMap,
      unknownCids = cidRefs -- cidMap.keySet,
      cidRefs = cidRefs,
      missingInstances = missingInstances,
      moduleRefs =
        refs.map(_.moduleName).toSet ++ timeRefs ++ partiesModuleRefs ++ unknownContractModuleRefs,
      actions = actions,
    )
  }

  private def partyMapping(parties: Set[Party]): Map[Party, String] = {
    // - PartyIdStrings are strings that match the regexp ``[A-Za-z0-9:\-_ ]+``.
    def safeParty(p: String) =
      Seq(":", "-", "_", " ").foldLeft(p) { case (p, x) => p.replace(x, "") }.toLowerCase
    // Map from original party id to Daml identifier
    var partyMap: Map[Party, String] = Map.empty
    // Number of times we’ve gotten the same result from safeParty, we resolve collisions with a suffix.
    var usedParties: Map[String, Int] = Map.empty
    parties.foreach { p =>
      val r = safeParty(Party.unwrap(p))
      usedParties.get(r) match {
        case None =>
          partyMap += p -> s"${r}_0"
          usedParties += r -> 0
        case Some(value) =>
          partyMap += p -> s"${r}_${value + 1}"
          usedParties += r -> (value + 1)
      }
    }
    partyMap
  }

  private def cidMapping(
      cids: Seq[Seq[CreatedContract]],
      cidRefs: Set[ContractId],
  ): Map[ContractId, String] = {
    def lowerFirst(s: String) =
      if (s.isEmpty) {
        s
      } else {
        s.head.toLower.toString + s.tail
      }
    cids.view.zipWithIndex.flatMap { case (cs, treeIndex) =>
      cs.view.zipWithIndex.collect {
        case (c, i) if cidRefs.contains(c.cid) =>
          c.cid -> s"${lowerFirst(TemplateId.unwrap(c.tplId).entityName)}_${treeIndex}_$i"
      }
    }.toMap
  }

  def writeExport(
      sdkVersion: String,
      damlScriptLib: String,
      targetDir: Path,
      acs: Map[ContractId, CreatedEvent],
      trees: Seq[TransactionTree],
      pkgRefs: Set[PackageId],
      pkgs: Map[PackageId, (ByteString, Ast.Package)],
      acsBatchSize: Int,
      setTime: Boolean,
  ) = {
    val missingInstances = Dependencies.templatesMissingInstances(pkgs)
    val scriptExport =
      Export.fromTransactionTrees(acs, trees, missingInstances, acsBatchSize, setTime)

    val dir = Files.createDirectories(targetDir)
    Files.write(
      dir.resolve("Export.daml"),
      Encode
        .encodeExport(scriptExport)
        .render(80)
        .getBytes(StandardCharsets.UTF_8),
    )
    Files.write(
      dir.resolve("args.json"),
      Encode
        .encodeArgs(scriptExport)
        .prettyPrint
        .getBytes(StandardCharsets.UTF_8),
    )
    val exposedPackages: Seq[String] =
      pkgRefs.view.collect(Function.unlift(Dependencies.toPackages(_, pkgs))).toSeq
    val deps = Files.createDirectory(dir.resolve("deps"))
    val dalfFiles = pkgs.toSeq
      .sortBy { case (pkgId, (_, pkg)) =>
        (pkg.metadata.map(md => (md.name.toString, md.version.toString)), pkgId.toString)
      }
      .map { case (pkgId, (bs, pkg)) =>
        val prefix = pkg.metadata.map(md => s"${md.name}-${md.version}-").getOrElse("")
        val file = deps.resolve(s"$prefix$pkgId.dalf")
        Dependencies.writeDalf(file, pkgId, bs)
        dir.relativize(file)
      }
    val lfTarget = Dependencies.targetLfVersion(pkgs.values.map(_._2.languageVersion))
    val targetFlag = lfTarget.fold("")(Dependencies.targetFlag(_))

    val buildOptions = targetFlag +: exposedPackages.map(pkgId => s"--package=$pkgId")
    val damlYaml = {
      import io.circe.Json
      import io.circe.yaml.Printer
      val json = Json.fromFields(
        mutable.LinkedHashMap(
          "sdk-version" -> Json.fromString(sdkVersion),
          "name" -> Json.fromString("export"),
          "version" -> Json.fromString("1.0.0"),
          "source" -> Json.fromString("."),
          "init-script" -> Json.fromString("Export:export"),
          "script-options" -> Json.fromValues(
            List(
              Json.fromString("--input-file"),
              Json.fromString("args.json"),
            )
          ),
          "parties" -> Json.fromValues(
            scriptExport.partyMap.keys.map(p => Json.fromString(Party.unwrap(p)))
          ),
          "dependencies" -> Json.fromValues(
            List(
              Json.fromString("daml-stdlib"),
              Json.fromString("daml-prim"),
              Json.fromString(damlScriptLib),
            )
          ),
          "data-dependencies" -> Json.fromValues(dalfFiles.map(p => Json.fromString(p.toString))),
          "build-options" -> Json.fromValues(buildOptions.map(Json.fromString)),
        )
      )
      Printer(
        indent = 4,
        preserveOrder = true,
      ).pretty(json)
    }

    Files.write(
      dir.resolve("daml.yaml"),
      damlYaml.getBytes(StandardCharsets.UTF_8),
    )
  }
}
