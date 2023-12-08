// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.Engine
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.lf.transaction.{Node, NodeId, Versioned}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

class NodeSeedsTestV1 extends NodeSeedsTest(LanguageMajorVersion.V1)
class NodeSeedsTestV2 extends NodeSeedsTest(LanguageMajorVersion.V2)

class NodeSeedsTest(majorLanguageVersion: LanguageMajorVersion) extends AnyWordSpec with Matchers {

  // Test for https://github.com/DACH-NY/canton/issues/14712

  val (mainPkgId, mainPkg, packages) = {
    val packages = UniversalArchiveDecoder.assertReadFile(
      new File(
        BazelRunfiles.rlocation(s"daml-lf/engine/Demonstrator-v${majorLanguageVersion.pretty}.dar")
      )
    )
    (packages.main._1, packages.main._2, packages.all.toMap)
  }

  val engine = Engine.DevEngine(majorLanguageVersion)

  val operator = Ref.Party.assertFromString("operator")
  val investor = Ref.Party.assertFromString("investor")
  val time = Time.Timestamp.now()

  val requestTmplId =
    Ref.Identifier(mainPkgId, Ref.QualifiedName.assertFromString("Demonstrator:TransferRequest"))
  val requestCid: Value.ContractId = Value.ContractId.V1.assertFromString(
    "001000000000000000000000000000000000000000000000000000000000000000"
  )
  val requestContract =
    Versioned(
      transaction.TransactionVersion.VDev,
      Value.ContractInstance(
        mainPkg.name,
        requestTmplId,
        Value.ValueRecord(
          None,
          ImmArray(None -> Value.ValueParty(operator), None -> Value.ValueParty(investor)),
        ),
      ),
    )
  val roleTmplId =
    Ref.Identifier(mainPkgId, Ref.QualifiedName.assertFromString("Demonstrator:RegistrarRole"))
  val roleCid: Value.ContractId = Value.ContractId.V1.assertFromString(
    "002000000000000000000000000000000000000000000000000000000000000000"
  )
  val roleContract = Versioned(
    transaction.TransactionVersion.VDev,
    Value.ContractInstance(
      mainPkg.name,
      roleTmplId,
      Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(operator))),
    ),
  )
  val contracts = Map(requestCid -> requestContract, roleCid -> roleContract)

  implicit val loggingContext: LoggingContext = LoggingContext.empty

  val Right((tx, metaData)) =
    engine
      .submit(
        Set(operator),
        Set.empty,
        command.ApiCommands(
          ImmArray(
            command.ApiCommand.Exercise(
              roleTmplId,
              roleCid,
              Ref.ChoiceName.assertFromString("AcceptTransfer"),
              Value.ValueRecord(None, ImmArray(None -> Value.ValueContractId(requestCid))),
            )
          ),
          time,
          "some ref",
        ),
        participantId = Ref.ParticipantId.assertFromString("participant"),
        submissionSeed = crypto.Hash.hashPrivateKey(getClass.getName + time.toString),
      )
      .consume(pcs = contracts, pkgs = packages)

  val nodeSeeds = metaData.nodeSeeds.iterator.toMap

  // return the create nodes under node `rootId`.
  def projectCreates(rootId: NodeId) =
    tx.foldInExecutionOrder((false, Set.empty[Node.Create]))(
      exerciseBegin = { case ((collecting, creates), nodeId, _) =>
        ((collecting || nodeId == rootId, creates), ChildrenRecursion.DoRecurse)
      },
      rollbackBegin = { case ((collecting, creates), nodeId, _) =>
        ((collecting || nodeId == rootId, creates), ChildrenRecursion.DoRecurse)
      },
      leaf = {
        case ((collecting, creates), nodeId, create: Node.Create)
            if collecting || nodeId == rootId =>
          (collecting, creates + create)
        case (state, _, _) => state
      },
      exerciseEnd = { case ((collecting, creates), nodeId, _) =>
        (collecting && rootId != nodeId, creates)
      },
      rollbackEnd = { case ((collecting, creates), nodeId, _) =>
        (collecting && rootId != nodeId, creates)
      },
    )._2

  def replay(nodeId: NodeId) = {
    val cmd = tx.nodes(nodeId) match {
      case exe: Node.Exercise =>
        command.ReplayCommand.Exercise(
          exe.templateId,
          None,
          exe.targetCoid,
          exe.choiceId,
          exe.chosenValue,
        )
      case create: Node.Create =>
        command.ReplayCommand.Create(
          create.templateId,
          create.arg,
        )
      case fetch: Node.Fetch if fetch.byKey =>
        command.ReplayCommand.FetchByKey(
          fetch.templateId,
          fetch.keyOpt.get.value,
        )
      case fetch: Node.Fetch =>
        command.ReplayCommand.Fetch(
          fetch.templateId,
          fetch.coid,
        )
      case lookup: Node.LookupByKey =>
        command.ReplayCommand.LookupByKey(
          lookup.templateId,
          lookup.key.value,
        )
      case _ =>
        sys.error("unexpected node")
    }
    val Right((rTx, _)) =
      engine
        .reinterpret(
          Set(operator),
          cmd,
          nodeSeeds.get(nodeId),
          time,
          time,
        )(LoggingContext.empty)
        .consume(pcs = contracts, pkgs = packages)
    rTx.nodes.values.collect { case create: Node.Create => create }.toSet
  }

  val n = tx.nodes.iterator.collect { case (nid, _: Node.Action) =>
    s"when run with $nid" in {
      replay(nid) shouldBe projectCreates(nid)
    }
  }.size

  // We double check we have exactly 4 action nodes in the transaction
  assert(n == 4)

}
