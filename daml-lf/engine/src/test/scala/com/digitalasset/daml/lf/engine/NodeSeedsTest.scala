// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.Engine
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.transaction.{Node, NodeId, Transaction, Versioned}
import com.daml.lf.transaction.test.TransactionUtil
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

class NodeSeedsTestV1 extends NodeSeedsTest(LanguageMajorVersion.V1)
class NodeSeedsTestV2 extends NodeSeedsTest(LanguageMajorVersion.V2)

class NodeSeedsTest(majorLanguageVersion: LanguageMajorVersion) extends AnyWordSpec with Matchers {

  // Test for https://github.com/DACH-NY/canton/issues/14712

  val (mainPkgId, packages) = {
    val packages = UniversalArchiveDecoder.assertReadFile(
      new File(
        BazelRunfiles.rlocation(s"daml-lf/engine/Demonstrator-v${majorLanguageVersion.pretty}.dar")
      )
    )
    (packages.main._1, packages.all.toMap)
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
      roleTmplId,
      Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(operator))),
    ),
  )
  val contracts = Map(requestCid -> requestContract, roleCid -> roleContract)

  implicit val loggingContext = LoggingContext.empty

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

  def replay(nodeId: NodeId): Transaction = {
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
    rTx.transaction
  }

  val n = tx.nodes.iterator.collect { case (nid, node: Node.Action) =>
    s"when run with $nid" in {
      node match {
        case e: Node.Exercise if e.exerciseResult.isEmpty =>
          // replaying an exercise node without a result should give a transaction with a rollback node.
          replay(nid).nodes.collect { case (_, _: Node.Rollback) => () } should not be empty
        case _ =>
          // replaying any other action node should give a transaction that matches the naive projection of that node.
          replay(nid) shouldBe TransactionUtil.projectNaively(tx.transaction, nid)
      }
    }
  }.size

  // We double check we have exactly 4 action nodes in the transaction
  assert(n == 4)

}
