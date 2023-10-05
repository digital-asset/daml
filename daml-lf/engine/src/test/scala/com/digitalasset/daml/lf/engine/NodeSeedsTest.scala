// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

//import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.data.{Ref, ImmArray, Time}
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.lf.transaction.{Versioned, Node, NodeId}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

class NodeSeedsTest extends AnyWordSpec with Matchers {

  // Test for https://github.com/DACH-NY/canton/issues/14712

  val (mainPkgId, packages) = {
    val packages = UniversalArchiveDecoder.assertReadFile(
      //     new File(BazelRunfiles.rlocation("daml-lf/engine/Demonstrator.dar"))
      new File("/tmp/Demonstrator.dar")
    )
    (packages.main._1, packages.all.toMap)
  }

  val engine = Engine.DevEngine()

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
        "",
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
      "",
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
      .consume(pcs = contracts.get, packages = packages.get, keys = _ => None)

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
          fetch.key.get.key,
        )
      case fetch: Node.Fetch =>
        command.ReplayCommand.Fetch(
          fetch.templateId,
          fetch.coid,
        )
      case lookup: Node.LookupByKey =>
        command.ReplayCommand.LookupByKey(
          lookup.templateId,
          lookup.key.key,
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
        .consume(pcs = contracts.get, packages = packages.get, keys = _ => None)
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
