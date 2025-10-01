// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.daml.bazeltools.BazelRunfiles
import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.archive.UniversalArchiveDecoder
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.transaction.Transaction.ChildrenRecursion
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{Node, NodeId}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

class NodeSeedsTestV2 extends NodeSeedsTest(LanguageMajorVersion.V2)

class NodeSeedsTest(majorLanguageVersion: LanguageMajorVersion) extends AnyWordSpec with Matchers {

  // Test for https://github.com/DACH-NY/canton/issues/14712

  val (mainPkgId, mainPkg, packages) = {
    val packages = UniversalArchiveDecoder.assertReadFile(
      new File(
        BazelRunfiles.rlocation(
          // TODO(https://github.com/digital-asset/daml/issues/18457): split key test cases and
          //  revert to non-dev dar
          s"daml-lf/engine/Demonstrator-v${majorLanguageVersion.pretty}dev.dar"
        )
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
    TransactionBuilder.fatContractInstanceWithDummyDefaults(
      version = transaction.TransactionVersion.VDev,
      packageName = mainPkg.pkgName,
      template = requestTmplId,
      arg = Value.ValueRecord(
        None,
        ImmArray(None -> Value.ValueParty(operator), None -> Value.ValueParty(investor)),
      ),
      signatories = List(operator),
      observers = List(investor),
    )
  val roleTmplId =
    Ref.Identifier(mainPkgId, Ref.QualifiedName.assertFromString("Demonstrator:RegistrarRole"))
  val roleCid: Value.ContractId = Value.ContractId.V1.assertFromString(
    "002000000000000000000000000000000000000000000000000000000000000000"
  )
  val roleContract = TransactionBuilder.fatContractInstanceWithDummyDefaults(
    version = transaction.TransactionVersion.VDev,
    packageName = mainPkg.pkgName,
    template = roleTmplId,
    arg = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(operator))),
    signatories = List(operator),
  )
  val contracts = Map(requestCid -> requestContract, roleCid -> roleContract)

  implicit val loggingContext: LoggingContext = LoggingContext.empty

  val Right((tx, metaData)) =
    engine
      .submit(
        submitters = Set(operator),
        readAs = Set.empty,
        cmds = command.ApiCommands(
          ImmArray(
            command.ApiCommand.Exercise(
              roleTmplId.toRef,
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
        prefetchKeys = Seq.empty,
      )
      .consume(pcs = contracts, pkgs = packages) match {
      case Right(x) =>
        println(s"Right encountered: $x")
        Right(x)
      case Left(error) =>
        // Failure: print the error and potentially throw an exception
        // or return a default/sentinel value if appropriate for the type.
        // Since you only want to proceed on Right, throwing is often
        // the clearest way to stop execution.
        println(s"Error encountered (Left): $error")
        throw new RuntimeException(s"Expression was Left: $error")
    }

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
          fetch.interfaceId,
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
