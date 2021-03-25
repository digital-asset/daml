// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.engine.replay

import java.lang.System.err.println
import java.nio.file.Path

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.export.{
  ProtobufBasedLedgerDataImporter,
  SubmissionInfo,
}
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw, DamlKvutils => Proto}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.daml.lf.crypto
import com.daml.lf.data._
import com.daml.lf.engine.{Engine, Error}
import com.daml.lf.language.{Ast, LanguageVersion, Util => AstUtil}
import com.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  SubmittedTransaction,
  Transaction => Tx,
  TransactionCoder => TxCoder,
}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.{Value, ValueCoder => ValCoder}

import scala.collection.compat._
import scala.collection.compat.immutable.LazyList
import scala.jdk.CollectionConverters._

final case class TxEntry(
    tx: SubmittedTransaction,
    participantId: ParticipantId,
    submitters: List[Ref.Party],
    ledgerTime: Time.Timestamp,
    submissionTime: Time.Timestamp,
    submissionSeed: crypto.Hash,
)

final case class BenchmarkState(
    name: String,
    transaction: TxEntry,
    contracts: Map[ContractId, Tx.ContractInst[ContractId]],
    contractKeys: Map[GlobalKey, ContractId],
) {

  private def getContract(cid: ContractId) =
    contracts.get(cid)

  private def getContractKey(globalKeyWithMaintainers: GlobalKeyWithMaintainers) =
    contractKeys.get(globalKeyWithMaintainers.globalKey)

  def replay(engine: Engine): Either[Error, Unit] =
    engine
      .replay(
        transaction.submitters.toSet,
        transaction.tx,
        transaction.ledgerTime,
        transaction.participantId,
        transaction.submissionTime,
        transaction.submissionSeed,
      )
      .consume(getContract, Replay.unexpectedError, getContractKey)
      .map(_ => ())

  def validate(engine: Engine): Either[Error, Unit] =
    engine
      .validate(
        transaction.submitters.toSet,
        transaction.tx,
        transaction.ledgerTime,
        transaction.participantId,
        transaction.submissionTime,
        transaction.submissionSeed,
      )
      .consume(getContract, Replay.unexpectedError, getContractKey)
}

private[replay] object Replay {

  val unexpectedError = (_: Any) => sys.error("Unexpected Error")

  def loadDar(darFile: Path): Map[Ref.PackageId, Ast.Package] = {
    println(s"%%% loading dar file $darFile ...")
    UniversalArchiveReader()
      .readFile(darFile.toFile)
      .get
      .all
      .map { case (pkgId, pkgArchive) =>
        Decode.readArchivePayloadAndVersion(pkgId, pkgArchive)._1
      }
      .toMap
  }

  def compile(pkgs: Map[Ref.PackageId, Ast.Package]): Engine = {
    println(s"%%% compile ${pkgs.size} packages ...")
    val engine = Engine.DevEngine()
    AstUtil.dependenciesInTopologicalOrder(pkgs.keys.toList, pkgs).foreach { pkgId =>
      val r = engine
        .preloadPackage(pkgId, pkgs(pkgId))
        .consume(unexpectedError, unexpectedError, unexpectedError)
      assert(r.isRight)
    }
    engine
  }

  private[this] def decodeSubmission(
      participantId: Ref.ParticipantId,
      submission: Proto.DamlSubmission,
  ): LazyList[TxEntry] =
    submission.getPayloadCase match {
      case Proto.DamlSubmission.PayloadCase.TRANSACTION_ENTRY =>
        val entry = submission.getTransactionEntry
        val tx = TxCoder
          .decodeTransaction(
            TxCoder.NidDecoder,
            ValCoder.CidDecoder,
            submission.getTransactionEntry.getTransaction,
          )
          .fold(err => sys.error(err.toString), SubmittedTransaction(_))
        LazyList(
          TxEntry(
            tx = tx,
            participantId = participantId,
            submitters = entry.getSubmitterInfo.getSubmittersList.asScala.toList
              .map(Ref.Party.assertFromString),
            ledgerTime = parseTimestamp(entry.getLedgerEffectiveTime),
            submissionTime = parseTimestamp(entry.getSubmissionTime),
            submissionSeed = parseHash(entry.getSubmissionSeed),
          )
        )
      case _ =>
        LazyList.empty
    }

  private[this] def decodeEnvelope(
      participantId: Ref.ParticipantId,
      envelope: Raw.Envelope,
  ): LazyList[TxEntry] =
    assertRight(Envelope.open(envelope)) match {
      case Envelope.SubmissionMessage(submission) =>
        decodeSubmission(participantId, submission)
      case Envelope.SubmissionBatchMessage(batch) =>
        batch.getSubmissionsList.asScala
          .to(LazyList)
          .map(_.getSubmission)
          .flatMap(submissionEnvelope =>
            decodeEnvelope(participantId, Raw.Envelope(submissionEnvelope))
          )
      case Envelope.LogEntryMessage(_) | Envelope.StateValueMessage(_) =>
        LazyList.empty
    }

  private[this] def decodeSubmissionInfo(submissionInfo: SubmissionInfo) =
    decodeEnvelope(submissionInfo.participantId, submissionInfo.submissionEnvelope)

  def loadBenchmarks(dumpFile: Path): Map[String, BenchmarkState] = {
    println(s"%%% load ledger export file  $dumpFile...")
    val importer = ProtobufBasedLedgerDataImporter(dumpFile)
    try {
      val transactions = importer.read().map(_._1).flatMap(decodeSubmissionInfo)
      if (transactions.isEmpty) sys.error("no transaction find")

      val createsNodes: Seq[Node.NodeCreate[ContractId]] =
        transactions.flatMap(entry =>
          entry.tx.nodes.values.collect { case create: Node.NodeCreate[ContractId] =>
            create
          }
        )

      val allContracts: Map[ContractId, Value.ContractInst[Tx.Value[ContractId]]] =
        createsNodes.map(node => node.coid -> node.versionedCoinst).toMap

      val allContractsWithKey = createsNodes.flatMap { node =>
        node.key.toList.map(key =>
          node.coid -> GlobalKey.assertBuild(node.coinst.template, key.key)
        )
      }.toMap

      val benchmarks = transactions.flatMap { entry =>
        entry.tx.roots.map(entry.tx.nodes) match {
          case ImmArray(exe: Node.NodeExercises[_, ContractId]) =>
            val inputContracts = entry.tx.inputContracts
            List(
              BenchmarkState(
                name = exe.templateId.qualifiedName.toString + ":" + exe.choiceId,
                transaction = entry,
                contracts = allContracts.view.filterKeys(inputContracts).toMap,
                contractKeys = inputContracts.iterator
                  .flatMap(cid => allContractsWithKey.get(cid).toList.map(_ -> cid))
                  .toMap,
              )
            )
          case _ =>
            List.empty
        }
      }

      benchmarks.groupBy(_.name).flatMap {
        case (key, Seq(test)) =>
          println(s"%%% found 1 exercise for $key")
          List(key -> test)
        case (key, tests) =>
          println(s"%%% found ${tests.length} exercises for $key. IGNORED")
          List.empty
      }
    } finally {
      importer.close()
    }
  }

  def adapt(
      pkgs: Map[Ref.PackageId, Ast.Package],
      pkgLangVersion: Ref.PackageId => LanguageVersion,
      state: BenchmarkState,
  ): BenchmarkState = {
    val adapter = new Adapter(pkgs, pkgLangVersion)
    state.copy(
      transaction = state.transaction.copy(tx = adapter.adapt(state.transaction.tx)),
      contracts = state.contracts.transform((_, v) => adapter.adapt(v)),
      contractKeys = state.contractKeys.iterator.map { case (k, v) => adapter.adapt(k) -> v }.toMap,
    )
  }

}
