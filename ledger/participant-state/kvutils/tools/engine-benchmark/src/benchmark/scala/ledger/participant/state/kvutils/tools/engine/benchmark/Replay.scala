// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.engine.benchmark

import java.lang.System.err.println
import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.export.{
  ProtobufBasedLedgerDataImporter,
  SubmissionInfo
}
import com.daml.ledger.participant.state.kvutils.{Envelope, DamlKvutils => Proto}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.daml.lf.crypto
import com.daml.lf.data._
import com.daml.lf.engine.Engine
import com.daml.lf.language.{Ast, LanguageVersion, Util => AstUtil}
import com.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  SubmittedTransaction,
  Transaction => Tx,
  TransactionCoder => TxCoder
}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.{Value, ValueCoder => ValCoder}
import com.google.protobuf.ByteString
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._

final case class TxEntry(
    tx: SubmittedTransaction,
    participantId: ParticipantId,
    submitters: List[Ref.Party],
    ledgerTime: Time.Timestamp,
    submissionTime: Time.Timestamp,
    submissionSeed: crypto.Hash,
) {
  // Note: this method will be removed when the entire kvutils code base
  // supports multi-party submissions
  def singleSubmitterOrThrow(): Ref.Party =
    if (submitters.length == 1)
      submitters.head
    else
      sys.error("Multi-party submissions are not supported")
}

final case class BenchmarkState(
    name: String,
    transaction: TxEntry,
    contracts: Map[ContractId, Tx.ContractInst[ContractId]],
    contractKeys: Map[GlobalKey, ContractId],
)

@State(Scope.Benchmark)
class Replay {

  @Param(Array())
  // choiceName of the exercise to benchmark
  // format: "ModuleName:TemplateName:ChoiceName"
  var choiceName: String = _

  @Param(Array())
  // path of the darFile
  var darFile: String = _

  @Param(Array())
  // path of the ledger export
  var ledgerFile: String = _

  @Param(Array("false"))
  // if 'true' try to adapt the benchmark to the dar
  var adapt: Boolean = _

  private var readDarFile: Option[String] = None
  private var loadedPackages: Map[Ref.PackageId, Ast.Package] = _
  private var engine: Engine = _
  private var benchmarksFile: Option[String] = None
  private var benchmarks: Map[String, BenchmarkState] = _
  private var benchmark: BenchmarkState = _

  private def getContract(cid: ContractId) =
    benchmark.contracts.get(cid)

  private def getContractKey(globalKeyWithMaintainers: GlobalKeyWithMaintainers) =
    benchmark.contractKeys.get(globalKeyWithMaintainers.globalKey)

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(): Unit = {
    val result = engine
      .replay(
        benchmark.transaction.submitters.toSet,
        benchmark.transaction.tx,
        benchmark.transaction.ledgerTime,
        benchmark.transaction.participantId,
        benchmark.transaction.submissionTime,
        benchmark.transaction.submissionSeed,
      )
      .consume(getContract, Replay.unexpectedError, getContractKey)
    assert(result.isRight)
  }

  @Setup(Level.Trial)
  def init(): Unit = {
    if (!readDarFile.contains(darFile)) {
      loadedPackages = Replay.loadDar(Paths.get(darFile))
      engine = Replay.compile(loadedPackages)
      readDarFile = Some(darFile)
    }
    if (!benchmarksFile.contains(ledgerFile)) {
      benchmarks = Replay.loadBenchmarks(Paths.get(ledgerFile))
      benchmarksFile = Some(ledgerFile)
    }

    benchmark =
      if (adapt)
        Replay.adapt(
          loadedPackages,
          engine.compiledPackages().packageLanguageVersion,
          benchmarks(choiceName)
        )
      else benchmarks(choiceName)

    // before running the bench, we validate the transaction first to be sure everything is fine.
    val result = engine
      .validate(
        benchmark.transaction.submitters.toSet,
        benchmark.transaction.tx,
        benchmark.transaction.ledgerTime,
        benchmark.transaction.participantId,
        benchmark.transaction.submissionTime,
        benchmark.transaction.submissionSeed,
      )
      .consume(getContract, Replay.unexpectedError, getContractKey)
    assert(result.isRight)
  }

}

object Replay {

  private val unexpectedError = (_: Any) => sys.error("Unexpected Error")

  private def loadDar(darFile: Path): Map[Ref.PackageId, Ast.Package] = {
    println(s"%%% loading dar file $darFile ...")
    UniversalArchiveReader()
      .readFile(darFile.toFile)
      .get
      .all
      .map {
        case (pkgId, pkgArchive) => Decode.readArchivePayloadAndVersion(pkgId, pkgArchive)._1
      }
      .toMap
  }

  private def compile(pkgs: Map[Ref.PackageId, Ast.Package]): Engine = {
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
  ): Stream[TxEntry] =
    submission.getPayloadCase match {
      case Proto.DamlSubmission.PayloadCase.TRANSACTION_ENTRY =>
        val entry = submission.getTransactionEntry
        val tx = TxCoder
          .decodeTransaction(
            TxCoder.NidDecoder,
            ValCoder.CidDecoder,
            submission.getTransactionEntry.getTransaction)
          .fold(err => sys.error(err.toString), SubmittedTransaction(_))
        Stream(
          TxEntry(
            tx = tx,
            participantId = participantId,
            submitters = entry.getSubmitterInfo.getSubmittersList.asScala.toList
              .map(Ref.Party.assertFromString),
            ledgerTime = parseTimestamp(entry.getLedgerEffectiveTime),
            submissionTime = parseTimestamp(entry.getSubmissionTime),
            submissionSeed = parseHash(entry.getSubmissionSeed)
          ))
      case _ =>
        Stream.empty
    }

  private[this] def decodeEnvelope(
      participantId: Ref.ParticipantId,
      envelope: ByteString,
  ): Stream[TxEntry] =
    assertRight(Envelope.open(envelope)) match {
      case Envelope.SubmissionMessage(submission) =>
        decodeSubmission(participantId, submission)
      case Envelope.SubmissionBatchMessage(batch) =>
        batch.getSubmissionsList.asScala.toStream
          .map(_.getSubmission)
          .flatMap(decodeEnvelope(participantId, _))
      case Envelope.LogEntryMessage(_) | Envelope.StateValueMessage(_) =>
        Stream.empty
    }

  private[this] def decodeSubmissionInfo(submissionInfo: SubmissionInfo) =
    decodeEnvelope(submissionInfo.participantId, submissionInfo.submissionEnvelope)

  private def loadBenchmarks(dumpFile: Path): Map[String, BenchmarkState] = {
    println(s"%%% load ledger export file  $dumpFile...")
    val importer = ProtobufBasedLedgerDataImporter(dumpFile)
    try {
      val transactions = importer.read().map(_._1).flatMap(decodeSubmissionInfo)
      if (transactions.isEmpty) sys.error("no transaction find")

      val createsNodes: Seq[Node.NodeCreate[ContractId]] =
        transactions.flatMap(entry =>
          entry.tx.nodes.values.collect {
            case create: Node.NodeCreate[ContractId] =>
              create
        })

      val allContracts: Map[ContractId, Value.ContractInst[Tx.Value[ContractId]]] =
        createsNodes.map(node => node.coid -> node.versionedCoinst).toMap

      val allContractsWithKey = createsNodes.flatMap { node =>
        node.key.toList.map(
          key =>
            node.coid -> GlobalKey(
              node.coinst.template,
              key.key.assertNoCid(key => s"found cid in key $key")))
      }.toMap

      val benchmarks = transactions.flatMap { entry =>
        entry.tx.roots.map(entry.tx.nodes) match {
          case ImmArray(exe: Node.NodeExercises[_, ContractId]) =>
            val inputContracts = entry.tx.inputContracts
            List(
              BenchmarkState(
                name = exe.templateId.qualifiedName.toString + ":" + exe.choiceId,
                transaction = entry,
                contracts = allContracts.filterKeys(inputContracts),
                contractKeys = inputContracts.iterator
                  .flatMap(cid => allContractsWithKey.get(cid).toList.map(_ -> cid))
                  .toMap
              ))
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
