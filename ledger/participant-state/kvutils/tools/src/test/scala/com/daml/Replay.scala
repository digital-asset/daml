// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.test

import java.io.DataInputStream
import java.lang.System.err.println
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.export.FileBasedLedgerDataExporter.SubmissionInfo
import com.daml.ledger.participant.state.kvutils.export.Serialization
import com.daml.ledger.participant.state.kvutils.{Envelope, DamlKvutils => Proto}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.daml.lf.crypto
import com.daml.lf.data._
import com.daml.lf.engine.Engine
import com.daml.lf.language.{Ast, Util => AstUtil}
import com.daml.lf.transaction.Node.GlobalKey
import com.daml.lf.transaction.{Node, Transaction => Tx, TransactionCoder => TxCoder}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.{Value, ValueCoder => ValCoder}
import com.google.protobuf.ByteString
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._

final case class TxEntry(
    tx: Tx.SubmittedTransaction,
    participantId: ParticipantId,
    ledgerTime: Time.Timestamp,
    submissionTime: Time.Timestamp,
    submissionSeed: crypto.Hash,
)

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

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(): Unit = {
    val result = engine
      .replay(
        benchmark.transaction.tx,
        benchmark.transaction.ledgerTime,
        benchmark.transaction.participantId,
        benchmark.transaction.submissionTime,
        benchmark.transaction.submissionSeed,
      )
      .consume(benchmark.contracts.get, _ => Replay.unexpectedError, benchmark.contractKeys.get)
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
      if (adapt) Replay.adapt(loadedPackages, benchmarks(choiceName)) else benchmarks(choiceName)

    // before running the bench, we validate the transaction first to be sure everything is fine.
    val result = engine
      .validate(
        benchmark.transaction.tx,
        benchmark.transaction.ledgerTime,
        benchmark.transaction.participantId,
        benchmark.transaction.submissionTime,
        benchmark.transaction.submissionSeed,
      )
      .consume(
        benchmark.contracts.get,
        pkgId => throw new IllegalArgumentException(s"package $pkgId not found"),
        benchmark.contractKeys.get,
      )
    assert(result.isRight)
  }

}

object Replay {

  private def unexpectedError = sys.error("Unexpected Error")

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
    val engine = new Engine(Engine.DevConfig)
    AstUtil.dependenciesInTopologicalOrder(pkgs.keys.toList, pkgs).foreach { pkgId =>
      val r = engine
        .preloadPackage(pkgId, pkgs(pkgId))
        .consume(_ => unexpectedError, _ => unexpectedError, _ => unexpectedError)
      assert(r.isRight)
    }
    engine
  }

  private[this] def exportEntries(file: Path): Stream[SubmissionInfo] = {

    val ledgerExportStream = new DataInputStream(Files.newInputStream(file))

    def go: Stream[SubmissionInfo] =
      if (ledgerExportStream.available() > 0)
        Serialization.readEntry(ledgerExportStream)._1 #:: go
      else {
        ledgerExportStream.close()
        Stream.empty
      }

    go
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
          .fold(err => sys.error(err.toString), Tx.SubmittedTransaction(_))
        Stream(
          TxEntry(
            tx = tx,
            participantId = participantId,
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
    val transactions = exportEntries(dumpFile).flatMap(decodeSubmissionInfo)
    if (transactions.isEmpty) sys.error("no transaction find")

    val createsNodes: Seq[Node.NodeCreate.WithTxValue[ContractId]] =
      transactions.flatMap(entry =>
        entry.tx.nodes.values.collect {
          case create: Node.NodeCreate.WithTxValue[ContractId] =>
            create
      })

    val allContracts: Map[ContractId, Value.ContractInst[Tx.Value[ContractId]]] =
      createsNodes.map(node => node.coid -> node.coinst).toMap

    val allContractsWithKey = createsNodes.flatMap { node =>
      node.key.toList.map(
        key =>
          node.coid -> Node.GlobalKey(
            node.coinst.template,
            key.key.value.assertNoCid(key => s"found cid in key $key")))
    }.toMap

    val benchmarks = transactions.flatMap { entry =>
      entry.tx.roots.map(entry.tx.nodes) match {
        case ImmArray(exe: Node.NodeExercises.WithTxValue[_, ContractId]) =>
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

  }

  def adapt(pkgs: Map[Ref.PackageId, Ast.Package], state: BenchmarkState): BenchmarkState = {
    val adapter = new Adapter(pkgs)
    state.copy(
      transaction = state.transaction.copy(tx = adapter.adapt(state.transaction.tx)),
      contracts = state.contracts.transform((_, v) => adapter.adapt(v)),
      contractKeys = state.contractKeys.iterator.map { case (k, v) => adapter.adapt(k) -> v }.toMap,
    )
  }

}
