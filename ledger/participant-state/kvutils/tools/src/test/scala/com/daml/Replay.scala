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

final case class BenchMarkState(
    name: String,
    transaction: TxEntry,
    contracts: ContractId => Option[Tx.ContractInst[ContractId]],
    contractKeys: GlobalKey => Option[ContractId],
)

@State(Scope.Benchmark)
class Replay {

  @Param(Array())
  var choiceName: String = _

  @Param(Array())
  var darFile: String = _

  @Param(Array())
  var ledgerFile: String = _

  private var engineDarFile: Option[String] = None
  private var engine: Engine = _
  private var benchmarksFile: Option[String] = None
  private var benchmarks: Map[String, BenchMarkState] = _
  private var benchmark: BenchMarkState = _

  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(): Unit = {
    val r = engine
      .replay(
        benchmark.transaction.tx,
        benchmark.transaction.ledgerTime,
        benchmark.transaction.participantId,
        benchmark.transaction.submissionTime,
        benchmark.transaction.submissionSeed,
      )
      .consume(benchmark.contracts, _ => Replay.unexpectedError, benchmark.contractKeys)
    assert(r.isRight)
  }

  @Setup(Level.Trial)
  def init(): Unit = {
    if (!engineDarFile.contains(darFile)) {
      engine = Replay.loadDar(Paths.get(darFile))
      engineDarFile = Some(darFile)
    }
    if (!benchmarksFile.contains(ledgerFile)) {
      benchmarks = Replay.loadBenchmarks(Paths.get(ledgerFile))
      benchmarksFile = Some(ledgerFile)
    }

    benchmark = benchmarks(choiceName)
    // before running the bench, we validate the transaction first to be sure everything is fine.
    val r = engine
      .validate(
        benchmark.transaction.tx,
        benchmark.transaction.ledgerTime,
        benchmark.transaction.participantId,
        benchmark.transaction.submissionTime,
        benchmark.transaction.submissionSeed,
      )
      .consume(benchmark.contracts, _ => Replay.unexpectedError, benchmark.contractKeys)
    assert(r.isRight)
  }

}

object Replay {

  private def unexpectedError = sys.error("Unexpected Error")

  def loadDar(darFile: Path): Engine = {
    println(s"%%% loading dar file $darFile ...")
    lazy val dar = UniversalArchiveReader().readFile(darFile.toFile).get
    lazy val packages = dar.all.map {
      case (pkgId, pkgArchive) => Decode.readArchivePayloadAndVersion(pkgId, pkgArchive)._1
    }.toMap
    val engine = new Engine(Engine.DevConfig)
    val r = engine
      .preloadPackage(dar.main._1, packages(dar.main._1))
      .consume(_ => unexpectedError, packages.get, _ => unexpectedError)
    assert(r.isRight)
    engine
  }

  private def exportEntries(file: Path): Stream[SubmissionInfo] = {
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

  private def decodeSubmission(participantId: ParticipantId, submission: Proto.DamlSubmission) = {
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
  }

  private def decodeEnvelope(participantId: ParticipantId, envelope: ByteString): Stream[TxEntry] =
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

  private def decodeSubmissionInfo(submissionInfo: SubmissionInfo) =
    decodeEnvelope(submissionInfo.participantId, submissionInfo.submissionEnvelope)

  private def loadBenchmarks(dumpFile: Path): Map[String, BenchMarkState] = {
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
            BenchMarkState(
              name = exe.templateId.qualifiedName.toString + ":" + exe.choiceId,
              transaction = entry,
              contracts = allContracts.filterKeys(inputContracts).get,
              contractKeys = inputContracts.iterator
                .flatMap(cid => allContractsWithKey.get(cid).toList.map(_ -> cid))
                .toMap
                .get
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

}
