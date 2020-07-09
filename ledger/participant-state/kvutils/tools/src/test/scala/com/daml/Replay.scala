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
import com.daml.lf.transaction.Node.{GlobalKey, KeyWithMaintainers}
import com.daml.lf.transaction.test.{TransactionBuilder => TxBuilder}
import com.daml.lf.transaction.{Node, Transaction => Tx, TransactionCoder => TxCoder}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.{Value, ValueCoder => ValCoder}
import com.google.protobuf.ByteString
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._
import scala.collection.mutable

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
  private var pkgs: Map[Ref.PackageId, Ast.Package] = _
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
      .consume(benchmark.contracts.get, _ => Replay.unexpectedError, benchmark.contractKeys.get)
    assert(r.isRight)
  }

  @Setup(Level.Trial)
  def init(): Unit = {
    if (!readDarFile.contains(darFile)) {
      pkgs = Replay.loadDar(Paths.get(darFile))
      engine = Replay.compile(pkgs)
      readDarFile = Some(darFile)
    }
    if (!benchmarksFile.contains(ledgerFile)) {
      benchmarks = Replay.loadBenchmarks(Paths.get(ledgerFile))
      benchmarksFile = Some(ledgerFile)
    }

    benchmark = if (adapt) Replay.adapt(pkgs, benchmarks(choiceName)) else benchmarks(choiceName)

    // before running the bench, we validate the transaction first to be sure everything is fine.
    val r = engine
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
    assert(r.isRight)
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

  def adapt(pkgs: Map[Ref.PackageId, Ast.Package], benchMarkState: BenchMarkState): BenchMarkState =
    new Adapter(pkgs).adapt(benchMarkState)

  private[this] final class Adapter(pkgs: Map[Ref.PackageId, Ast.Package]) {

    def adapt(state: BenchMarkState): BenchMarkState =
      state.copy(
        transaction = state.transaction.copy(tx = adapt(state.transaction.tx)),
        contracts = state.contracts.transform((_, v) => adapt(v)),
        contractKeys = state.contractKeys.iterator.map { case (k, v) => adapt(k) -> v }.toMap,
      )

    private[this] def adapt(tx: Tx.Transaction): Tx.SubmittedTransaction =
      tx.foldWithPathState(new TxBuilder, Option.empty[Tx.NodeId])(
          (builder, parent, _, node) =>
            (builder, Some(parent.fold(builder.add(adapt(node)))(builder.add(adapt(node), _))))
        )
        .buildSubmitted()

    // drop version and children
    private[this] def adapt(node: Tx.Node): Node.GenNode[Tx.NodeId, ContractId, Value[ContractId]] =
      node match {
        case create: Node.NodeCreate.WithTxValue[ContractId] =>
          create.copy(
            coinst =
              create.coinst.copy(adapt(create.coinst.template), adapt(create.coinst.arg.value)),
            optLocation = None,
            key = create.key.map(adapt)
          )
        case exe: Node.NodeExercises.WithTxValue[Tx.NodeId, ContractId] =>
          exe.copy(
            templateId = adapt(exe.templateId),
            optLocation = None,
            chosenValue = adapt(exe.chosenValue.value),
            children = ImmArray.empty,
            exerciseResult = exe.exerciseResult.map(v => adapt(v.value)),
            key = exe.key.map(adapt),
          )
        case fetch: Node.NodeFetch.WithTxValue[ContractId] =>
          fetch.copy(
            templateId = adapt(fetch.templateId),
            optLocation = None,
            key = fetch.key.map(adapt),
          )
        case lookup: Node.NodeLookupByKey.WithTxValue[ContractId] =>
          lookup.copy(
            templateId = adapt(lookup.templateId),
            optLocation = None,
            key = adapt(lookup.key),
          )
      }

    // drop version
    private[this] def adapt(
        k: KeyWithMaintainers[Tx.Value[ContractId]],
    ): KeyWithMaintainers[Value[ContractId]] =
      k.copy(adapt(k.key.value))

    private[this] def adapt(coinst: Tx.ContractInst[ContractId]): Tx.ContractInst[ContractId] =
      coinst.copy(
        template = adapt(coinst.template),
        arg = coinst.arg.copy(value = adapt(coinst.arg.value)),
      )

    private[this] def adapt(gkey: GlobalKey): GlobalKey =
      GlobalKey.assertBuild(adapt(gkey.templateId), adapt(gkey.key))

    private[this] def adapt(value: Value[ContractId]): Value[ContractId] =
      value match {
        case Value.ValueEnum(tycon, value) =>
          Value.ValueEnum(tycon.map(adapt), value)
        case Value.ValueRecord(tycon, fields) =>
          Value.ValueRecord(tycon.map(adapt), fields.map { case (f, v) => f -> adapt(v) })
        case Value.ValueVariant(tycon, variant, value) =>
          Value.ValueVariant(tycon.map(adapt), variant, adapt(value))
        case Value.ValueList(values) =>
          Value.ValueList(values.map(adapt))
        case Value.ValueOptional(value) =>
          Value.ValueOptional(value.map(adapt))
        case Value.ValueTextMap(value) =>
          Value.ValueTextMap(value.mapValue(adapt))
        case Value.ValueGenMap(entries) =>
          Value.ValueGenMap(entries.map { case (k, v) => adapt(k) -> adapt(v) })
        case Value.ValueStruct(fields) =>
          Value.ValueStruct(fields.map { case (f, v) => f -> adapt(v) })
        case _: Value.ValueCidlessLeaf | _: Value.ValueContractId[ContractId] =>
          value
      }

    private[this] val cache = mutable.Map.empty[Ref.Identifier, Ref.Identifier]

    private[this] def adapt(id: Ref.Identifier): Ref.Identifier =
      cache.getOrElseUpdate(id, assertRight(lookup(id)))

    private[this] def lookup(id: Ref.Identifier): Either[String, Ref.Identifier] = {
      val pkgIds = pkgs.collect {
        case (pkgId, pkg) if pkg.lookupIdentifier(id.qualifiedName).isRight => pkgId
      }
      pkgIds.toSeq match {
        case Seq(pkgId) => Right(id.copy(packageId = pkgId))
        case Seq() => Left(s"no package foud for ${id.qualifiedName}")
        case _ => Left(s"2 or more packages found for ${id.qualifiedName}")
      }
    }

  }

}
