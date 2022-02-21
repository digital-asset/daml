// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.engine.replay

import java.lang.System.err.println
import java.nio.file.Path
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.export.{
  ProtobufBasedLedgerDataImporter,
  SubmissionInfo,
}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{Conversions, Envelope, Raw}
import com.daml.lf.archive.{ArchiveDecoder, UniversalArchiveDecoder}
import com.daml.lf.crypto
import com.daml.lf.data._
import com.daml.lf.engine.{Engine, EngineConfig, Error}
import com.daml.lf.kv.transactions.RawTransaction
import com.daml.lf.language.{Ast, LanguageVersion, Util => AstUtil}
import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, Node, SubmittedTransaction}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.google.protobuf.ByteString

import scala.jdk.CollectionConverters._

sealed abstract class SubmissionEntry extends Product with Serializable

final case class TxEntry(
    tx: SubmittedTransaction,
    participantId: Ref.ParticipantId,
    submitters: List[Ref.Party],
    ledgerTime: Time.Timestamp,
    submissionTime: Time.Timestamp,
    submissionSeed: crypto.Hash,
) extends SubmissionEntry

final case class PkgEntry(archive: ByteString) extends SubmissionEntry

final class BenchmarkState(
    val transaction: TxEntry,
    val contracts: Map[ContractId, Value.VersionedContractInstance],
    val contractKeys: Map[GlobalKey, ContractId],
    val pkgs: Map[Ref.PackageId, Ast.Package],
    val profileDir: Option[Path],
) {

  private[this] implicit def loggingContext: LoggingContext = LoggingContext.ForTesting

  private[this] def getContractKey(globalKeyWithMaintainers: GlobalKeyWithMaintainers) =
    contractKeys.get(globalKeyWithMaintainers.globalKey)

  private[this] lazy val engine = Replay.compile(pkgs, profileDir)

  def replay(): Either[Error, Unit] =
    engine
      .replay(
        transaction.submitters.toSet,
        transaction.tx,
        transaction.ledgerTime,
        transaction.participantId,
        transaction.submissionTime,
        transaction.submissionSeed,
      )
      .consume(contracts.get, pkgs.get, getContractKey)
      .map(_ => ())

  def validate(): Either[Error, Unit] =
    engine
      .validate(
        transaction.submitters.toSet,
        transaction.tx,
        transaction.ledgerTime,
        transaction.participantId,
        transaction.submissionTime,
        transaction.submissionSeed,
      )
      .consume(contracts.get, pkgs.get, getContractKey)

}

private[replay] object Replay {

  val unexpectedError = (_: Any) => sys.error("Unexpected Error")

  def loadDar(darFile: Path): Map[Ref.PackageId, Ast.Package] = {
    println(s"%%% loading dar file $darFile ...")
    UniversalArchiveDecoder.assertReadFile(darFile.toFile).all.toMap
  }

  def compile(pkgs: Map[Ref.PackageId, Ast.Package], profileDir: Option[Path] = None): Engine = {
    println(s"%%% compile ${pkgs.size} packages ...")
    val engine = new Engine(
      EngineConfig(allowedLanguageVersions = LanguageVersion.DevVersions, profileDir = profileDir)
    )
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
      submission: DamlSubmission,
  ): LazyList[SubmissionEntry] = {
    submission.getPayloadCase match {
      case DamlSubmission.PayloadCase.TRANSACTION_ENTRY =>
        val entry = submission.getTransactionEntry
        val rawTransaction = RawTransaction(submission.getTransactionEntry.getRawTransaction)
        val tx = SubmittedTransaction(Conversions.assertDecodeTransaction(rawTransaction))
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
      case DamlSubmission.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        val entry = submission.getPackageUploadEntry
        entry.getArchivesList.asScala.iterator.map(PkgEntry).to(LazyList)
      case _ =>
        LazyList.empty
    }
  }

  private[this] def decodeEnvelope(
      participantId: Ref.ParticipantId,
      envelope: Raw.Envelope,
  ): LazyList[SubmissionEntry] =
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

  def loadBenchmark(
      dumpFile: Path,
      choice: (Ref.QualifiedName, Ref.Name),
      index: Int,
      profileDir: Option[Path],
  ): BenchmarkState = {
    println(s"%%% load ledger export file  $dumpFile...")
    val importer = ProtobufBasedLedgerDataImporter(dumpFile)

    var idx: Int = index
    var contracts = Map.empty[ContractId, Value.VersionedContractInstance]
    var contractKeys = Map.empty[GlobalKey, ContractId]
    var pkgs = Map.empty[Ref.PackageId, Ast.Package]

    var result = Option.empty[BenchmarkState]

    try {
      val entries = importer.read().map(_._1).flatMap(decodeSubmissionInfo).iterator

      while (result.isEmpty && entries.hasNext) {
        entries.next() match {
          case entry: TxEntry =>
            val root = entry.tx.roots.iterator.toSet
            entry.tx.foreachInExecutionOrder(
              { (nid, exe) =>
                if (root(nid) && (exe.templateId.qualifiedName, exe.choiceId) == choice) {
                  if (idx == 0) {
                    val inputContract = entry.tx.inputContracts
                    result = Some(
                      new BenchmarkState(
                        entry,
                        contracts.filter { case (cid, _) => inputContract(cid) },
                        contractKeys.filter { case (_, cid) => inputContract(cid) },
                        pkgs,
                        profileDir,
                      )
                    )
                  }
                  idx -= 1
                }
                if (exe.consuming) {
                  contracts = contracts - exe.targetCoid
                  exe.key.foreach(key =>
                    contractKeys = contractKeys - GlobalKey.assertBuild(exe.templateId, key.key)
                  )
                }
                ChildrenRecursion.DoRecurse
              },
              (_, _) => ChildrenRecursion.DoNotRecurse,
              {
                case (_, create: Node.Create) =>
                  contracts = contracts.updated(create.coid, create.versionedCoinst)
                  create.key.foreach(key =>
                    contractKeys = contractKeys
                      .updated(GlobalKey.assertBuild(create.templateId, key.key), create.coid)
                  )
                case (_, _) =>
              },
              (_, _) => (),
              (_, _) => (),
            )
          case PkgEntry(archive) =>
            pkgs += ArchiveDecoder.assertFromByteString(archive)
        }
      }
    } finally {
      importer.close()
    }

    result match {
      case Some(value) => value
      case None => sys.error(s"choice ${choice._1}:${choice._2} not found")
    }
  }

  def adapt(pkgs: Map[Ref.PackageId, Ast.Package], state: BenchmarkState): BenchmarkState = {
    val adapter = new Adapter(pkgs)
    new BenchmarkState(
      transaction = state.transaction.copy(tx = adapter.adapt(state.transaction.tx)),
      contracts = state.contracts.transform((_, v) => adapter.adapt(v)),
      contractKeys = state.contractKeys.iterator.map { case (k, v) => adapter.adapt(k) -> v }.toMap,
      pkgs,
      state.profileDir,
    )
  }

}
