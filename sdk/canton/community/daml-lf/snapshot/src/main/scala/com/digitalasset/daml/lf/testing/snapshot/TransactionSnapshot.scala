// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.archive.{ArchiveDecoder, DarDecoder}
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.engine.{Engine, EngineConfig, Error}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, Util => AstUtil}
import com.digitalasset.daml.lf.speedy.metrics.{StepCount, TxNodeCount}
import com.digitalasset.daml.lf.speedy.Speedy
import com.digitalasset.daml.lf.testing.snapshot.Snapshot.SubmissionEntry.EntryCase
import com.digitalasset.daml.lf.transaction.Transaction.ChildrenRecursion
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  GlobalKeyWithMaintainers,
  Node,
  SubmittedTransaction => SubmittedTx,
  TransactionCoder => TxCoder,
  TransactionOuterClass => TxOuterClass,
}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.value.ContractIdVersion
import com.google.protobuf.ByteString

import java.io.BufferedInputStream
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

final case class TransactionSnapshot(
    transaction: SubmittedTx,
    participantId: Ref.ParticipantId,
    submitters: Set[Ref.Party],
    ledgerTime: Time.Timestamp,
    preparationTime: Time.Timestamp,
    submissionSeed: crypto.Hash,
    contracts: Map[ContractId, FatContractInstance],
    contractKeys: Map[GlobalKeyWithMaintainers, ContractId],
    pkgs: Map[Ref.PackageId, Ast.Package],
    profileDir: Option[Path],
    contractIdVersion: ContractIdVersion,
    gasBudget: Option[Long],
) {

  private[this] implicit def loggingContext: LoggingContext = LoggingContext.ForTesting

  private[this] lazy val engine =
    TransactionSnapshot.compile(pkgs, profileDir, gasBudget = gasBudget)

  private[this] lazy val metricPlugins =
    Seq(new StepCount(engine.config.iterationsBetweenInterruptions), new TxNodeCount)

  def replay(): Either[Error, Speedy.Metrics] =
    engine
      .replayAndCollectMetrics(
        submitters,
        transaction,
        ledgerTime,
        participantId,
        preparationTime,
        submissionSeed,
        contractIdVersion,
        metricPlugins = metricPlugins,
      )
      .consume(contracts, pkgs, contractKeys)
      .map { case (_, _, metrics) => metrics }

  def validate(): Either[Error, Unit] =
    engine
      .validate(
        submitters,
        transaction,
        ledgerTime,
        participantId,
        preparationTime,
        submissionSeed,
        contractIdVersion,
        metricPlugins = metricPlugins,
      )
      .consume(contracts, pkgs, contractKeys)

}

private[snapshot] object TransactionSnapshot {

  val unexpectedError: PartialFunction[Any, Nothing] = { case _ => sys.error("Unexpected Error") }

  def loadDar(darFile: Path): Map[Ref.PackageId, Ast.Package] = {
    println(s"%%% loading dar file $darFile ...")
    DarDecoder.assertReadArchiveFromFile(darFile.toFile).all.toMap
  }

  def compile(
      pkgs: Map[Ref.PackageId, Ast.Package],
      profileDir: Option[Path] = None,
      snapshotDir: Option[Path] = None,
      gasBudget: Option[Long] = None,
  ): Engine = {
    require(pkgs.nonEmpty, "expected at least one package, got none")
    println(s"%%% compile ${pkgs.size} packages ...")
    val engine = new Engine(
      EngineConfig(
        allowedLanguageVersions = LanguageVersion.allLfVersionsRange,
        profileDir = profileDir,
        snapshotDir = snapshotDir,
        gasBudget = gasBudget,
      )
    )
    AstUtil.dependenciesInTopologicalOrder(pkgs.keys.toList, pkgs).foreach { pkgId =>
      val r = engine
        .preloadPackage(pkgId, pkgs(pkgId))
        .consume(unexpectedError, unexpectedError, unexpectedError)
      assert(r.isRight)
    }
    engine
  }

  def getAllTopLevelChoiceNames(dumpFile: Path): Set[String] = {
    val inputStream = new BufferedInputStream(Files.newInputStream(dumpFile))

    val entries = new Iterator[Snapshot.SubmissionEntry] {
      override def hasNext: Boolean = (inputStream.available() != 0)

      override def next(): Snapshot.SubmissionEntry = {
        Snapshot.SubmissionEntry.parseDelimitedFrom(inputStream)
      }
    }

    var result = Set.empty[String]

    def decodeTx(txEntry: Snapshot.TransactionEntry): SubmittedTx = {
      val protoTx = TxOuterClass.Transaction.parseFrom(txEntry.getRawTransaction)
      TxCoder
        .decodeTransaction(protoTx)
        .fold(
          err => sys.error("Decoding Error: " + err.errorMessage),
          tx => SubmittedTx(tx),
        )
    }

    def updateWithChoicesFromTx(tx: SubmittedTx): Unit =
      tx.foreachInExecutionOrder(
        exerciseBegin = { (_, exe) =>
          result = result + s"${exe.templateId}:${exe.choiceId}"
          ChildrenRecursion.DoNotRecurse
        },
        rollbackBegin = (_, _) => ChildrenRecursion.DoNotRecurse,
        leaf = (_, _) => (),
        exerciseEnd = (_, _) => (),
        rollbackEnd = (_, _) => (),
      )

    try {
      while (entries.hasNext) {
        val entry = entries.next()
        entry.getEntryCase match {
          case EntryCase.TRANSACTION =>
            val tx = decodeTx(entry.getTransaction)
            updateWithChoicesFromTx(tx)

          case EntryCase.ARCHIVES =>
          // No work to do

          case EntryCase.ENTRY_NOT_SET =>
            sys.error("Decoding Error: Unexpected EntryCase.ENTRY_NOT_SET")
        }
      }
    } finally {
      inputStream.close()
    }

    result
  }

  def loadBenchmark(
      dumpFile: Path,
      choice: (Ref.QualifiedName, Ref.Name),
      index: Int,
      profileDir: Option[Path],
      contractIdVersion: ContractIdVersion,
      gasBudget: Option[Long] = None,
  ): TransactionSnapshot = {
    println(s"%%% loading submission entries from $dumpFile...")
    val inputStream = new BufferedInputStream(Files.newInputStream(dumpFile))

    val entries = new Iterator[Snapshot.SubmissionEntry] {
      override def hasNext: Boolean = (inputStream.available() != 0)
      override def next(): Snapshot.SubmissionEntry = {
        Snapshot.SubmissionEntry.parseDelimitedFrom(inputStream)
      }
    }

    var idx: Int = index
    var activeCreates = Map.empty[ContractId, Node.Create]
    var archives = List.empty[ByteString]
    var result = Option.empty[TransactionSnapshot]

    def toLocalContractId(coid: ContractId): ContractId = coid match {
      case ContractId.V1(discriminator, _) =>
        ContractId.V1(discriminator)

      case ContractId.V2(local, _) =>
        ContractId.V2(local, Bytes.Empty)
    }

    def decodeTx(txEntry: Snapshot.TransactionEntry): SubmittedTx = {
      val protoTx = TxOuterClass.Transaction.parseFrom(txEntry.getRawTransaction)
      TxCoder
        .decodeTransaction(protoTx)
        .fold(
          err => sys.error("Decoding Error: " + err.errorMessage),
          tx => SubmittedTx(tx.mapCid(toLocalContractId)),
        )
    }

    def matchingTx(tx: SubmittedTx) =
      tx.roots.iterator.map(tx.nodes).exists {
        case exe: Node.Exercise => (exe.templateId.qualifiedName, exe.choiceId) == choice
        case _ => false
      }

    def updateWithTx(tx: SubmittedTx): Unit =
      tx.foreachInExecutionOrder(
        exerciseBegin = { (_, exe) =>
          if (exe.consuming) activeCreates = activeCreates - exe.targetCoid
          ChildrenRecursion.DoRecurse
        },
        rollbackBegin = (_, _) => ChildrenRecursion.DoNotRecurse,
        leaf = {
          case (_, create: Node.Create) => activeCreates += (create.coid -> create)
          case (_, _) =>
        },
        exerciseEnd = (_, _) => (),
        rollbackEnd = (_, _) => (),
      )

    def updateWithArchive(archive: ByteString): Unit =
      archives = archive :: archives

    def buildSnapshot(
        txEntry: Snapshot.TransactionEntry,
        tx: SubmittedTx,
    ) = {
      val relevantCreateNodes = activeCreates.view.filterKeys(tx.inputContracts).toList
      val contracts = relevantCreateNodes.view.map { case (cid, create) =>
        val ledgerTime = Time.Timestamp.assertFromLong(txEntry.getLedgerTime)
        cid -> FatContractInstance.fromCreateNode(
          create,
          CreationTime.CreatedAt(ledgerTime),
          Bytes.Empty,
        )
      }.toMap
      val contractKeys = relevantCreateNodes.view.flatMap { case (cid, create) =>
        create.keyOpt.map(_ -> cid).toList
      }.toMap
      new TransactionSnapshot(
        transaction = tx,
        participantId = Ref.ParticipantId.assertFromString(txEntry.getParticipantId),
        submitters = txEntry.getSubmittersList
          .iterator()
          .asScala
          .map(Ref.Party.assertFromString)
          .toSet,
        ledgerTime = Time.Timestamp.assertFromLong(txEntry.getLedgerTime),
        preparationTime = Time.Timestamp.assertFromLong(txEntry.getPreparationTime),
        submissionSeed = crypto.Hash.assertFromBytes(
          Bytes.fromByteString(txEntry.getSubmissionSeed)
        ),
        contracts = contracts,
        contractKeys = contractKeys,
        pkgs = archives.view.map(ArchiveDecoder.assertFromByteString).toMap,
        profileDir = profileDir,
        contractIdVersion = contractIdVersion,
        gasBudget = gasBudget,
      )
    }

    try {
      while (result.isEmpty && entries.hasNext) {
        val entry = entries.next()
        entry.getEntryCase match {
          case EntryCase.TRANSACTION =>
            val tx = decodeTx(entry.getTransaction)
            if (matchingTx(tx)) {
              if (idx == 0) {
                result = Some(buildSnapshot(entry.getTransaction, tx))
              } else {
                idx -= 1
              }
            }
            updateWithTx(tx)

          case EntryCase.ARCHIVES =>
            updateWithArchive(entry.getArchives)

          case EntryCase.ENTRY_NOT_SET =>
            sys.error("Decoding Error: Unexpected EntryCase.ENTRY_NOT_SET")
        }
      }
    } finally {
      inputStream.close()
    }

    result match {
      case Some(value) => value
      case None => sys.error(s"choice ${choice._1}:${choice._2} not found")
    }
  }

}
