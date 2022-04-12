// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package testing.snapshot

import com.daml.lf.archive.{ArchiveDecoder, UniversalArchiveDecoder}
import com.daml.lf.data.{Bytes, Ref, Time}
import com.daml.lf.engine.{Engine, EngineConfig, Error}
import com.daml.lf.language.{Ast, LanguageVersion, Util => AstUtil}
import com.daml.lf.testing.snapshot.Snapshot.SubmissionEntry.EntryCase
import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  SubmittedTransaction => SubmittedTx,
  TransactionCoder => TxCoder,
  TransactionOuterClass => TxOuterClass,
}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.{Value, ValueCoder}
import com.daml.logging.LoggingContext
import com.google.protobuf.ByteString

import java.io.BufferedInputStream
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

final case class TransactionSnapshot(
    transaction: SubmittedTx,
    participantId: Ref.ParticipantId,
    submitters: Set[Ref.Party],
    ledgerTime: Time.Timestamp,
    submissionTime: Time.Timestamp,
    submissionSeed: crypto.Hash,
    contracts: Map[ContractId, Value.VersionedContractInstance],
    contractKeys: Map[GlobalKeyWithMaintainers, ContractId],
    pkgs: Map[Ref.PackageId, Ast.Package],
    profileDir: Option[Path] = None,
) {

  private[this] implicit def loggingContext: LoggingContext = LoggingContext.ForTesting

  private[this] lazy val engine = TransactionSnapshot.compile(pkgs, profileDir)

  def replay(): Either[Error, Unit] =
    engine
      .replay(
        submitters,
        transaction,
        ledgerTime,
        participantId,
        submissionTime,
        submissionSeed,
      )
      .consume(contracts.get, pkgs.get, contractKeys.get)
      .map(_ => ())

  def validate(): Either[Error, Unit] =
    engine
      .validate(
        submitters,
        transaction,
        ledgerTime,
        participantId,
        submissionTime,
        submissionSeed,
      )
      .consume(contracts.get, pkgs.get, contractKeys.get)

  def adapt(pkgs: Map[Ref.PackageId, Ast.Package]): TransactionSnapshot = {
    val adapter = new Adapter(pkgs)
    this.copy(
      transaction = adapter.adapt(transaction),
      contracts = contracts.transform((_, v) => adapter.adapt(v)),
      contractKeys = contractKeys.iterator.map { case (k, v) => adapter.adapt(k) -> v }.toMap,
      pkgs = pkgs,
    )
  }

}

private[snapshot] object TransactionSnapshot {

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

  def loadBenchmark(
      dumpFile: Path,
      choice: (Ref.QualifiedName, Ref.Name),
      index: Int,
      profileDir: Option[Path],
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

    def decodeTx(txEntry: Snapshot.TransactionEntry) = {
      val protoTx = TxOuterClass.Transaction.parseFrom(txEntry.getRawTransaction)
      TxCoder
        .decodeTransaction(TxCoder.NidDecoder, ValueCoder.CidDecoder, protoTx)
        .fold(
          err => sys.error("Decoding Error: " + err.errorMessage),
          SubmittedTx(_),
        )
    }

    def matchingTx(tx: SubmittedTx) =
      tx.roots.iterator.map(tx.nodes).exists {
        case exe: Node.Exercise => (exe.templateId.qualifiedName, exe.choiceId) == choice
        case _ => false
      }

    def updateWithTx(tx: SubmittedTx) =
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

    def updateWithArchive(archive: ByteString) =
      archives = archive :: archives

    def buildSnapshot(
        txEntry: Snapshot.TransactionEntry,
        tx: SubmittedTx,
    ) = {
      val relevantCreateNodes = activeCreates.view.filterKeys(tx.inputContracts).toList
      val contracts = relevantCreateNodes.view.map { case (cid, create) =>
        cid -> create.versionedCoinst
      }.toMap
      val contractKeys = relevantCreateNodes.view.flatMap { case (cid, create) =>
        create.key.map { case Node.KeyWithMaintainers(key, maintainers) =>
          GlobalKeyWithMaintainers(
            GlobalKey.assertBuild(create.templateId, key),
            maintainers,
          ) -> cid
        }.toList
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
        submissionTime = Time.Timestamp.assertFromLong(txEntry.getSubmissionTime),
        submissionSeed = crypto.Hash.assertFromBytes(
          Bytes.fromByteString(txEntry.getSubmissionSeed)
        ),
        contracts = contracts,
        contractKeys = contractKeys,
        pkgs = archives.view.map(ArchiveDecoder.assertFromByteString).toMap,
        profileDir = profileDir,
      )
    }

    try {
      while (result.isEmpty && entries.hasNext) {
        val entry = entries.next()
        entry.getEntryCase match {
          case EntryCase.TRANSACTION =>
            val tx = decodeTx(entry.getTransaction)
            if (matchingTx(tx))
              if (idx == 0) result = Some(buildSnapshot(entry.getTransaction, tx))
              else idx -= 1
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
