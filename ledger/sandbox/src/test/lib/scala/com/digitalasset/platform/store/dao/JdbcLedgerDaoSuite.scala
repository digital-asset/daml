// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import java.io.File
import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicLong

import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1.{Configuration, Offset, TimeModel}
import com.digitalasset.daml.bazeltools.BazelRunfiles.rlocation
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref.{Identifier, Party}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractInst,
  NodeId,
  ValueRecord,
  ValueText,
  ValueUnit,
  VersionedValue
}
import com.digitalasset.daml.lf.value.ValueVersions
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.ledger.{EventId, TransactionId}
import com.digitalasset.platform.events.EventIdFormatter
import com.digitalasset.platform.store.PersistenceEntry
import com.digitalasset.platform.store.entries.LedgerEntry
import org.scalatest.Suite

import scala.collection.immutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Success, Try}

private[dao] trait JdbcLedgerDaoSuite extends AkkaBeforeAndAfterAll with JdbcLedgerDaoBackend {
  this: Suite =>

  protected final val nextOffset: () => Offset = {
    val base = BigInt(1) << 32
    val counter = new AtomicLong(0)
    () =>
      Offset.fromBytes((base + counter.getAndIncrement()).toByteArray)
  }

  protected final implicit class OffsetToLong(offset: Offset) {
    def toLong: Long = BigInt(offset.toByteArray).toLong
  }

  protected final val alice = Party.assertFromString("Alice")
  protected final val bob = Party.assertFromString("Bob")
  protected final val charlie = Party.assertFromString("Charlie")

  protected final val someAgreement = "agreement"
  protected final val someTemplateId = Identifier(
    Ref.PackageId.assertFromString("packageId"),
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString("moduleName"),
      Ref.DottedName.assertFromString("someTemplate"))
  )
  protected final val someRecordId = Identifier(
    Ref.PackageId.assertFromString("packageId"),
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString("moduleName"),
      Ref.DottedName.assertFromString("someRecord"))
  )
  protected final val someValueText = ValueText("some text")
  protected final val someValueRecord = ValueRecord(
    Some(someRecordId),
    ImmArray(Some(Ref.Name.assertFromString("field")) -> someValueText))
  protected final val someContractKey =
    VersionedValue(ValueVersions.acceptedVersions.head, someValueText)
  protected final val someContractInstance = ContractInst(
    someTemplateId,
    VersionedValue(ValueVersions.acceptedVersions.head, someValueText),
    someAgreement
  )

  protected final val defaultConfig = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault,
    Duration.ofDays(1),
  )

  private val reader = DarReader { (_, stream) =>
    Try(DamlLf.Archive.parseFrom(stream))
  }
  private val Success(dar) =
    reader.readArchiveFromFile(new File(rlocation("ledger/test-common/Test-stable.dar")))
  private val now = Instant.now()

  protected final val packages: List[(DamlLf.Archive, v2.PackageDetails)] =
    dar.all.map(dar => dar -> v2.PackageDetails(dar.getSerializedSize.toLong, now, None))

  protected implicit def toParty(s: String): Ref.Party = Ref.Party.assertFromString(s)

  protected implicit def toLedgerString(s: String): Ref.LedgerString =
    Ref.LedgerString.assertFromString(s)

  protected final def event(txid: TransactionId, idx: Long): EventId =
    EventIdFormatter.fromTransactionId(txid, NodeId(idx.toInt))

  private def genCreateTransaction(offset: Offset): LedgerEntry.Transaction = {
    val id = offset.toLong
    val txId = s"trId$id"
    val absCid = AbsoluteContractId(s"cId$id")
    val let = Instant.now

    LedgerEntry.Transaction(
      Some(s"commandId$id"),
      txId,
      Some("appID1"),
      Some("Alice"),
      Some("workflowId"),
      let,
      let,
      GenTransaction(
        HashMap(
          event(txId, id) -> NodeCreate(
            nodeSeed = None,
            coid = absCid,
            coinst = someContractInstance,
            optLocation = None,
            signatories = Set(alice, bob),
            stakeholders = Set(alice, bob),
            key = None
          )),
        ImmArray(event(txId, id)),
      ),
      Map(event(txId, id) -> Set("Alice", "Bob"))
    )
  }

  protected final def storeCreateTransaction()(implicit ec: ExecutionContext): Future[Unit] = {
    val offset = nextOffset()
    val t = genCreateTransaction(offset)
    ledgerDao
      .storeLedgerEntry(offset, PersistenceEntry.Transaction(t, Map.empty, List.empty))
      .map(_ => ())
  }

  private def genExerciseTransaction(
      offset: Offset,
      targetCid: AbsoluteContractId): LedgerEntry.Transaction = {
    val id = offset.toLong
    val txId = s"trId$id"
    val let = Instant.now
    LedgerEntry.Transaction(
      Some(s"commandId$id"),
      txId,
      Some("appID1"),
      Some("Alice"),
      Some("workflowId"),
      let,
      let,
      GenTransaction(
        HashMap(
          event(txId, id) -> NodeExercises(
            nodeSeed = None,
            targetCoid = targetCid,
            templateId = someTemplateId,
            choiceId = Ref.Name.assertFromString("choice"),
            optLocation = None,
            consuming = true,
            actingParties = Set(alice),
            chosenValue =
              VersionedValue(ValueVersions.acceptedVersions.head, ValueText("some choice value")),
            stakeholders = Set(alice, bob),
            signatories = Set(alice, bob),
            children = ImmArray.empty,
            exerciseResult = Some(
              VersionedValue(
                ValueVersions.acceptedVersions.head,
                ValueText("some exercise result"))),
            key = None
          )),
        ImmArray(event(txId, id)),
      ),
      Map(event(txId, id) -> Set("Alice", "Bob"))
    )
  }

  protected final def storeExerciseTransaction(targetCid: AbsoluteContractId)(
      implicit ec: ExecutionContext): Future[Unit] = {
    val offset = nextOffset()
    val t = genExerciseTransaction(offset, targetCid)
    ledgerDao
      .storeLedgerEntry(offset, PersistenceEntry.Transaction(t, Map.empty, List.empty))
      .map(_ => ())
  }

  /** A transaction that creates the given key */
  protected final def txCreateContractWithKey(
      let: Instant,
      offset: Offset,
      party: Party,
      key: String): PersistenceEntry.Transaction = {
    val id = offset.toLong
    PersistenceEntry.Transaction(
      LedgerEntry.Transaction(
        Some(s"commandId$id"),
        s"transactionId$id",
        Some("applicationId"),
        Some(party),
        Some("workflowId"),
        let,
        let,
        GenTransaction(
          HashMap(
            event(s"transactionId$id", id) -> NodeCreate(
              nodeSeed = None,
              coid = AbsoluteContractId(s"contractId$id"),
              coinst = someContractInstance,
              optLocation = None,
              signatories = Set(party),
              stakeholders = Set(party),
              key = Some(
                KeyWithMaintainers(
                  VersionedValue(ValueVersions.acceptedVersions.head, ValueText(key)),
                  Set(party)))
            )),
          ImmArray(event(s"transactionId$id", id)),
        ),
        Map(event(s"transactionId$id", id) -> Set(party))
      ),
      Map.empty,
      List.empty
    )
  }

  /** A transaction that archives the given contract with the given key */
  protected final def txArchiveContract(
      let: Instant,
      offset: Offset,
      party: Party,
      cid: Offset,
      key: String): PersistenceEntry.Transaction = {
    val id = offset.toLong
    PersistenceEntry.Transaction(
      LedgerEntry.Transaction(
        Some(s"commandId$id"),
        s"transactionId$id",
        Some("applicationId"),
        Some(party),
        Some("workflowId"),
        let,
        let,
        GenTransaction(
          HashMap(
            event(s"transactionId$id", id) -> NodeExercises(
              nodeSeed = None,
              targetCoid = AbsoluteContractId(s"contractId${cid.toLong}"),
              templateId = someTemplateId,
              choiceId = Ref.ChoiceName.assertFromString("Archive"),
              optLocation = None,
              consuming = true,
              actingParties = Set(party),
              chosenValue = VersionedValue(ValueVersions.acceptedVersions.head, ValueUnit),
              stakeholders = Set(party),
              signatories = Set(party),
              controllers = Set(party),
              children = ImmArray.empty,
              exerciseResult = Some(VersionedValue(ValueVersions.acceptedVersions.head, ValueUnit)),
              key = Some(
                KeyWithMaintainers(
                  VersionedValue(ValueVersions.acceptedVersions.head, ValueText(key)),
                  Set(party)))
            )),
          ImmArray(event(s"transactionId$id", id)),
        ),
        Map(event(s"transactionId$id", id) -> Set(party))
      ),
      Map.empty,
      List.empty
    )
  }

  /** A transaction that looks up a key */
  protected final def txLookupByKey(
      let: Instant,
      offset: Offset,
      party: Party,
      key: String,
      result: Option[Offset]): PersistenceEntry.Transaction = {
    val id = offset.toLong
    PersistenceEntry.Transaction(
      LedgerEntry.Transaction(
        Some(s"commandId$id"),
        s"transactionId$id",
        Some("applicationId"),
        Some(party),
        Some("workflowId"),
        let,
        let,
        GenTransaction(
          HashMap(
            event(s"transactionId$id", id) -> NodeLookupByKey(
              someTemplateId,
              None,
              KeyWithMaintainers(
                VersionedValue(ValueVersions.acceptedVersions.head, ValueText(key)),
                Set(party)),
              result.map(id => AbsoluteContractId(s"contractId${id.toLong}")),
            )),
          ImmArray(event(s"transactionId$id", id)),
        ),
        Map(event(s"transactionId$id", id) -> Set(party))
      ),
      Map.empty,
      List.empty
    )
  }

  /** A transaction that fetches a contract Id */
  protected final def txFetch(
      let: Instant,
      offset: Offset,
      party: Party,
      cid: Offset): PersistenceEntry.Transaction = {
    val id = offset.toLong
    PersistenceEntry.Transaction(
      LedgerEntry.Transaction(
        Some(s"commandId$id"),
        s"transactionId$id",
        Some("applicationId"),
        Some(party),
        Some("workflowId"),
        let,
        let,
        GenTransaction(
          HashMap(
            event(s"transactionId$id", id) -> NodeFetch(
              coid = AbsoluteContractId(s"contractId${cid.toLong}"),
              templateId = someTemplateId,
              optLocation = None,
              actingParties = Some(Set(party)),
              signatories = Set(party),
              stakeholders = Set(party),
            )),
          ImmArray(event(s"transactionId$id", id)),
        ),
        Map(event(s"transactionId$id", id) -> Set(party))
      ),
      Map.empty,
      List.empty
    )
  }

  protected final def emptyTxWithDivulgedContracts(
      offset: Offset,
      let: Instant,
  ): PersistenceEntry.Transaction = {
    val id = offset.toLong
    PersistenceEntry.Transaction(
      LedgerEntry.Transaction(
        Some(s"commandId$id"),
        s"transactionId$id",
        Some("applicationId"),
        Some(alice),
        Some("workflowId"),
        let,
        let,
        GenTransaction(HashMap.empty, ImmArray.empty),
        Map.empty
      ),
      Map(AbsoluteContractId(s"contractId$id") -> Set(bob)),
      List(AbsoluteContractId(s"contractId$id") -> someContractInstance)
    )
  }

}
