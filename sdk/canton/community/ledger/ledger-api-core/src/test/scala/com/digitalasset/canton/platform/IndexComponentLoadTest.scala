// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.api.v2.update_service.GetUpdateResponse
import com.digitalasset.canton
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, LedgerTimeBoundaries}
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  TemplateWildcardFilter,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
}
import com.digitalasset.canton.ledger.participant.state.{
  Reassignment,
  ReassignmentInfo,
  TestAcsChangeFactory,
  TransactionMeta,
  Update,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.protocol.{ReassignmentId, TestUpdateId, UpdateId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.transaction.{CommittedTransaction, Node}
import com.digitalasset.daml.lf.value.Value
import com.google.protobuf.ByteString
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.Ignore
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.Span

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.collection.mutable
import scala.concurrent.duration.{Duration, FiniteDuration}

/** Goal of this test is to provide a light-weight approach to ingest synthetic Index DB data for
  * load-testing, benchmarking purposes. This test is not supposed to run in CI (this is why it is
  * ignored, and logs on WARN log level).
  */
@Ignore
class IndexComponentLoadTest extends AnyFlatSpec with IndexComponentTest {

  override val jdbcUrl: String = s"jdbc:postgresql://localhost:5433/load_test?user=postgres"

  private val synchronizer1 = SynchronizerId.tryFromString("x::synchronizer1")
  private val synchronizer2 = SynchronizerId.tryFromString("x::synchronizer2")
  private val packageName: Ref.PackageName = Ref.PackageName.assertFromString("-package-name-")
  private val dsoParty = Ref.Party.assertFromString("dsoParty") // sees all
  private lazy val parties =
    (1 to 10000).view.map(index => Ref.Party.assertFromString(s"party$index")).toVector
  private lazy val templates =
    (1 to 300).view.map(index => Ref.Identifier.assertFromString(s"P:M:T$index")).toVector
  private val wildcardTemplates = CumulativeFilter(
    templateFilters = Set.empty,
    interfaceFilters = Set.empty,
    templateWildcardFilter = Some(TemplateWildcardFilter(includeCreatedEventBlob = false)),
  )
  private def eventFormat(party: Ref.Party) = EventFormat(
    filtersByParty = Map(
      party -> wildcardTemplates
    ),
    filtersForAnyParty = None,
    verbose = false,
  )
  private val allPartyEventFormat = EventFormat(
    filtersByParty = Map.empty,
    filtersForAnyParty = Some(wildcardTemplates),
    verbose = false,
  )
  private val someLFHash = com.digitalasset.daml.lf.crypto.Hash
    .assertFromString("01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086")
  override implicit val traceContext: TraceContext = TraceContext.createNew("load-test")

  private val builder = TxBuilder()
  private val testAcsChangeFactory = TestAcsChangeFactory()

  it should "Index assign/unassign updates" ignore {
    val nextRecordTime = nextRecordTimeFactory()
    logger.warn(s"start preparing updates...")
    val passes = 1
    val batchesPerPasses = 50 // this is doubled: first all assign batches then all unassign batches
    val eventsPerBatch = 10
    val allUpdates = 1
      .to(passes)
      .toVector
      .flatMap(_ =>
        allAssignsThenAllUnassigns(
          nextRecordTime = nextRecordTime,
          assignPayloadLength = 400,
          unassignPayloadLength = 150,
          batchSize = eventsPerBatch,
          batches = batchesPerPasses,
        )
      ) :+ assigns(nextRecordTime(), 400)(Vector(builder.newCid, builder.newCid))
    val allUpdateSize = allUpdates.size
    logger.warn(s"prepared $allUpdateSize updates")
    indexUpdates(allUpdates)
  }

  it should "Index CN ACS Export NFR fixture updates" ignore {
    val nextRecordTime: () => CantonTimestamp = nextRecordTimeFactory()
    def ingestionIteration(): Unit = {
      logger.warn(s"start preparing updates...")
      if (1 == "1".toInt)
        fail(
          "WARNING! Please check if you are really want to do this! The following parameters result in a fixture probably not fitting in the memory. Please verify parameters. Bigger workloads are possible with doing multiple iterations."
        )
      val passes = 4320
      val txSize = 5
      // 2000*5 = 10'000 contracts created then archived in a pass,
      // 10'000*4320 = 43'200'000 contracts created and archived at the end
      val txsCreatedAndArchivedPerPass = 2000
      // 23*5 = 115 contracts staying active per pass,
      // 115*4320 = 496'800 contracts staying alive at the end
      val txsStayingActivePerPass = 23
      val createPayloadLength = 300
      val archiveArgumentPayloadLengthFromTo = (13, 38)
      val archiveResultPayloadLengthFromTo = (13, 58)
      val allUpdates = (1 to passes).toVector
        .flatMap(_ =>
          createsAndArchives(
            nextRecordTime = nextRecordTime,
            txSize = txSize,
            txsCreatedThenArchived = txsCreatedAndArchivedPerPass,
            txsCreatedNotArchived = txsStayingActivePerPass,
            createPayloadLength = createPayloadLength,
            archiveArgumentPayloadLengthFromTo = archiveArgumentPayloadLengthFromTo,
            archiveResultPayloadLengthFromTo = archiveResultPayloadLengthFromTo,
          )
        )
      val allUpdateSize = allUpdates.size
      logger.warn(s"prepared $allUpdateSize updates")
      indexUpdates(allUpdates)
    }

    (1 to 1).foreach { i =>
      logger.warn(s"ingestion iteration: $i started")
      ingestionIteration()
      logger.warn(s"ingestion iteration: $i finished")
    }
  }

  it should "Fetch ACS" ignore TraceContext.withNewTraceContext("ACS fetch") {
    implicit traceContext =>
      val ledgerEndOffset = index.currentLedgerEnd().futureValue
      implicit val loggingContextWithTrace: LoggingContextWithTrace =
        LoggingContextWithTrace(loggerFactory)
      logger.warn("start fetching acs...")
      val startTime = System.currentTimeMillis()
      index
        .getActiveContracts(
          eventFormat = eventFormat(dsoParty),
          activeAt = ledgerEndOffset,
        )
        .zipWithIndex
        .runWith(Sink.last)
        .map { case (last, lastIndex) =>
          val totalMillis = System.currentTimeMillis() - startTime
          logger.warn(
            s"finished fetching acs in $totalMillis ms, ${lastIndex + 1} active contracts returned."
          )
          logger.warn(s"last active contract acs: $last")
        }
        .futureValue(
          PatienceConfiguration.Timeout(Span.convertDurationToSpan(Duration(2000, "seconds")))
        )
  }

  private def nextRecordTimeFactory(): () => CantonTimestamp = {
    logger.warn(s"looking up base record time")
    val ledgerEnd = index.currentLedgerEnd().futureValue
    val baseRecordTime: CantonTimestamp = ledgerEnd match {
      case Some(offset) =>
        // try to get the last one
        logger.warn(s"looks like ledger not empty, getting record time from last update")
        val lastUpdate: GetUpdateResponse.Update = index
          .getUpdateBy(
            LookupKey.ByOffset(offset),
            UpdateFormat(
              includeTransactions = Some(
                TransactionFormat(
                  eventFormat = allPartyEventFormat,
                  transactionShape = TransactionShape.LedgerEffects,
                )
              ),
              includeReassignments = Some(allPartyEventFormat),
              includeTopologyEvents = None,
            ),
          )
          .futureValue
          .value
          .update
        lastUpdate.reassignment
          .flatMap(_.recordTime)
          .orElse(lastUpdate.transaction.flatMap(_.recordTime))
          .map(CantonTimestamp.fromProtoTimestamp(_).value)
          .getOrElse(fail("On LedgerEnd a reassignment or transaction is expected"))
      case None =>
        // empty ledger getting now
        logger.warn(s"looks like ledger is empty, using now as a baseline record time")
        CantonTimestamp.now()
    }
    val recordTime = new AtomicReference(baseRecordTime)
    () => recordTime.updateAndGet(_.immediateSuccessor)
  }

  private def indexUpdates(updates: Vector[Update]): Unit = {
    val numOfUpdates = updates.size
    val startTime = System.currentTimeMillis()
    val state = new AtomicLong(0L)
    val ledgerEndLongBefore = index.currentLedgerEnd().futureValue.map(_.positive).getOrElse(0L)
    logger.warn(s"start ingesting $numOfUpdates updates...")
    val reportingSeconds = 5
    val reporter = system.scheduler.scheduleAtFixedRate(
      initialDelay = FiniteDuration(reportingSeconds, "seconds"),
      interval = FiniteDuration(reportingSeconds, "seconds"),
    )(new Runnable {
      val lastState = new AtomicLong(0L)
      override def run(): Unit = {
        val current = state.get()
        val last = lastState.get()
        lastState.set(current)
        val reportRate = (current - last) / reportingSeconds
        val avgRate = current * 1000 / (System.currentTimeMillis() - startTime)
        val minutesLeft = (numOfUpdates - current) / avgRate / 60
        logger.warn(
          s"ingesting $current/$numOfUpdates, ${100 * current / numOfUpdates}% (since last: ${current - last}, $reportRate update/seconds) (avg: $avgRate update/seconds, estimated minutes left: $minutesLeft)..."
        )
      }
    })
    Source
      .fromIterator(() => updates.iterator)
      .async
      .mapAsync(1)(ingestUpdateAsync)
      .async
      .foreach(_ => state.incrementAndGet())
      .run()
      .map { _ =>
        reporter.cancel()
        logger.warn(
          s"finished pushing $numOfUpdates updates to indexer, waiting for all to be indexed..."
        )
        eventually(
          timeUntilSuccess = FiniteDuration(1000, "seconds"),
          maxPollInterval = FiniteDuration(100, "milliseconds"),
        ) {
          (index
            .currentLedgerEnd()
            .futureValue
            .map(_.positive)
            .getOrElse(0L) - ledgerEndLongBefore) shouldBe numOfUpdates
        }
        val avgRate = numOfUpdates * 1000 / (System.currentTimeMillis() - startTime)
        logger.warn(
          s"finished ingesting $numOfUpdates updates with average rate $avgRate updates/second"
        )
      }
      .futureValue(
        PatienceConfiguration.Timeout(Span.convertDurationToSpan(Duration(200000, "seconds")))
      )
  }

  private val random = new scala.util.Random
  private def randomString(length: Int) = {
    val sb = new mutable.StringBuilder()
    for (i <- 1 to length) {
      sb.append(random.alphanumeric.head)
    }
    sb.toString
  }

  private def randomTemplate = templates(random.nextInt(templates.size))
  private def randomParty = parties(random.nextInt(parties.size))
  private def randomHash: Hash = Hash.digest(
    HashPurpose.PreparedSubmission,
    ByteString.copyFromUtf8(s"${random.nextLong()}"),
    Sha256,
  )
  private def randomUpdateId: UpdateId = TestUpdateId(randomHash.toHexString)
  private def randomLength(lengthFromToInclusive: (Int, Int)) = {
    val (from, to) = lengthFromToInclusive
    val randomDistance = to - from + 1
    assert(randomDistance > 1)
    from + random.nextInt(randomDistance)
  }

  private def allAssignsThenAllUnassigns(
      nextRecordTime: () => CantonTimestamp,
      assignPayloadLength: Int,
      unassignPayloadLength: Int,
      batchSize: Int,
      batches: Int,
  ): Vector[Update.SequencedReassignmentAccepted] = {
    val cidBatches =
      1.to(batches).map(_ => 1.to(batchSize).map(_ => builder.newCid).toVector).toVector
    cidBatches
      .map(cids => assigns(nextRecordTime(), assignPayloadLength)(cids))
      .++(cidBatches.map(cids => unassigns(nextRecordTime(), unassignPayloadLength)(cids)))
  }

  private def createsAndArchives(
      nextRecordTime: () => CantonTimestamp,
      txSize: Int,
      txsCreatedThenArchived: Int,
      txsCreatedNotArchived: Int,
      createPayloadLength: Int,
      archiveArgumentPayloadLengthFromTo: (Int, Int),
      archiveResultPayloadLengthFromTo: (Int, Int),
  ): Vector[Update.SequencedTransactionAccepted] = {
    val (createTxs, createNodes) =
      (1 to txsCreatedThenArchived + txsCreatedNotArchived).iterator
        .map(_ =>
          creates(
            recordTime = nextRecordTime,
            payloadLength = createPayloadLength,
          )(
            (1 to txSize).map(_ => builder.newCid).toVector
          )
        )
        .toVector
        .unzip
    val archivingTxs = createNodes.iterator
      .take(txsCreatedThenArchived)
      .map(
        archives(
          recordTime = nextRecordTime,
          argumentLength = randomLength(archiveArgumentPayloadLengthFromTo),
          resultLength = randomLength(archiveResultPayloadLengthFromTo),
        )
      )
      .toVector
    createTxs ++ archivingTxs
  }

  private def assigns(recordTime: CantonTimestamp, payloadLength: Int)(
      coids: Seq[ContractId]
  ): Update.SequencedReassignmentAccepted =
    reassignment(
      sourceSynchronizerId = synchronizer2,
      targetSynchronizerId = synchronizer1,
      synchronizerId = synchronizer1,
      recordTime = recordTime,
      workflowId = None,
    )(coids.zipWithIndex.map { case (coid, index) =>
      assign(
        coid = coid,
        nodeId = index,
        ledgerEffectiveTime = recordTime.underlying,
        argumentPayload = randomString(payloadLength),
      )
    })

  private def unassigns(recordTime: CantonTimestamp, payloadLength: Int)(
      coids: Seq[ContractId]
  ): Update.SequencedReassignmentAccepted =
    reassignment(
      sourceSynchronizerId = synchronizer1,
      targetSynchronizerId = synchronizer2,
      synchronizerId = synchronizer1,
      recordTime = recordTime,
      workflowId = Some(
        WorkflowId.assertFromString(randomString(payloadLength))
      ), // mimick unassign payload with workflowID. This is also stored with all events.
    )(coids.zipWithIndex.map { case (coid, index) =>
      unassign(
        coid = coid,
        nodeId = index,
      )
    })

  private def creates(recordTime: () => CantonTimestamp, payloadLength: Int)(
      coids: Seq[ContractId]
  ): (Update.SequencedTransactionAccepted, Vector[Create]) = {
    val txBuilder = TxBuilder()
    val creates = coids.iterator
      .map(coid =>
        create(
          coid = coid,
          argumentPayload = randomString(payloadLength),
          template = randomTemplate,
          signatories = Set(
            dsoParty,
            randomParty,
            randomParty,
            randomParty,
          ),
        )
      )
      .toVector
    creates.foreach(txBuilder.add)
    val tx = txBuilder.buildCommitted()
    val contractAuthenticationData = coids.iterator
      .map(
        _ -> Bytes.fromByteString(ByteString.copyFromUtf8(randomString(42)))
      )
      .toMap
    transaction(
      synchronizerId = synchronizer1,
      recordTime = recordTime(),
    )(tx, contractAuthenticationData) -> creates
  }

  private def archives(
      recordTime: () => CantonTimestamp,
      argumentLength: Int,
      resultLength: Int,
  )(
      creates: Seq[Node.Create]
  ): Update.SequencedTransactionAccepted = {
    val txBuilder = TxBuilder()
    val archives = creates.iterator
      .map(create =>
        archive(
          create = create,
          actingParties = Set(
            randomParty,
            randomParty,
            randomParty,
          ),
          argumentPayload = randomString(argumentLength),
          resultPayload = randomString(resultLength),
        )
      )
      .toVector
    archives.foreach(txBuilder.add)
    val tx = txBuilder.buildCommitted()
    transaction(
      synchronizerId = synchronizer1,
      recordTime = recordTime(),
    )(tx)
  }

  private def create(
      coid: ContractId,
      argumentPayload: String,
      template: Ref.Identifier,
      signatories: Set[Party],
  ): canton.platform.Create =
    builder.create(
      id = coid,
      templateId = template,
      argument = Value.ValueRecord(
        tycon = None,
        fields = ImmArray(None -> Value.ValueText(argumentPayload)),
      ),
      signatories = signatories,
      observers = Set.empty,
      packageName = packageName,
    )

  private def archive(
      create: Node.Create,
      actingParties: Set[Ref.Party],
      argumentPayload: String,
      resultPayload: String,
  ): platform.Exercise =
    builder.exercise(
      contract = create,
      choice = Ref.Name.assertFromString("archivingarchivingarchivingarchivingarchivingarchiving"),
      consuming = true,
      actingParties = actingParties,
      argument = Value.ValueRecord(
        tycon = None,
        fields = ImmArray(None -> Value.ValueText(argumentPayload)),
      ),
      byKey = false,
      interfaceId = None,
      result = Some(
        Value.ValueRecord(
          tycon = None,
          fields = ImmArray(None -> Value.ValueText(resultPayload)),
        )
      ),
    )

  private def assign(
      coid: ContractId,
      nodeId: Int,
      ledgerEffectiveTime: Time.Timestamp,
      argumentPayload: String,
  ): Reassignment.Assign =
    Reassignment.Assign(
      ledgerEffectiveTime = ledgerEffectiveTime,
      createNode = create(
        coid = coid,
        argumentPayload = argumentPayload,
        template = templates(0),
        signatories = Set(dsoParty),
      ),
      contractAuthenticationData = Bytes.Empty,
      reassignmentCounter = 10L,
      nodeId = nodeId,
    )

  private def unassign(
      coid: ContractId,
      nodeId: Int,
  ): Reassignment.Unassign =
    Reassignment.Unassign(
      contractId = coid,
      templateId = templates(0),
      packageName = packageName,
      stakeholders = Set(dsoParty),
      assignmentExclusivity = None,
      reassignmentCounter = 11L,
      nodeId = nodeId,
    )

  private def reassignment(
      sourceSynchronizerId: SynchronizerId,
      targetSynchronizerId: SynchronizerId,
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      workflowId: Option[WorkflowId],
  )(reassignments: Seq[Reassignment]): Update.SequencedReassignmentAccepted =
    Update.SequencedReassignmentAccepted(
      optCompletionInfo = None,
      workflowId = workflowId,
      updateId = randomUpdateId,
      reassignmentInfo = ReassignmentInfo(
        sourceSynchronizer = ReassignmentTag.Source(sourceSynchronizerId),
        targetSynchronizer = ReassignmentTag.Target(targetSynchronizerId),
        submitter = Some(dsoParty),
        reassignmentId = ReassignmentId.tryCreate("000123"),
        isReassigningParticipant = false,
      ),
      reassignment = Reassignment.Batch(reassignments.head, reassignments.tail*),
      recordTime = recordTime,
      synchronizerId = synchronizerId,
      acsChangeFactory = testAcsChangeFactory,
      internalContractIds = Map.empty,
    )

  private def transaction(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(
      transaction: CommittedTransaction,
      contractAuthenticationData: Map[ContractId, Bytes] = Map.empty,
  ): Update.SequencedTransactionAccepted =
    Update.SequencedTransactionAccepted(
      completionInfoO = None,
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = recordTime.underlying,
        workflowId = None,
        preparationTime = recordTime.underlying,
        submissionSeed = someLFHash,
        timeBoundaries = LedgerTimeBoundaries.unconstrained,
        optUsedPackages = None,
        optNodeSeeds = None,
        optByKeyNodes = None,
      ),
      transaction = transaction,
      updateId = randomUpdateId,
      contractAuthenticationData = contractAuthenticationData,
      synchronizerId = synchronizerId,
      recordTime = recordTime,
      acsChangeFactory = testAcsChangeFactory,
      externalTransactionHash = None,
      internalContractIds = Map.empty,
    )
}
