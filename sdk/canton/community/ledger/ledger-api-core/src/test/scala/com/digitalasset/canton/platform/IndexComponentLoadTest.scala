// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  TemplateWildcardFilter,
  UpdateFormat,
}
import com.digitalasset.canton.ledger.participant.state.{
  Reassignment,
  ReassignmentInfo,
  TestAcsChangeFactory,
  Update,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.value.Value
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.Ignore
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.Span

import java.util.UUID
import scala.concurrent.duration.Duration

@Ignore
class IndexComponentLoadTest extends AnyFlatSpec with IndexComponentTest {

  override val jdbcUrl: String = s"jdbc:postgresql://localhost:5433/load_test?user=postgres"

  private val synchronizer1 = SynchronizerId.tryFromString("x::synchronizer1")
  private val synchronizer2 = SynchronizerId.tryFromString("x::synchronizer2")
  private val templateId = Ref.Identifier.assertFromString("P:M:T")
  private val packageName: Ref.PackageName = Ref.PackageName.assertFromString("-package-name-")
  private val party = Ref.Party.assertFromString("party1")
  private val eventFormat = EventFormat(
    filtersByParty = Map(
      party -> CumulativeFilter(
        templateFilters = Set.empty,
        interfaceFilters = Set.empty,
        templateWildcardFilter = Some(TemplateWildcardFilter(includeCreatedEventBlob = false)),
      )
    ),
    filtersForAnyParty = None,
    verbose = false,
  )

  private val builder = TxBuilder()
  private val testAcsChangeFactory = TestAcsChangeFactory()

  it should "Index updates" ignore {
    logger.warn(s"start preparing updates...")
    val ledgerEnd = index.currentLedgerEnd().futureValue
    val baseRecordTime: CantonTimestamp = ledgerEnd match {
      case Some(offset) =>
        // try to get the last one
        logger.warn(s"looks like ledger not empty, getting record time from last update")
        CantonTimestamp
          .fromProtoTimestamp(
            index
              .getUpdateBy(
                LookupKey.Offset(offset),
                UpdateFormat(
                  includeTransactions = None,
                  includeReassignments = Some(eventFormat),
                  includeTopologyEvents = None,
                ),
              )
              .futureValue
              .value
              .getReassignment
              .recordTime
              .value
          )
          .value
      case None =>
        // empty ledger getting now
        CantonTimestamp.now()
    }
    var recordTime = baseRecordTime
    def nextRecordTime: CantonTimestamp = {
      recordTime = recordTime.immediateSuccessor
      recordTime
    }
    val passes = 1000
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
      ) :+ assigns(nextRecordTime, 400)(Vector(builder.newCid, builder.newCid))
    val allUpdateSize = allUpdates.size
    logger.warn(s"prepared $allUpdateSize updates")

    val startTime = System.currentTimeMillis()
    var lastReportedTime = startTime
    var lastReportedIngested = 0
    var ingestedPercent = 0
    val ledgerEndLongBefore = ledgerEnd.map(_.positive).getOrElse(0L)
    logger.warn(s"start ingesting $allUpdateSize updates...")
    allUpdates.iterator.zipWithIndex.foreach { case (update, index) =>
      ingestUpdateAsync(update)
      val newIngestedPercent = 100 * index / allUpdateSize
      if (newIngestedPercent > ingestedPercent) {
        val newTime = System.currentTimeMillis()
        val reportRate =
          if (newTime == lastReportedTime) 0
          else (index - lastReportedIngested) * 1000 / (newTime - lastReportedTime)
        logger.warn(s"ingesting ${100 * index / allUpdateSize}% ($reportRate update/seconds)...")
        ingestedPercent = newIngestedPercent
        lastReportedTime = newTime
        lastReportedIngested = index
      }
    }
    logger.warn(
      s"finished pushing $allUpdateSize updates to indexer, waiting for all to be indexed..."
    )
    eventually() {
      (index
        .currentLedgerEnd()
        .futureValue
        .map(_.positive)
        .getOrElse(0L) - ledgerEndLongBefore) shouldBe allUpdateSize
    }
    val avgRate = allUpdateSize * 1000 / (System.currentTimeMillis() - startTime)
    logger.warn(
      s"finished ingesting $allUpdateSize updates with average rate $avgRate updates/second"
    )
  }

  it should "Fetch ACS" in TraceContext.withNewTraceContext("ACS fetch") { implicit traceContext =>
    val ledgerEndOffset = index.currentLedgerEnd().futureValue
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)
    logger.warn("start fetching acs...")
    val startTime = System.currentTimeMillis()
    val results = index
      .getActiveContracts(
        eventFormat = eventFormat,
        activeAt = ledgerEndOffset,
      )
      .runWith(Sink.collection[GetActiveContractsResponse, Vector[GetActiveContractsResponse]])
      .futureValue(
        PatienceConfiguration.Timeout(Span.convertDurationToSpan(Duration(200, "seconds")))
      )
    val totalMillis = System.currentTimeMillis() - startTime
    logger.warn(s"finished fetching acs in $totalMillis ms")
    logger.warn(s"acs: \n${results.take(1000).mkString("\n")}")
  }

  private def allAssignsThenAllUnassigns(
      nextRecordTime: => CantonTimestamp,
      assignPayloadLength: Int,
      unassignPayloadLength: Int,
      batchSize: Int,
      batches: Int,
  ): Vector[Update.SequencedReassignmentAccepted] = {
    val cidBatches =
      1.to(batches).map(_ => 1.to(batchSize).map(_ => builder.newCid).toVector).toVector
    cidBatches
      .map(cids => assigns(nextRecordTime, assignPayloadLength)(cids))
      .++(cidBatches.map(cids => unassigns(nextRecordTime, unassignPayloadLength)(cids)))
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

  private val random = new scala.util.Random
  private def randomString(length: Int) = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      sb.append(random.alphanumeric.head)
    }
    sb.toString
  }

  private def assign(
      coid: ContractId,
      nodeId: Int,
      ledgerEffectiveTime: Time.Timestamp,
      argumentPayload: String,
  ): Reassignment.Assign = {
    val createNode = builder
      .create(
        id = coid,
        templateId = templateId,
        argument = Value.ValueRecord(
          tycon = None,
          fields = ImmArray(None -> Value.ValueText(argumentPayload)),
        ),
        signatories = Set(party),
        observers = Set.empty,
        packageName = packageName,
      )
    Reassignment.Assign(
      ledgerEffectiveTime = ledgerEffectiveTime,
      createNode = createNode,
      contractAuthenticationData = Bytes.Empty,
      reassignmentCounter = 10L,
      nodeId = nodeId,
    )
  }

  private def unassign(
      coid: ContractId,
      nodeId: Int,
  ): Reassignment.Unassign =
    Reassignment.Unassign(
      contractId = coid,
      templateId = templateId,
      packageName = packageName,
      stakeholders = Set(party),
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
      updateId = Ref.TransactionId.assertFromString(
        UUID.randomUUID().toString.appendedAll(UUID.randomUUID().toString).substring(0, 67)
      ),
      reassignmentInfo = ReassignmentInfo(
        sourceSynchronizer = ReassignmentTag.Source(sourceSynchronizerId),
        targetSynchronizer = ReassignmentTag.Target(targetSynchronizerId),
        submitter = Some(party),
        reassignmentId = ReassignmentId.tryCreate("000123"),
        isReassigningParticipant = false,
      ),
      reassignment = Reassignment.Batch(reassignments.head, reassignments.tail*),
      recordTime = recordTime,
      synchronizerId = synchronizerId,
      acsChangeFactory = testAcsChangeFactory,
    )
}
