// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.logging.entries.LoggingEntries
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, SuppressionRule}
import com.digitalasset.canton.platform.store.backend.StorageBackendTestValues.{
  offset,
  someIdentityParams,
}
import com.digitalasset.canton.platform.store.dao.events.QueryValidRangeImpl
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.StatusRuntimeException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

private[backend] trait StorageBackendTestsQueryValidRange extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  implicit val loggingContextWithTrace: LoggingContextWithTrace =
    new LoggingContextWithTrace(LoggingEntries.empty, TraceContext.empty)

  implicit val ec: ExecutionContext = directExecutionContext

  behavior of "QueryValidRange.withRangeNotPruned"

  it should "allow valid range if no pruning and before ledger end" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(None))
    executeSql(updateLedgerEnd(offset(10), 10L))
    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).withRangeNotPruned(
      minOffsetInclusive = offset(3),
      maxOffsetInclusive = offset(8),
      errorPruning = _ => "",
      errorLedgerEnd = _ => "",
    )(Future.unit)
      .futureValue
  }

  it should "allow valid range if no pruning and before ledger end and start from ledger begin" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(None))
    executeSql(updateLedgerEnd(offset(10), 10L))
    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).withRangeNotPruned(
      minOffsetInclusive = Offset.firstOffset,
      maxOffsetInclusive = offset(8),
      errorPruning = _ => "",
      errorLedgerEnd = _ => "",
    )(Future.unit)
      .futureValue
  }

  it should "allow valid range after pruning and before ledger end" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).withRangeNotPruned(
      minOffsetInclusive = offset(6),
      maxOffsetInclusive = offset(8),
      errorPruning = _ => "",
      errorLedgerEnd = _ => "",
    )(Future.unit)
      .futureValue
  }

  it should "allow valid range boundary case" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).withRangeNotPruned(
      minOffsetInclusive = offset(4),
      maxOffsetInclusive = offset(10),
      errorPruning = _ => "",
      errorLedgerEnd = _ => "",
    )(Future.unit)
      .futureValue
  }

  it should "deny in-valid range: earlier than pruning" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    loggerFactory
      .assertThrowsAndLogsSuppressingAsync[StatusRuntimeException](
        SuppressionRule.Level(Level.INFO)
      )(
        within = QueryValidRangeImpl(
          ledgerEndCache = backend.ledgerEndCache,
          pruningOffsetService = backend.pruningOffsetService,
          loggerFactory = this.loggerFactory,
        ).withRangeNotPruned(
          minOffsetInclusive = offset(3),
          maxOffsetInclusive = offset(10),
          errorPruning = pruningOffset => s"pruning issue: ${pruningOffset.unwrap}",
          errorLedgerEnd = _ => "",
        )(Future.unit),
        assertions = _.infoMessage should include(
          "PARTICIPANT_PRUNED_DATA_ACCESSED(9,0): pruning issue: 3"
        ),
      )
      .futureValue
  }

  it should "deny in-valid range: later than ledger end when ledger is not empty" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    loggerFactory
      .assertThrowsAndLogsSuppressingAsync[StatusRuntimeException](
        SuppressionRule.Level(Level.INFO)
      )(
        within = QueryValidRangeImpl(
          ledgerEndCache = backend.ledgerEndCache,
          pruningOffsetService = backend.pruningOffsetService,
          loggerFactory = this.loggerFactory,
        ).withRangeNotPruned(
          minOffsetInclusive = offset(4),
          maxOffsetInclusive = offset(11),
          errorPruning = _ => "",
          errorLedgerEnd =
            ledgerEndOffset => s"ledger-end issue: ${ledgerEndOffset.fold(0L)(_.unwrap)}",
        )(Future.unit),
        assertions = _.infoMessage should include(
          "PARTICIPANT_DATA_ACCESSED_AFTER_LEDGER_END(9,0): ledger-end issue: 10"
        ),
      )
  }

  it should "deny in-valid range: later than ledger end when ledger end is none" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    loggerFactory
      .assertThrowsAndLogsSuppressingAsync[StatusRuntimeException](
        SuppressionRule.Level(Level.INFO)
      )(
        within = QueryValidRangeImpl(
          ledgerEndCache = backend.ledgerEndCache,
          pruningOffsetService = backend.pruningOffsetService,
          loggerFactory = this.loggerFactory,
        ).withRangeNotPruned(
          minOffsetInclusive = offset(1),
          maxOffsetInclusive = offset(1),
          errorPruning = _ => "",
          errorLedgerEnd =
            ledgerEndOffset => s"ledger-end issue: ${ledgerEndOffset.fold(0L)(_.unwrap)}",
        )(Future.unit),
        assertions = _.infoMessage should include(
          "PARTICIPANT_DATA_ACCESSED_AFTER_LEDGER_END(9,0): ledger-end issue: 0"
        ),
      )
      .futureValue
  }

  behavior of "QueryValidRange.withOffsetNotBeforePruning"

  it should "allow offset in the valid range" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))

    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).withOffsetNotBeforePruning(
      offset = offset(5),
      errorPruning = _ => "",
      errorLedgerEnd = _ => "",
    )(Future.unit)
      .futureValue
  }

  it should "allow offset in the valid range if no pruning before" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(None))
    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).withOffsetNotBeforePruning(
      offset = offset(5),
      errorPruning = _ => "",
      errorLedgerEnd = _ => "",
    )(Future.unit)
      .futureValue
  }

  it should "allow offset in the valid range lower boundary" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).withOffsetNotBeforePruning(
      offset = offset(3),
      errorPruning = _ => "",
      errorLedgerEnd = _ => "",
    )(Future.unit)
      .futureValue
  }

  it should "allow offset in the valid range higher boundary" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).withOffsetNotBeforePruning(
      offset = offset(10),
      errorPruning = _ => "",
      errorLedgerEnd = _ => "",
    )(Future.unit)
      .futureValue
  }

  it should "deny in-valid range: earlier than pruning" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    loggerFactory
      .assertThrowsAndLogsSuppressingAsync[StatusRuntimeException](
        SuppressionRule.Level(Level.INFO)
      )(
        within = QueryValidRangeImpl(
          ledgerEndCache = backend.ledgerEndCache,
          pruningOffsetService = backend.pruningOffsetService,
          loggerFactory = this.loggerFactory,
        ).withOffsetNotBeforePruning(
          offset = offset(2),
          errorPruning = pruningOffset => s"pruning issue: ${pruningOffset.unwrap}",
          errorLedgerEnd = _ => "",
        )(Future.unit),
        assertions = _.infoMessage should include(
          "PARTICIPANT_PRUNED_DATA_ACCESSED(9,0): pruning issue: 3"
        ),
      )
      .futureValue
  }

  it should "deny in-valid range: later than ledger end" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    loggerFactory
      .assertThrowsAndLogsSuppressingAsync[StatusRuntimeException](
        SuppressionRule.Level(Level.INFO)
      )(
        within = QueryValidRangeImpl(
          ledgerEndCache = backend.ledgerEndCache,
          pruningOffsetService = backend.pruningOffsetService,
          loggerFactory = this.loggerFactory,
        ).withOffsetNotBeforePruning(
          offset = offset(11),
          errorPruning = _ => "",
          errorLedgerEnd =
            ledgerEndOffset => s"ledger-end issue: ${ledgerEndOffset.fold(0L)(_.unwrap)}",
        )(Future.unit),
        assertions = _.infoMessage should include(
          "PARTICIPANT_DATA_ACCESSED_AFTER_LEDGER_END(9,0): ledger-end issue: 10"
        ),
      )
      .futureValue
  }

  behavior of "QueryValidRange.filterPrunedEvents"

  it should "return all events if no pruning" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    val events = (1L to 5L).map(offset)
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(None))
    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).filterPrunedEvents[Offset](identity)(events.toVector).futureValue shouldBe events
  }

  it should "filter out events at or below the pruning offset" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    val events = (1L to 5L).map(offset)
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).filterPrunedEvents[Offset](identity)(events.toVector).futureValue shouldBe (4L to 5L).map(
      offset
    )
  }

  it should "return empty if all events are pruned" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    val events = (1L to 3L).map(offset)
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).filterPrunedEvents[Offset](identity)(events.toVector).futureValue shouldBe empty
  }

  it should "return all events if pruning offset is before all events" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    val events = (5L to 7L).map(offset)
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(3))))
    QueryValidRangeImpl(
      ledgerEndCache = backend.ledgerEndCache,
      pruningOffsetService = backend.pruningOffsetService,
      loggerFactory = this.loggerFactory,
    ).filterPrunedEvents[Offset](identity)(events.toVector).futureValue shouldBe events
  }

  it should "fail if any event offset is beyond ledger end" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val events = (1L to 5L).map(offset)
    when(backend.pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(None))
    loggerFactory
      .assertThrowsAndLogsSuppressingAsync[StatusRuntimeException](
        SuppressionRule.Level(Level.INFO)
      )(
        within = QueryValidRangeImpl(
          ledgerEndCache = backend.ledgerEndCache,
          pruningOffsetService = backend.pruningOffsetService,
          loggerFactory = this.loggerFactory,
        ).filterPrunedEvents[Offset](identity)(events.toVector),
        assertions = _.infoMessage should include(
          "PARTICIPANT_DATA_ACCESSED_AFTER_LEDGER_END(9,0): Offset of event to be filtered Offset(3) is beyond ledger end"
        ),
      )
      .futureValue
  }
}
