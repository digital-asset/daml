// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

private[backend] trait StorageBackendTestsQueryValidRange extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  implicit val loggingContextWithTrace: LoggingContextWithTrace =
    new LoggingContextWithTrace(LoggingEntries.empty, TraceContext.empty)

  behavior of "QueryValidRange.withRangeNotPruned"

  it should "allow valid range if no pruning and before ledger end" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(implicit connection =>
      QueryValidRangeImpl(backend.parameter, this.loggerFactory).withRangeNotPruned(
        minOffsetExclusive = offset(2),
        maxOffsetInclusive = offset(8),
        errorPruning = _ => "",
        errorLedgerEnd = _ => "",
      )(())
    )
  }

  it should "allow valid range if no pruning and before ledger end and start from ledger begin" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(implicit connection =>
      QueryValidRangeImpl(backend.parameter, this.loggerFactory).withRangeNotPruned(
        minOffsetExclusive = Offset.beforeBegin,
        maxOffsetInclusive = offset(8),
        errorPruning = _ => "",
        errorLedgerEnd = _ => "",
      )(())
    )
  }

  it should "allow valid range after pruning and before ledger end" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(3)))
    executeSql(implicit connection =>
      QueryValidRangeImpl(backend.parameter, this.loggerFactory).withRangeNotPruned(
        minOffsetExclusive = offset(5),
        maxOffsetInclusive = offset(8),
        errorPruning = _ => "",
        errorLedgerEnd = _ => "",
      )(())
    )
  }

  it should "allow valid range boundary case" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(3)))
    executeSql(implicit connection =>
      QueryValidRangeImpl(backend.parameter, this.loggerFactory).withRangeNotPruned(
        minOffsetExclusive = offset(3),
        maxOffsetInclusive = offset(10),
        errorPruning = _ => "",
        errorLedgerEnd = _ => "",
      )(())
    )
  }

  it should "deny in-valid range: earlier than pruning" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(3)))
    loggerFactory.assertThrowsAndLogsSuppressing[StatusRuntimeException](
      SuppressionRule.Level(Level.INFO)
    )(
      within = executeSql(implicit connection =>
        QueryValidRangeImpl(backend.parameter, this.loggerFactory).withRangeNotPruned(
          minOffsetExclusive = offset(2),
          maxOffsetInclusive = offset(10),
          errorPruning = pruningOffset => s"pruning issue: ${pruningOffset.toHexString}",
          errorLedgerEnd = _ => "",
        )(())
      ),
      assertions = _.infoMessage should include(
        "PARTICIPANT_PRUNED_DATA_ACCESSED(9,0): pruning issue: 00000003"
      ),
    )
  }

  it should "deny in-valid range: later than ledger end" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(3)))
    loggerFactory.assertThrowsAndLogsSuppressing[StatusRuntimeException](
      SuppressionRule.Level(Level.INFO)
    )(
      within = executeSql(implicit connection =>
        QueryValidRangeImpl(backend.parameter, this.loggerFactory).withRangeNotPruned(
          minOffsetExclusive = offset(3),
          maxOffsetInclusive = offset(11),
          errorPruning = _ => "",
          errorLedgerEnd = ledgerEndOffset => s"ledger-end issue: ${ledgerEndOffset.toHexString}",
        )(())
      ),
      assertions = _.infoMessage should include(
        "PARTICIPANT_DATA_ACCESSED_AFTER_LEDGER_END(9,0): ledger-end issue: 00000010"
      ),
    )
  }

  it should "execute query before reading parameters from the db" in {
    executeSql(implicit connection =>
      QueryValidRangeImpl(backend.parameter, this.loggerFactory).withRangeNotPruned(
        minOffsetExclusive = offset(2),
        maxOffsetInclusive = offset(8),
        errorPruning = _ => "",
        errorLedgerEnd = _ => "",
      ) {
        backend.parameter.initializeParameters(someIdentityParams, loggerFactory)(connection)
        updateLedgerEnd(offset(10), 10L)(connection)
      }
    )
  }

  behavior of "QueryValidRange.withOffsetNotBeforePruning"

  it should "allow offset in the valid range" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(3)))
    executeSql(implicit connection =>
      QueryValidRangeImpl(backend.parameter, this.loggerFactory).withOffsetNotBeforePruning(
        offset = offset(5),
        errorPruning = _ => "",
        errorLedgerEnd = _ => "",
      )(())
    )
  }

  it should "allow offset in the valid range if no pruning before" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(implicit connection =>
      QueryValidRangeImpl(backend.parameter, this.loggerFactory).withOffsetNotBeforePruning(
        offset = offset(5),
        errorPruning = _ => "",
        errorLedgerEnd = _ => "",
      )(())
    )
  }

  it should "allow offset in the valid range lower boundary" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(3)))
    executeSql(implicit connection =>
      QueryValidRangeImpl(backend.parameter, this.loggerFactory).withOffsetNotBeforePruning(
        offset = offset(3),
        errorPruning = _ => "",
        errorLedgerEnd = _ => "",
      )(())
    )
  }

  it should "allow offset in the valid range higher boundary" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(3)))
    executeSql(implicit connection =>
      QueryValidRangeImpl(backend.parameter, this.loggerFactory).withOffsetNotBeforePruning(
        offset = offset(10),
        errorPruning = _ => "",
        errorLedgerEnd = _ => "",
      )(())
    )
  }

  it should "deny in-valid range: earlier than pruning" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(3)))
    loggerFactory.assertThrowsAndLogsSuppressing[StatusRuntimeException](
      SuppressionRule.Level(Level.INFO)
    )(
      within = executeSql(implicit connection =>
        QueryValidRangeImpl(backend.parameter, this.loggerFactory).withOffsetNotBeforePruning(
          offset = offset(2),
          errorPruning = pruningOffset => s"pruning issue: ${pruningOffset.toHexString}",
          errorLedgerEnd = _ => "",
        )(())
      ),
      assertions = _.infoMessage should include(
        "PARTICIPANT_PRUNED_DATA_ACCESSED(9,0): pruning issue: 00000003"
      ),
    )
  }

  it should "deny in-valid range: later than ledger end" in {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(offset(10), 10L))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(3)))
    loggerFactory.assertThrowsAndLogsSuppressing[StatusRuntimeException](
      SuppressionRule.Level(Level.INFO)
    )(
      within = executeSql(implicit connection =>
        QueryValidRangeImpl(backend.parameter, this.loggerFactory).withOffsetNotBeforePruning(
          offset = offset(11),
          errorPruning = _ => "",
          errorLedgerEnd = ledgerEndOffset => s"ledger-end issue: ${ledgerEndOffset.toHexString}",
        )(())
      ),
      assertions = _.infoMessage should include(
        "PARTICIPANT_DATA_ACCESSED_AFTER_LEDGER_END(9,0): ledger-end issue: 00000010"
      ),
    )
  }
}
