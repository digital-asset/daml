// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.scalautil.Statement
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.backend.PruningDto.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Checkpoints, OptionValues}

private[backend] trait StorageBackendTestsPruning
    extends Matchers
    with OptionValues
    with Checkpoints
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (pruning)"

  import StorageBackendTestValues.*

//  private val signatoryParty = Ref.Party.assertFromString("signatory")
//  private val observerParty = Ref.Party.assertFromString("observer")
//  private val nonStakeholderInformeeParty = Ref.Party.assertFromString("nonstakeholderinformee")
//  private val actorParty = Ref.Party.assertFromString("actor")

  def pruneEventsSql(
      previousPruneUpToInclusive: Option[Offset],
      previousIncompleteReassignmentOffsets: Vector[Offset],
      pruneUpToInclusive: Offset,
      incompleteReassignmentOffsets: Vector[Offset],
  )(implicit
      traceContext: TraceContext
  ): Unit =
    executeSql { conn =>
      conn.setAutoCommit(false)
      backend.event.pruneEvents(
        previousPruneUpToInclusive = previousPruneUpToInclusive,
        previousIncompleteReassignmentOffsets = previousIncompleteReassignmentOffsets,
        pruneUpToInclusive = pruneUpToInclusive,
        incompleteReassignmentOffsets = incompleteReassignmentOffsets,
      )(
        conn,
        traceContext,
      )
      conn.commit()
      conn.setAutoCommit(false)
    }

  it should "correctly update the pruning offset" in {
    val offset_1 = offset(3)
    val offset_2 = offset(2)
    val offset_3 = offset(4)

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    val initialPruningOffset = executeSql(backend.parameter.prunedUpToInclusive)

    executeSql(backend.parameter.updatePrunedUptoInclusive(offset_1))
    val updatedPruningOffset_1 = executeSql(backend.parameter.prunedUpToInclusive)

    executeSql(backend.parameter.updatePrunedUptoInclusive(offset_2))
    val updatedPruningOffset_2 = executeSql(backend.parameter.prunedUpToInclusive)

    executeSql(backend.parameter.updatePrunedUptoInclusive(offset_3))
    val updatedPruningOffset_3 = executeSql(backend.parameter.prunedUpToInclusive)

    initialPruningOffset shouldBe empty
    updatedPruningOffset_1 shouldBe Some(offset_1)
    // The pruning offset is not updated if lower than the existing offset
    updatedPruningOffset_2 shouldBe Some(offset_1)
    updatedPruningOffset_3 shouldBe Some(offset_3)
  }

  it should "prune completions" in {
    val someParty = Ref.Party.assertFromString("party")
    val completion = dtoCompletion(
      offset = offset(1),
      submitters = Set(someParty),
    )
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    // Ingest a completion
    executeSql(ingest(Vector(completion), _))
    assertIndexDbDataSql(completion = Seq(PruningDto.Completion(1)))
    // Prune
    executeSql(backend.completion.pruneCompletions(offset(1))(_, TraceContext.empty))
    assertIndexDbDataSql(completion = Seq.empty)
  }

  it should "prune various witnessed events" in {
    val updates = Vector(
      // before pruning start
      meta(event_offset = 1)(
        dtosWitnessedCreate(
          event_sequential_id = 100
        )()
      ),
      meta(event_offset = 2)(
        dtosWitnessedExercised(
          event_sequential_id = 200,
          consuming = true,
        )
      ),
      meta(event_offset = 3)(
        dtosWitnessedExercised(
          event_sequential_id = 300,
          consuming = false,
        )
      ),
      // in pruning range
      meta(event_offset = 4)(
        dtosWitnessedCreate(
          event_sequential_id = 400
        )()
      ),
      meta(event_offset = 5)(
        dtosWitnessedExercised(
          event_sequential_id = 500,
          consuming = true,
        )
      ),
      meta(event_offset = 6)(
        dtosWitnessedExercised(
          event_sequential_id = 600,
          consuming = false,
        )
      ),
      // after pruning range
      meta(event_offset = 7)(
        dtosWitnessedCreate(
          event_sequential_id = 700
        )()
      ),
      meta(event_offset = 8)(
        dtosWitnessedExercised(
          event_sequential_id = 800,
          consuming = true,
        )
      ),
      meta(event_offset = 9)(
        dtosWitnessedExercised(
          event_sequential_id = 900,
          consuming = false,
        )
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(9), 900L))
    assertIndexDbDataSql(
      variousWitnessed = List(
        100, 200, 300, 400, 500, 600, 700, 800, 900,
      ),
      variousFilterWitness = List(
        100, 100, 200, 200, 300, 300, 400, 400, 500, 500, 600, 600, 700, 700, 800, 800, 900, 900,
      ),
      txMeta = List(
        TxMeta(1),
        TxMeta(2),
        TxMeta(3),
        TxMeta(4),
        TxMeta(5),
        TxMeta(6),
        TxMeta(7),
        TxMeta(8),
        TxMeta(9),
      ),
    )
    // Prune
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(3)),
      previousIncompleteReassignmentOffsets = Vector.empty,
      pruneUpToInclusive = offset(6),
      incompleteReassignmentOffsets = Vector.empty,
    )(TraceContext.empty)

    assertIndexDbDataSql(
      variousWitnessed = List(
        100, 200, 300, 700, 800, 900,
      ),
      variousFilterWitness = List(
        100, 100, 200, 200, 300, 300, 700, 700, 800, 800, 900, 900,
      ),
      txMeta = List(
        TxMeta(1),
        TxMeta(2),
        TxMeta(3),
        TxMeta(7),
        TxMeta(8),
        TxMeta(9),
      ),
    )
  }

  it should "prune activate and deactivate events" in {
    val updates = Vector(
      // before pruning start will be pruned later
      meta(event_offset = 1)(
        dtosCreate(
          event_sequential_id = 100
        )()
      ),
      meta(event_offset = 2)(
        dtosAssign(
          event_sequential_id = 200
        )()
      ),
      // before pruning start won't be pruned later
      meta(event_offset = 3)(
        dtosCreate(
          event_sequential_id = 300
        )()
      ),
      meta(event_offset = 4)(
        dtosAssign(
          event_sequential_id = 400
        )()
      ),
      // in pruning range will be pruned later
      meta(event_offset = 5)(
        dtosCreate(
          event_sequential_id = 500
        )()
      ),
      meta(event_offset = 6)(
        dtosAssign(
          event_sequential_id = 600
        )()
      ),
      // in pruning range will be not pruned - no deactivation
      meta(event_offset = 7)(
        dtosCreate(
          event_sequential_id = 700
        )()
      ),
      meta(event_offset = 8)(
        dtosAssign(
          event_sequential_id = 800
        )()
      ),
      // in pruning range will be not pruned - deactivation outside of the pruning range
      meta(event_offset = 9)(
        dtosCreate(
          event_sequential_id = 900
        )()
      ),
      meta(event_offset = 10)(
        dtosAssign(
          event_sequential_id = 1000
        )()
      ),
      // deactivations in pruning range
      meta(event_offset = 11)(
        dtosConsumingExercise(
          event_sequential_id = 1100,
          deactivated_event_sequential_id = Some(100),
        )
      ),
      meta(event_offset = 12)(
        dtosUnassign(
          event_sequential_id = 1200,
          deactivated_event_sequential_id = Some(200),
        )
      ),
      meta(event_offset = 13)(
        dtosUnassign(
          event_sequential_id = 1300,
          deactivated_event_sequential_id = Some(500),
        )
      ),
      meta(event_offset = 14)(
        dtosConsumingExercise(
          event_sequential_id = 1400,
          deactivated_event_sequential_id = Some(600),
        )
      ),
      meta(event_offset = 15)(
        dtosConsumingExercise(
          event_sequential_id = 1500,
          deactivated_event_sequential_id = None,
        )
      ),
      // outside of pruning range some activations deactivated later
      meta(event_offset = 16)(
        dtosCreate(
          event_sequential_id = 1600
        )()
      ),
      meta(event_offset = 17)(
        dtosAssign(
          event_sequential_id = 1700
        )()
      ),
      // outside of pruning range some activations never deactivated
      meta(event_offset = 18)(
        dtosCreate(
          event_sequential_id = 1800
        )()
      ),
      meta(event_offset = 19)(
        dtosAssign(
          event_sequential_id = 1900
        )()
      ),
      // outside of pruning range some deactivations
      meta(event_offset = 20)(
        dtosUnassign(
          event_sequential_id = 2000,
          deactivated_event_sequential_id = Some(1700),
        )
      ),
      meta(event_offset = 21)(
        dtosConsumingExercise(
          event_sequential_id = 2100,
          deactivated_event_sequential_id = Some(1600),
        )
      ),
      meta(event_offset = 22)(
        dtosConsumingExercise(
          event_sequential_id = 2200,
          deactivated_event_sequential_id = None,
        )
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(22), 2200L))
    assertIndexDbDataSql(
      activate = List(
        100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1600, 1700, 1800, 1900,
      ),
      activateFilterStakeholder = List(
        100, 100, 200, 200, 300, 300, 400, 400, 500, 500, 600, 600, 700, 700, 800, 800, 900, 900,
        1000, 1000, 1600, 1600, 1700, 1700, 1800, 1800, 1900, 1900,
      ),
      activateFilterWitness = List(
        100, 100, 300, 300, 500, 500, 700, 700, 900, 900, 1600, 1600, 1800, 1800,
      ),
      deactivate = List(
        1100, 1200, 1300, 1400, 1500, 2000, 2100, 2200,
      ),
      deactivateFilterStakeholder = List(
        1100, 1100, 1200, 1200, 1300, 1300, 1400, 1400, 1500, 1500, 2000, 2000, 2100, 2100, 2200,
        2200,
      ),
      deactivateFilterWitness = List(
        1100, 1100, 1400, 1400, 1500, 1500, 2100, 2100, 2200, 2200,
      ),
      txMeta = List(
        TxMeta(1),
        TxMeta(2),
        TxMeta(3),
        TxMeta(4),
        TxMeta(5),
        TxMeta(6),
        TxMeta(7),
        TxMeta(8),
        TxMeta(9),
        TxMeta(10),
        TxMeta(11),
        TxMeta(12),
        TxMeta(13),
        TxMeta(14),
        TxMeta(15),
        TxMeta(16),
        TxMeta(17),
        TxMeta(18),
        TxMeta(19),
        TxMeta(20),
        TxMeta(21),
        TxMeta(22),
      ),
    )
    // Prune
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(4)),
      previousIncompleteReassignmentOffsets = Vector.empty,
      pruneUpToInclusive = offset(15),
      incompleteReassignmentOffsets = Vector.empty,
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(
        300, 400, 700, 800, 900, 1000, 1600, 1700, 1800, 1900,
      ),
      activateFilterStakeholder = List(
        300, 300, 400, 400, 700, 700, 800, 800, 900, 900, 1000, 1000, 1600, 1600, 1700, 1700, 1800,
        1800, 1900, 1900,
      ),
      activateFilterWitness = List(
        300, 300, 700, 700, 900, 900, 1600, 1600, 1800, 1800,
      ),
      deactivate = List(
        2000,
        2100,
        2200,
      ),
      deactivateFilterStakeholder = List(
        2000, 2000, 2100, 2100, 2200, 2200,
      ),
      deactivateFilterWitness = List(
        2100,
        2100,
        2200,
        2200,
      ),
      txMeta = List(
        TxMeta(1),
        TxMeta(2),
        TxMeta(3),
        TxMeta(4),
        TxMeta(16),
        TxMeta(17),
        TxMeta(18),
        TxMeta(19),
        TxMeta(20),
        TxMeta(21),
        TxMeta(22),
      ),
    )
  }

  it should "not prune incomplete events and related other events, but prune completed, older incomplete events and related other events" in {
    val updates = Vector(
      // before pruning start, incomplete assignments, won't be pruned
      meta(event_offset = 2)(
        dtosAssign(event_sequential_id = 200)() ++
          dtosAssign(event_sequential_id = 201)() ++
          dtosAssign(event_sequential_id = 202)()
      ),
      // before pruning start, relates to incomplete, so still retained
      meta(event_offset = 3)(
        dtosConsumingExercise(
          event_sequential_id = 300,
          deactivated_event_sequential_id = Some(202),
        )
      ),
      // in pruning range will be pruned later
      meta(event_offset = 5)(
        dtosCreate(event_sequential_id = 500)()
      ),
      meta(event_offset = 6)(
        dtosCreate(event_sequential_id = 600)()
      ),
      // deactivations in pruning range
      meta(event_offset = 11)(
        dtosConsumingExercise(
          event_sequential_id = 1100,
          deactivated_event_sequential_id = Some(500),
        ) ++
          // related to incomplete assignment, won't be pruned
          dtosConsumingExercise(
            event_sequential_id = 1101,
            deactivated_event_sequential_id = Some(201),
          )
      ),
      // incomplete unassignments, won't be pruned
      meta(event_offset = 12)(
        // this also relates to an incomplete unassignment
        dtosUnassign(event_sequential_id = 1200, deactivated_event_sequential_id = Some(200)) ++
          dtosUnassign(event_sequential_id = 1201, deactivated_event_sequential_id = Some(600))
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(22), 2200L))
    assertIndexDbDataSql(
      activate = List(
        200, 201, 202, 500, 600,
      ),
      activateFilterStakeholder = List(
        200, 200, 201, 201, 202, 202, 500, 500, 600, 600,
      ),
      activateFilterWitness = List(
        500,
        500,
        600,
        600,
      ),
      deactivate = List(
        300, 1100, 1101, 1200, 1201,
      ),
      deactivateFilterStakeholder = List(
        300, 300, 1100, 1100, 1101, 1101, 1200, 1200, 1201, 1201,
      ),
      deactivateFilterWitness = List(
        300, 300, 1100, 1100, 1101, 1101,
      ),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
        TxMeta(5),
        TxMeta(6),
        TxMeta(11),
        TxMeta(12),
      ),
    )
    // Prune
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(4)),
      previousIncompleteReassignmentOffsets = Vector(offset(2)),
      pruneUpToInclusive = offset(15),
      incompleteReassignmentOffsets = Vector(offset(2), offset(12)),
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(
        200,
        201,
        202,
        600,
      ),
      activateFilterStakeholder = List(
        200, 200, 201, 201, 202, 202, 600, 600,
      ),
      activateFilterWitness = List(
        600,
        600,
      ),
      deactivate = List(
        300,
        1101,
        1200,
        1201,
      ),
      deactivateFilterStakeholder = List(
        300, 300, 1101, 1101, 1200, 1200, 1201, 1201,
      ),
      deactivateFilterWitness = List(
        300,
        300,
        1101,
        1101,
      ),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
      ),
    )

    // Prune again
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(15)),
      previousIncompleteReassignmentOffsets = Vector(offset(2), offset(12)),
      pruneUpToInclusive = offset(17),
      incompleteReassignmentOffsets = Vector(offset(2)),
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(
        200,
        201,
        202,
      ),
      activateFilterStakeholder = List(
        200, 200, 201, 201, 202, 202,
      ),
      activateFilterWitness = List(),
      deactivate = List(
        300,
        1101,
        1200,
      ),
      deactivateFilterStakeholder = List(
        300, 300, 1101, 1101, 1200, 1200,
      ),
      deactivateFilterWitness = List(
        300,
        300,
        1101,
        1101,
      ),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
      ),
    )

    // Prune again
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(17)),
      previousIncompleteReassignmentOffsets = Vector(offset(2)),
      pruneUpToInclusive = offset(21),
      incompleteReassignmentOffsets = Vector(),
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(),
      activateFilterStakeholder = List(),
      activateFilterWitness = List(),
      deactivate = List(),
      deactivateFilterStakeholder = List(),
      deactivateFilterWitness = List(),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
      ),
    )
  }

  it should "not prune incomplete events and related other events, but prune completed, older incomplete events and related other events - combined case having new incomplete and complete as well" in {
    val updates = Vector(
      // before pruning start, older incomplete assignments, will become completed, and prunable
      meta(event_offset = 2)(
        dtosAssign(event_sequential_id = 200)() ++
          dtosAssign(event_sequential_id = 201)() ++
          dtosAssign(event_sequential_id = 202)()
      ),
      // before pruning start, relates to incomplete, so still retained previously, but with that becoming completed, prunable
      meta(event_offset = 3)(
        dtosConsumingExercise(
          event_sequential_id = 300,
          deactivated_event_sequential_id = Some(202),
        )
      ),
      // in pruning range will be pruned later
      meta(event_offset = 5)(
        dtosCreate(event_sequential_id = 500)()
      ),
      meta(event_offset = 6)(
        dtosCreate(event_sequential_id = 600)()
      ),
      // deactivations in pruning range
      meta(event_offset = 11)(
        dtosConsumingExercise(
          event_sequential_id = 1100,
          deactivated_event_sequential_id = Some(500),
        ) ++
          // related to completed assignment, will be pruned
          dtosConsumingExercise(
            event_sequential_id = 1101,
            deactivated_event_sequential_id = Some(201),
          )
      ),
      // incomplete unassignments, won't be pruned
      meta(event_offset = 12)(
        // this also relates to a previously incomplete unassignment
        dtosUnassign(event_sequential_id = 1200, deactivated_event_sequential_id = Some(200)) ++
          dtosUnassign(event_sequential_id = 1201, deactivated_event_sequential_id = Some(600))
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(22), 2200L))
    assertIndexDbDataSql(
      activate = List(
        200, 201, 202, 500, 600,
      ),
      activateFilterStakeholder = List(
        200, 200, 201, 201, 202, 202, 500, 500, 600, 600,
      ),
      activateFilterWitness = List(
        500,
        500,
        600,
        600,
      ),
      deactivate = List(
        300, 1100, 1101, 1200, 1201,
      ),
      deactivateFilterStakeholder = List(
        300, 300, 1100, 1100, 1101, 1101, 1200, 1200, 1201, 1201,
      ),
      deactivateFilterWitness = List(
        300, 300, 1100, 1100, 1101, 1101,
      ),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
        TxMeta(5),
        TxMeta(6),
        TxMeta(11),
        TxMeta(12),
      ),
    )
    // Prune
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(4)),
      previousIncompleteReassignmentOffsets = Vector(offset(2)),
      pruneUpToInclusive = offset(15),
      incompleteReassignmentOffsets = Vector(offset(12)),
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(
        200,
        600,
      ),
      activateFilterStakeholder = List(
        200,
        200,
        600,
        600,
      ),
      activateFilterWitness = List(
        600,
        600,
      ),
      deactivate = List(
        1200,
        1201,
      ),
      deactivateFilterStakeholder = List(
        1200,
        1200,
        1201,
        1201,
      ),
      deactivateFilterWitness = List(),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
      ),
    )

    // Prune again
    pruneEventsSql(
      previousPruneUpToInclusive = Some(offset(15)),
      previousIncompleteReassignmentOffsets = Vector(offset(12)),
      pruneUpToInclusive = offset(17),
      incompleteReassignmentOffsets = Vector(),
    )(TraceContext.empty)

    assertIndexDbDataSql(
      activate = List(),
      activateFilterStakeholder = List(),
      activateFilterWitness = List(),
      deactivate = List(),
      deactivateFilterStakeholder = List(),
      deactivateFilterWitness = List(),
      txMeta = List(
        TxMeta(2),
        TxMeta(3),
      ),
    )
  }

  // TODO(i21351) Implement pruning tests for topology events

  /** Asserts the content of the tables subject to pruning. Be default asserts the tables are empty.
    */
  def assertIndexDbDataSql(
      activate: Seq[Long] = Seq.empty,
      activateFilterStakeholder: Seq[Long] = Seq.empty,
      activateFilterWitness: Seq[Long] = Seq.empty,
      deactivate: Seq[Long] = Seq.empty,
      deactivateFilterStakeholder: Seq[Long] = Seq.empty,
      deactivateFilterWitness: Seq[Long] = Seq.empty,
      variousWitnessed: Seq[Long] = Seq.empty,
      variousFilterWitness: Seq[Long] = Seq.empty,
      txMeta: Seq[TxMeta] = Seq.empty,
      completion: Seq[Completion] = Seq.empty,
  ): Assertion = executeSql { implicit c =>
    val queries = backend.pruningDtoQueries
    val cp = new Checkpoint
    // activate
    cp(clue("activate")(Statement.discard(queries.eventActivate shouldBe activate)))
    cp(
      clue("activate filter stakeholder")(
        Statement.discard(queries.filterActivateStakeholder shouldBe activateFilterStakeholder)
      )
    )
    cp(
      clue("activate filter witness")(
        Statement.discard(queries.filterActivateWitness shouldBe activateFilterWitness)
      )
    )
    // deactivate
    cp(clue("deactivate")(Statement.discard(queries.eventDeactivate shouldBe deactivate)))
    cp(
      clue("deactivate filter stakeholder")(
        Statement.discard(queries.filterDeactivateStakeholder shouldBe deactivateFilterStakeholder)
      )
    )
    cp(
      clue("deactivate filter witness")(
        Statement.discard(queries.filterDeactivateWitness shouldBe deactivateFilterWitness)
      )
    )
    // witnessed
    cp(
      clue("various witnessed")(
        Statement.discard(queries.eventVariousWitnessed shouldBe variousWitnessed)
      )
    )
    cp(
      clue("various witnessed filter")(
        Statement.discard(queries.filterVariousWitness shouldBe variousFilterWitness)
      )
    )
    // other
    cp(clue("meta")(Statement.discard(queries.updateMeta shouldBe txMeta)))
    cp(clue("completion")(Statement.discard(queries.completions shouldBe completion)))
    cp.reportAll()
    succeed
  }
}
