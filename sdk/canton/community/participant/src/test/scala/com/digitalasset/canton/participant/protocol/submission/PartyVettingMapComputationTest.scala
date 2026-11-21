// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.ledger.participant.state.RoutingSynchronizerState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.SuppressionRule.LoggerNameContains
import com.digitalasset.canton.participant.protocol.submission.routing.AdmissibleSynchronizersComputation
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError
import com.digitalasset.canton.topology.client.TopologySnapshotLoader
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPackageId, LfPartyId, LfTimestamp}
import io.grpc.StatusRuntimeException
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import scala.concurrent.Future

class PartyVettingMapComputationTest extends AsyncWordSpec with BaseTest with HasExecutionContext {
  "PartyVettingMapComputation.computePartyVettingMap" should {
    "correctly compute the vetted package state per party per synchronizer" in withNewTestContext {
      testContext =>
        import testContext.*
        arrangeTopologyState(
          topologyState = Map(
            psId1 -> (sync1TopologySnapshotLoader, Map(
              participant1 -> (Set(alice), Set(pkgId1)),
              participant2 -> (Set(bob), Set(pkgId2)),
            )),
            psId2 -> (sync2TopologySnapshotLoader, Map(
              participant2 -> (Set(alice), Set(pkgId2)),
              participant3 -> (Set(bob), Set(pkgId3)),
            )),
          ),
          expectedSubmitters = Set(alice),
          expectedInformees = Set(alice, bob),
        )

        partyVettingMapComputation
          .computePartyVettingMap(
            submitters = Set(alice),
            informees = Set(alice, bob),
            vettingValidityTimestamp = vettingValidityTimestamp,
            prescribedSynchronizerIdO = None,
            synchronizerState = routingSynchronizerState,
          )
          .map(packageMap =>
            packageMap shouldBe Map(
              psId1 -> Map(alice -> Set(pkgId1), bob -> Set(pkgId2)),
              psId2 -> Map(alice -> Set(pkgId2), bob -> Set(pkgId3)),
            )
          )
    }

    "discard a package if it is not commonly vetted on all hosting participants of a party" in withNewTestContext {
      testContext =>
        import testContext.*
        arrangeTopologyState(
          topologyState = Map(
            psId1 -> (sync1TopologySnapshotLoader, Map(
              participant1 -> (Set(alice), Set(pkgId2)),
              participant2 -> (Set(alice), Set(pkgId1, pkgId2)),
            ))
          ),
          expectedSubmitters = Set(alice),
          expectedInformees = Set(alice),
        )

        loggerFactory.assertLogsSeq(
          LoggerNameContains(classOf[PartyVettingMapComputation].getSimpleName)
        )(
          partyVettingMapComputation
            .computePartyVettingMap(
              submitters = Set(alice),
              informees = Set(alice),
              vettingValidityTimestamp = vettingValidityTimestamp,
              prescribedSynchronizerIdO = None,
              synchronizerState = routingSynchronizerState,
            )
            .map(packageMap => packageMap shouldBe Map(psId1 -> Map(alice -> Set(pkgId2)))),
          logs => {
            val entry = logs.loneElement
            entry.level shouldBe Level.DEBUG
            entry.message should include(
              show"Discarding the following packages from package selection as they are not vetted on all hosting participants of party $alice on synchronizer $psId1: ${Set(pkgId1)}"
            )
          },
        )
    }

    "consider only the prescribed synchronizer if provided" in withNewTestContext { testContext =>
      import testContext.*
      arrangeTopologyState(
        topologyState = Map(
          psId1 -> (sync1TopologySnapshotLoader, Map(
            participant1 -> (Set(alice, bob), Set(pkgId1))
          )),
          psId2 -> (sync2TopologySnapshotLoader, Map(
            participant2 -> (Set(alice, bob), Set(pkgId2))
          )),
        ),
        expectedSubmitters = Set(alice),
        expectedInformees = Set(alice, bob),
      )
      when(routingSynchronizerState.getPhysicalId(psId1.logical)).thenReturn(Some(psId1))
      partyVettingMapComputation
        .computePartyVettingMap(
          submitters = Set(alice),
          informees = Set(alice, bob),
          vettingValidityTimestamp = vettingValidityTimestamp,
          prescribedSynchronizerIdO = Some(psId1.logical),
          synchronizerState = routingSynchronizerState,
        )
        .map(packageMap =>
          packageMap shouldBe Map(psId1 -> Map(alice -> Set(pkgId1), bob -> Set(pkgId1)))
        )
    }

    "discard a package if the validity timestamp is outside the package's vetting bounds" in withNewTestContext {
      testContext =>
        import testContext.*

        arrangeTopologyState(
          Map(
            psId1 -> (sync1TopologySnapshotLoader, Map(
              participant1 -> (Set(bob, alice), Set(pkgId1, pkgId2, pkgId3)),
              participant2 -> (Set(bob), Set(pkgId1, pkgId3)),
            ))
          ),
          expectedSubmitters = Set(alice),
          expectedInformees = Set(alice, bob),
          // pkg 1 is valid on participant2 but not on participant1 at the vettingValidityTimestamp
          withVettingBounds = Map(
            psId1 -> Map(
              participant1 ->
                Map(
                  pkgId1 -> VettedPackage(
                    pkgId1,
                    // Outside vetting bounds
                    validFromInclusive = Some(vettingValidityTimestamp.plusMillis(1L)),
                    validUntilExclusive = None,
                  )
                ),
              participant2 -> Map(
                pkgId1 -> VettedPackage(
                  pkgId1,
                  // Within vetting bounds
                  validFromInclusive = Some(vettingValidityTimestamp.minusMillis(1L)),
                  validUntilExclusive = None,
                )
              ),
            )
          ),
        )
        partyVettingMapComputation
          .computePartyVettingMap(
            submitters = Set(alice),
            informees = Set(alice, bob),
            vettingValidityTimestamp = vettingValidityTimestamp,
            prescribedSynchronizerIdO = None,
            synchronizerState = routingSynchronizerState,
          )
          .map(packageMap =>
            packageMap shouldBe Map(
              psId1 -> Map(
                // pkgId1 is discarded for alice since its hosting participant vetted with exceeded bounds
                alice -> Set(pkgId2, pkgId3),
                // pkgId1 is discarded for bob since one of its hosting participants vetted with exceeded bounds
                bob -> Set(pkgId3),
              )
            )
          )
    }

    "fail if there is no ready synchronizer" in withNewTestContext { testContext =>
      import testContext.*
      when(routingSynchronizerState.existsReadySynchronizer()).thenReturn(false)
      partyVettingMapComputation
        .computePartyVettingMap(
          submitters = Set(alice),
          informees = Set(alice, bob),
          vettingValidityTimestamp = vettingValidityTimestamp,
          prescribedSynchronizerIdO = None,
          synchronizerState = routingSynchronizerState,
        )
        .failed
        .map(inside(_) { case ex: StatusRuntimeException =>
          val errorCode = DecodedCantonError.fromStatusRuntimeException(ex).value
          errorCode.code.id shouldBe SyncServiceInjectionError.NotConnectedToAnySynchronizer.id
        })
    }

    "fail if the prescribed sync id is not known" in withNewTestContext { testContext =>
      import testContext.*
      arrangeTopologyState(
        topologyState = Map(), // not relevant
        expectedSubmitters = Set.empty, // not relevant
        expectedInformees = Set.empty, // not relevant
      )
      val unknownSyncId =
        PhysicalSynchronizerId.tryFromString(s"da::unknown::${ProtocolVersion.latest.toString}-0")
      when(routingSynchronizerState.getPhysicalId(unknownSyncId.logical)).thenReturn(None)
      partyVettingMapComputation
        .computePartyVettingMap(
          submitters = Set(alice),
          informees = Set(alice, bob),
          vettingValidityTimestamp = vettingValidityTimestamp,
          prescribedSynchronizerIdO = Some(unknownSyncId.logical),
          synchronizerState = routingSynchronizerState,
        )
        .failed
        .map(inside(_) { case ex: StatusRuntimeException =>
          val errorCode = DecodedCantonError.fromStatusRuntimeException(ex).value
          errorCode.code.id shouldBe InvalidPrescribedSynchronizerId.id
          errorCode.cause should include(
            s"cannot resolve to physical synchronizer; ensure the node is connected to ${unknownSyncId.logical}"
          )
        })
    }

    "fail if the prescribed sync id is not found in the admissible synchronizers" in withNewTestContext {
      testContext =>
        import testContext.*
        arrangeTopologyState(
          topologyState = Map(
            psId1 -> (sync1TopologySnapshotLoader, Map(participant1 -> (Set(alice), Set(pkgId1))))
          ),
          expectedSubmitters = Set(alice),
          expectedInformees = Set(alice),
        )
        when(routingSynchronizerState.getPhysicalId(psId2.logical)).thenReturn(Some(psId2))
        partyVettingMapComputation
          .computePartyVettingMap(
            submitters = Set(alice),
            informees = Set(alice),
            vettingValidityTimestamp = vettingValidityTimestamp,
            prescribedSynchronizerIdO = Some(psId2.logical),
            synchronizerState = routingSynchronizerState,
          )
          .failed
          .map(inside(_) { case ex: StatusRuntimeException =>
            val errorCode = DecodedCantonError.fromStatusRuntimeException(ex).value
            errorCode.code.id shouldBe InvalidPrescribedSynchronizerId.id
            errorCode.cause should include(
              s"Not all informees are on the specified synchronizer: $psId2, but on ${Set(psId1)}"
            )
          })
    }
  }

  private def withNewTestContext(
      testCode: TestContext => FutureUnlessShutdown[Assertion]
  ): Future[Assertion] =
    testCode(new TestContext).failOnShutdown("Unexpected shutdown during test")

  private class TestContext {
    val admissibleSynchronizersComputation = mock[AdmissibleSynchronizersComputation]
    val partyVettingMapComputation = new PartyVettingMapComputation(
      admissibleSynchronizersComputation,
      loggerFactory,
    )
    val vettingValidityTimestamp = CantonTimestamp(LfTimestamp.assertFromLong(1337L))
    val routingSynchronizerState = mock[RoutingSynchronizerState]
    val psId1 =
      PhysicalSynchronizerId.tryFromString(s"da::one::${ProtocolVersion.latest.toString}-0")
    val psId2 =
      PhysicalSynchronizerId.tryFromString(s"da::two::${ProtocolVersion.latest.toString}-0")
    val participant1 = ParticipantId.tryFromProtoPrimitive("PAR::bothsyncs::participant1")
    val participant2 = ParticipantId.tryFromProtoPrimitive("PAR::bothsyncs::participant2")
    val participant3 = ParticipantId.tryFromProtoPrimitive("PAR::sync1::participant3")
    val alice = LfPartyId.assertFromString("alice")
    val bob = LfPartyId.assertFromString("bob")
    val pkgId1 = LfPackageId.assertFromString("pkg1")
    val pkgId2 = LfPackageId.assertFromString("pkg2")
    val pkgId3 = LfPackageId.assertFromString("pkg3")

    val sync1TopologySnapshotLoader = mock[TopologySnapshotLoader]
    val sync2TopologySnapshotLoader = mock[TopologySnapshotLoader]

    def arrangeTopologyState(
        topologyState: Map[
          PhysicalSynchronizerId,
          (TopologySnapshotLoader, Map[ParticipantId, (Set[LfPartyId], Set[LfPackageId])]),
        ],
        expectedSubmitters: Set[LfPartyId],
        expectedInformees: Set[LfPartyId],
        // If specified, overrides the default VettedPackage with None bounds
        withVettingBounds: Map[
          PhysicalSynchronizerId,
          Map[ParticipantId, Map[LfPackageId, VettedPackage]],
        ] = Map.empty,
    ) = {
      topologyState.foreach { case (psId, (snapshotLoader, participantMap)) =>
        when(routingSynchronizerState.getTopologySnapshotFor(psId))
          .thenReturn(Right(snapshotLoader))
        when(snapshotLoader.loadVettedPackages(participantMap.keySet))
          .thenReturn(
            FutureUnlessShutdown.pure(
              participantMap.view.map { case (participantId, (_parties, pkgs)) =>
                participantId -> pkgs
                  .map(pkgId =>
                    pkgId ->
                      withVettingBounds
                        .get(psId)
                        .flatMap(_.get(participantId))
                        .flatMap(_.get(pkgId))
                        .getOrElse(VettedPackage(pkgId, None, None))
                  )
                  .toMap
              }.toMap
            )
          )
      }

      if (topologyState.nonEmpty)
        when(
          admissibleSynchronizersComputation.forParties(
            expectedSubmitters,
            expectedInformees,
            routingSynchronizerState,
          )
        ).thenReturn(
          EitherT.rightT(
            NonEmpty
              .from(
                topologyState.view.map { case (psId, (_snapshotLoader, participantMap)) =>
                  psId -> participantMap.view
                    .flatMap { case (participantId, (parties, _pkgs)) =>
                      parties.map(_ -> participantId)
                    }
                    .groupMap(_._1)(_._2)
                    .view
                    .mapValues(_.toSet)
                    .toMap
                }.toMap
              )
              .value
          )
        )
    }
    when(routingSynchronizerState.existsReadySynchronizer()).thenReturn(true)
  }
}
