// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.NeedsNewLfContractIds
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.crypto.{KeyPurpose, SigningPublicKey}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.synchronizer.sequencer.SendDecision
import com.digitalasset.canton.synchronizer.sequencer.SendPolicy.processTimeProofs_
import com.digitalasset.canton.time.NonNegativeFiniteDuration

import scala.concurrent.{Future, Promise}

abstract class ReassignmentBackdatingIntegrationTest
    extends SynchronizerChangeIntegrationTest(
      SynchronizerChangeIntegrationTest.Config(
        simClock = true,
        assignmentExclusivityTimeout = NonNegativeFiniteDuration.tryOfMinutes(10),
      )
    )
    with NeedsNewLfContractIds {

  "An unassignment succeeds even if a stakeholder is no longer hosted on the target synchronizer" in {
    implicit env =>
      import env.*

      withUniqueParties { case (alice, bank, painter) =>
        // we get the namespace key for the synchronizer owner because we are going to use it as a delegation key for P4
        val synchronizerOwnerSigningKey = sequencer1.keys.secret
          .list(
            filterName = s"${sequencer1.name}-namespace",
            filterPurpose = Set(KeyPurpose.Signing),
          )
          .headOption
          .getOrElse(fail("Cannot get namespace signing key"))
          .publicKey
          .asInstanceOf[SigningPublicKey]

        P4.keys.public
          .upload(
            synchronizerOwnerSigningKey.toByteString(testedProtocolVersion),
            Some("synchronizer owner pubkey"),
          )
        P4.topology.namespace_delegations.propose_delegation(
          P4.namespace,
          synchronizerOwnerSigningKey,
          isRootDelegation = false,
        )

        // Create PaintOffer on PaintSynchronizer
        val cmd = createPaintOfferCmd(alice, bank, painter, newLfContractId())
        P5.ledger_api.commands.submit(Seq(alice), Seq(cmd), Some(paintSynchronizerId))
        val paintOfferId =
          searchAcsSync(
            Seq(P4, P5),
            paintSynchronizerAlias.unwrap,
            PaintModule,
            OfferToPaintHouseByOwnerTemplate,
          )

        val sequencerPaint = getProgrammableSequencer("sequencer2")

        val unassignmentP = Promise[Unit]()
        val painterDisabledP = Promise[Unit]()

        val P5id = P5.id
        // When P5 tries to sent the unassignment request (for moving the paint offer to the IouSynchronizer),
        // disable the Painter on P4 for the IouSynchronizer and
        // advance the sim clock to simulate that a very old time-proof has been used.
        sequencerPaint.setPolicy_("advance time before unassignment request is sequenced") {
          processTimeProofs_ { submissionRequest =>
            if (
              submissionRequest.sender == P5id && submissionRequest.isConfirmationRequest &&
              submissionRequest.batch.envelopes.sizeIs == 3 // Informee tree + reassignment request + root hash message
            ) {
              unassignmentP.success(())
              SendDecision.HoldBack(painterDisabledP.future)
            } else SendDecision.Process
          }
        }

        val unassignedEventF = Future {
          // TODO(#4009) This should fail because P4 does not host the painter on the target synchronizer
          //  when the unassignment is sequenced.
          P5.ledger_api.commands
            .submit_unassign(
              alice,
              paintOfferId,
              paintSynchronizerId,
              iouSynchronizerId,
            )
        }

        val disableF = unassignmentP.future.map { _ =>
          logger.info("Disabling the Painter on P4")
          sequencer1.topology.party_to_participant_mappings.propose_delta(
            painter,
            removes = List(P4.id),
            store = daId,
            signedBy = Some(synchronizerOwnerSigningKey.fingerprint),
          )
          eventually() {
            val painters =
              P4.parties.list(filterParty = painter.filterString)
            assert(
              painters.forall(
                _.participants.forall(pd =>
                  pd.participant != P4.id || pd.synchronizers.forall(
                    _.synchronizerId != iouSynchronizerId
                  )
                )
              )
            )
          }
          logger.info("Disabled the Painter on P4")

          Seq(P4, P5).foreach(_.testing.fetch_synchronizer_times())

          painterDisabledP.success(())
        }

        val unassignedEvent = unassignedEventF.futureValue
        disableF.futureValue

        for (participantRef <- Seq(P4, P5)) {
          withClue(s"For participant ${participantRef.name}") {
            eventually() {
              participantRef.ledger_api.state.acs
                .incomplete_unassigned_of_party(alice)
                .filter(
                  _.unassignId == unassignedEvent.unassignId
                ) should have size 1
            }
          }
        }

      // We could now try to submit the assignment request and fail,
      // but since we're running with a sim clock,
      // this would be stuck until some GRPc time limit elapses.
      }
  }
}

class ReassignmentBackdatingIntegrationTestDefault extends ReassignmentBackdatingIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

//class ReassignmentBackdatingTestPostgres extends ReassignmentBackdatingTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(
//    new UseReferenceBlockSequencer[DbConfig.Postgres](
//      loggerFactory,
//      sequencerGroups = Seq(
//        Set(InstanceName.tryCreate("sequencer1")),
//        Set(InstanceName.tryCreate("sequencer2")),
//      ),
//    )
//  )
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}
