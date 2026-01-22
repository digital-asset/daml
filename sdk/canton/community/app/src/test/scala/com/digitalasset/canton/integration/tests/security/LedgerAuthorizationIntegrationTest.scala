// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.syntax.compose.*
import cats.syntax.functor.*
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.commands.Command.toJavaProto
import com.daml.ledger.javaapi
import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.TagContainer
import com.daml.test.evidence.tag.EvidenceTag
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.{CommandFailure, ParticipantReference}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.error.MediatorError.{
  InvalidMessage,
  MalformedMessage,
  ParticipantEquivocation,
}
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.NoSynchronizerOnWhichAllSubmittersCanSubmit
import com.digitalasset.canton.integration.plugins.{
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.security.SecurityTestHelpers.SignedMessageTransform
import com.digitalasset.canton.integration.util.TestSubmissionService
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.protocol.ProtocolProcessor
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.MaliciousParticipantNode
import io.grpc.Status.Code
import monocle.Traversal
import monocle.macros.GenLens
import monocle.macros.syntax.lens.*
import org.scalatest.{Assertion, Tag}

import java.util.List as JList
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

/** Test suite verifying all validation steps required to ensure that all commits on the virtual
  * shared ledger are well-authorized.
  *
  * More precisely, the validation steps ensure that every transaction `tx` committed by an honest
  * participant is well-authorized, i.e., the transaction `tx` paired with its required authorizers
  * is well-authorized as defined in the ledger model and for every required authorizer `ra` of a
  * top-most action `act` that has been committed by an honest participant, one of the following
  * conditions holds:
  *   - A participant has submitted the underlying transaction confirmation request with `ra` as
  *     submitting party and the participant is authorized to submit on behalf of `ra`.
  *   - A malicious participant has approved `act` on behalf of `ra` and the participant is
  *     authorized to approve on behalf of `ra`.
  *
  * '''Note 1:''' The action `act` qualifies as "top-most" if for all actions `act2` that have been
  * committed by an honest participant, if `act2` contains `act`, then `act2` equals `act`. In other
  * words, there is no action `act2` that is closer to a root than `act` and that has also been
  * committed by an honest participant.
  *
  * '''Note 2:''' The above authorization property does not rule out the possibility that an
  * attacker replays a committed request and consequently commits an already committed transaction
  * again, which would result in duplicate obligations for the submitting parties. Such replay
  * attacks will be mitigated by rejecting duplicate requests at the mediator and at the submitting
  * participant; the corresponding test evidence can be found in
  * [[ReplayedRequestsIntegrationTest]].
  *
  * The high-level proof of this property proceeds as follows. Suppose an honest participant `p`
  * commits a transaction `tx`. Firstly, participant `p` has verified that `tx` paired with its
  * required authorizers is well-authorized as defined in the ledger model.
  *
  * Moreover, observe:
  *   - Participant `p` has received a transaction result message with verdict approve for the
  *     underlying request.
  *   - Participant `p` has verified that the root hash of the transaction result message matches
  *     the root hash of the underlying request. Consequently, the full informee tree visible to the
  *     mediator and the transaction view trees visible to the participant coincide modulo blinding
  *     of subtrees.
  *   - Participant `p` has verified that the mediator listed in the transaction view trees is
  *     registered as mediator at the synchronizer.
  *   - The transaction result message contains a signature of the underlying mediator. Therefore,
  *     the message has indeed been sent by a mediator of the underlying synchronizer.
  *   - The mediator has received confirmation responses with verdict "local approve" from all
  *     declared confirming parties of `tx` and for all declared views of `tx`. A declared
  *     confirming party is a party listed as confirming party in the full informee tree (received
  *     by the mediator in Phase 2). A declared view is an unblinded view contained in the full
  *     informee tree.
  *   - The mediator has verified that every confirmation response has been signed by a declared
  *     confirming participant, i.e., a participant authorized to confirm on behalf of the
  *     respective declared confirming parties. The mediator confirmation has therefore really been
  *     sent by the declared confirming participant.
  *   - The mediator has verified that duplicate responses are deduplicated and each confirmation
  *     response is accounted to the right synchronizer, request, and view.
  *   - The mediator has verified that at least one confirmation threshold of the quorums in the
  *     view meets or exceeds the minimum threshold of the underlying confirmation policy so that no
  *     view gets vacuously approved.
  *   - The mediator has verified that the informee tree is full, i.e., for every view in the
  *     transaction, the declared confirming parties are unblinded. Consequently, all views of `tx`
  *     have been approved by the respective confirming participants.
  *   - Recall that a view is identified by its position, which is inherently unique. Hence, a
  *     single confirmation response cannot be accounted to several views.
  *   - Participant `p` has verified that the declared confirming parties, weights, and thresholds
  *     on all views part of `tx` have been declared correctly according to the underlying
  *     confirmation policy.
  *
  * Consequently, for every view in `tx` and every confirming party of the view, there is a
  * participant authorized to confirm on behalf of the party who has sent the verdict "local
  * approve" to the mediator for the view.
  *
  * Finally, let `ra` be a required authorizer of a top-most action `act` that has been committed by
  * an honest participant. Moreover, let `v` be the view underlying `act`. The proof is completed by
  * showing that one of the above Conditions 1 or 2 is satisfied.
  *   - Case 1: the view `v` has been approved by a malicious participant `RA` who is authorized to
  *     confirm on behalf of `ra`. The proof is complete, as the above Condition 2 is satisfied.
  *   - Case 2: the action `act` is at the root of the transaction confirmation request and the view
  *     `v` has been approved by an honest participant `p`. Then, participant `p` has verified:
  *     - The required authorizer `ra` of `act` is also declared as a submitting party of the
  *       transaction confirmation request.
  *     - The declared submitting participant is authorized to submit on behalf of `ra`.
  *     - The request contains a valid signature of the declared submitting participant. So the
  *       declared submitting participant actually submitted the underlying request.
  *     - Overall, the proof is complete, as the above Condition 1 is satisfied.
  *
  *   - Case 3: if the conditions of Case 1 and Case 2 do not hold, the request uses the signatory
  *     confirmation policy.
  *     - Due to the definition of "required authorizer" in the ledger model, `ra` is either a
  *       signatory or an actor of `act`. Every signatory and actor of `act` is also a confirming
  *       party of `v`. Hence, `ra` is also a confirming party of `v`.
  *       - Let `RA` be a participant who is authorized to confirm on behalf of `ra` and who has
  *         approved `v`. As the condition of Case 1 does not hold, `RA` is honest. As the condition
  *         of Case 2 does not hold, `act` is not at the root of the request.
  *       - As part of validating `act`, `RA` has verified that (a) it has also received the parent
  *         action `pact` of `act`, (b) `pact` is suitable for committing. Hence, `pact` paired with
  *         its required authorizers is well-authorized and therefore `ra` is a signatory or actor
  *         of `pact`. As the request uses the signatory confirmation policy, `ra` is a confirming
  *         party of the view underlying `pact`. As the underlying request has been approved and
  *         `pact` is suitable for committing, `RA` has also committed `pact`. The proof is
  *         complete, as we have reached a contradiction to the assumption that `act` is top-most.
  */
trait LedgerAuthorizationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers
    with SecurityTestSuite
    with AccessTestScenario {

  // Workaround to avoid false errors reported by IDEA.
  implicit def tagToContainer(tag: EvidenceTag): Tag = new TagContainer(tag)

  private lazy val authorization: SecurityTest =
    SecurityTest(SecurityTest.Property.Authorization, "virtual shared ledger")

  private def authorizationAttack(threat: String, mitigation: String)(implicit
      lineNo: sourcecode.Line,
      fileName: sourcecode.File,
  ): SecurityTest =
    authorization.setAttack(Attack("a malicious participant", threat, mitigation))

  private lazy val authenticity: SecurityTest =
    SecurityTest(SecurityTest.Property.Authenticity, "virtual shared ledger")

  private def authenticityAttack(threat: String, mitigation: String)(implicit
      lineNo: sourcecode.Line,
      fileName: sourcecode.File,
  ): SecurityTest =
    authenticity.setAttack(Attack("a malicious network participant", threat, mitigation))

  // Using AtomicRef, because this gets read from various threads.
  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()

  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  private var maliciousP1: MaliciousParticipantNode = _

  // hosted on participant1
  private var party1: PartyId = _
  // hosted on participant1
  private var party1Bis: PartyId = _
  // hosted on participant2
  private var party2: PartyId = _
  // hosted on participant3
  private var party3: PartyId = _
  // hosted on participant 1 and 3 with observation permissions on participant3
  private var party13Obs: PartyId = _
  // hosted on participant 2 and 3.
  private var party23: PartyId = _
  // hosted on participant 1 and 2.
  private var party12: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonTestsPath)

        synchronizerOwners1.foreach {
          _.topology.participant_synchronizer_permissions.propose(
            synchronizerId = daId,
            participantId = participant2.id,
            permission = ParticipantPermission.Submission,
          )
        }

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)

        val submissionService = TestSubmissionService(
          participant = participant1,
          checkAuthorization = false,
        )

        maliciousP1 = MaliciousParticipantNode(
          participant1,
          daId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
          testSubmissionServiceOverrideO = Some(submissionService),
        )

        party1 = participant1.adminParty
        party1Bis = participant1.parties.enable(
          "party1Bis"
        )
        party2 = participant2.adminParty

        party3 = participant3.adminParty
        party13Obs = participant1.parties.enable(
          "party13Obs",
          synchronizeParticipants = Seq(participant1, participant3),
        )

        Seq(participant1, participant3).foreach(
          _.topology.party_to_participant_mappings.propose_delta(
            party13Obs,
            adds = List(participant3.id -> ParticipantPermission.Observation),
            store = daId,
          )
        )

        party23 = participant2.parties.enable(
          "party23",
          synchronizeParticipants = Seq(participant2, participant3),
        )

        Seq(participant2, participant3).foreach(
          _.topology.party_to_participant_mappings.propose_delta(
            party23,
            adds = List(participant3.id -> ParticipantPermission.Submission),
            store = daId,
          )
        )

        party12 = participant2.parties.enable(
          "party12",
          synchronizeParticipants = Seq(participant1, participant2),
        )

        Seq(participant2, participant1).foreach { p =>
          p.topology.party_to_participant_mappings.propose_delta(
            party12,
            adds = List(participant1.id -> ParticipantPermission.Submission),
            store = daId,
          )
        }

      }

  lazy val badHash: Hash = Hash
    .build(HashPurpose.MerkleTreeInnerNode, pureCrypto.defaultHashAlgorithm)
    .addWithoutLengthPrefix("pfannkuchen")
    .finish()

  lazy val alertMessage =
    "An error occurred. Please contact the operator and inquire about the request <no-correlation-id> with tid <no-tid>"

  "all honest participants approve the request" when_ { outcome =>
    "the mediator verdict is approve" taggedAs_ { scenario =>
      authorization.setHappyCase(s"$outcome when $scenario.")
    } in { implicit env =>
      import env.*

      // Check that the command can be approved and is idempotent
      submitSimpleP1Cmd()
      submitSimpleP1Cmd()

      // Check that the machinery for replacing verdicts does not introduce errors
      replacingConfirmationResult(
        daId,
        sequencer1,
        mediator1,
        withMediatorVerdict(mediatorApprove),
      ) {
        submitSimpleP1Cmd()
      }
    }
  }

  "all honest participants roll back a view with a security alert" when_ { mitigation =>
    "the mediator approves the request" when {
      "the root action paired with its dynamic authorizers is not well-authorized" taggedAs_ {
        // This test only covers a single situation where a transaction is not well-authorized.
        // Further scenarios should be covered by appropriate unit tests on DAMLE.
        threat => authorizationAttack(threat, mitigation)
      } in { implicit env =>
        import env.*

        // Preparation: Create a contract
        val cid1 = submitP1CreateCmd(
          mkUniversal(
            maintainers = List(party1),
            actors = List(party1),
            observers = List(party2),
          ).create.commands.asScala.toSeq
        )

        // Happy case: replace contract

        // Exercise the contract, creating a new contract with a new maintainer.
        // Happy case: the new maintainer is party1, so the the creation is authorized, as party1 is a signatory of the exercised contract.
        // Unhappy case: the new maintainer is party2, so the creation is not authorized.
        def replaceCmd(
            cid: UniversalContract.ContractId,
            newMaintainer: PartyId,
            newObserverO: Option[PartyId] = None,
        ): CommandsWithMetadata =
          CommandsWithMetadata(
            cid
              .exerciseReplace(
                List(newMaintainer.toProtoPrimitive).asJava,
                List.empty.asJava,
                (List(party2.toProtoPrimitive) ++ newObserverO
                  .map(_.toProtoPrimitive)
                  .toList).asJava,
                List(newMaintainer.toProtoPrimitive).asJava,
                List.empty.asJava,
              )
              .commands
              .asScala
              .toSeq
              .map(c => Command.fromJavaProto(c.toProtoCommand)),
            Seq(party1),
            ledgerTime = environment.now.toLf,
          )

        // This will essentially leave the contract as is.
        val happyCaseCommand = replaceCmd(cid1, party1)
        val (result, happyCaseEvents) =
          trackingLedgerEvents(Seq(participant1, participant2), Seq(party1, party2))(
            maliciousP1.submitCommand(happyCaseCommand).futureValueUS
          )

        result.valueOrFail("Unexpected failure in command submission")
        happyCaseEvents.assertStatusOk(participant1)

        // Both participants create the new contract (child view)
        val p1Created = happyCaseEvents.allCreated(UniversalContract.COMPANION)(participant1)
        val p2Created = happyCaseEvents.allCreated(UniversalContract.COMPANION)(participant2)
        p1Created.loneElement shouldBe p2Created.loneElement

        // Both participants archive the old contract (root view)
        val p1Archived = happyCaseEvents.allArchived(UniversalContract.COMPANION)(participant1)
        val p2Archived = happyCaseEvents.allArchived(UniversalContract.COMPANION)(participant2)
        p1Archived.loneElement shouldBe cid1
        p2Archived.loneElement shouldBe cid1

        val cid2 = p1Created.loneElement.id

        // Unhappy case: replace contract introducing an authorization issue

        def checkAlert(loggerAssertion: String => Assertion)(entry: LogEntry): Assertion =
          entry.shouldBeCantonError(
            LocalRejectError.MalformedRejects.ModelConformance,
            _ should include regex "(?s)DamlException.*FailedAuthorization.*CreateMissingAuthorization",
            loggerAssertion = loggerAssertion,
          )

        // We need to disable the check ensuring the mediator does not approve a transaction we
        // have rejected because this test will trigger it.
        // TODO(i15395): to be adapted when a more graceful check is implemented
        ProtocolProcessor.withApprovalContradictionCheckDisabled(loggerFactory) {
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            {
              // Create a new version of the contract with party2 as signatory
              // without authorization from party2.
              val unhappyCaseCommand = replaceCmd(cid2, party2, Some(party3))

              val ((_, maliciousCaseEvents), _) =
                // Force approval by the mediator to check if the participants partly rollback nevertheless.
                replacingConfirmationResult(
                  daId,
                  sequencer1,
                  mediator1,
                  withMediatorVerdict(mediatorApprove),
                )(
                  trackingLedgerEvents(Seq(participant1, participant2), Seq(party1, party2))(
                    maliciousP1.submitCommand(unhappyCaseCommand).futureValueUS
                  )
                )

              // The transaction succeeds because the child view passes the conformance check
              maliciousCaseEvents.assertStatusOk(participant1)

              // Both participants create the new contract (child view)
              val p1Created =
                maliciousCaseEvents.allCreated(UniversalContract.COMPANION)(participant1)
              val p2Created =
                maliciousCaseEvents.allCreated(UniversalContract.COMPANION)(participant2)
              p1Created.loneElement shouldBe p2Created.loneElement

              // No participant archives the original contract because the root view is rolled back
              val p1Archived =
                maliciousCaseEvents.allArchived(UniversalContract.COMPANION)(participant1)
              val p2Archived =
                maliciousCaseEvents.allArchived(UniversalContract.COMPANION)(participant2)
              p1Archived shouldBe empty
              p2Archived shouldBe empty
            },
            LogEntry.assertLogSeq(
              assertPhase3Alert(checkAlert, "model conformance check")(
                participant1,
                participant2,
              )
            ),
          )
        }
      }
    }
  }

  "all honest participants roll back the request" when_ { mitigation =>
    "the request gets rejected by the mediator with verdict 'participant reject' (e.g. use of blocked contract)" taggedAs_ {
      threat => authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      replacingConfirmationResult(
        daId,
        sequencer1,
        mediator1,
        withMediatorVerdict(participantReject(participant1)),
      ) {
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submitSimpleP1Cmd(),
          _.shouldBeCommandFailure(localReject.code),
        )
      }
    }

    "the request gets rejected by the mediator with verdict 'mediator reject' (e.g. malformed message)" taggedAs_ {
      threat => authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      replacingConfirmationResult(
        daId,
        sequencer1,
        mediator1,
        withMediatorVerdict(mediatorReject),
      ) {
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submitSimpleP1Cmd(),
          _.commandFailureMessage should include(
            "INVALID_ARGUMENT/An error occurred. Please contact the operator and inquire about the request"
          ),
        )
      }
    }

    "the mediator does not send a result message" taggedAs_ { threat =>
      authorization.setAttack(Attack("an overloaded mediator", threat, mitigation))
    } in { implicit env =>
      import env.*

      replacingConfirmationResult(
        daId,
        sequencer1,
        mediator1,
        // The result message is dropped, because we don't specify a replacement
      ) {
        loggerFactory.assertThrowsAndLogsUnordered[CommandFailure](
          submitSimpleP1Cmd(),
          _.shouldBeCommandFailure(LocalRejectError.TimeRejects.LocalTimeout),
        )
      }
    }

    "the full informee tree sent to the mediator is not consistent with the transaction trees validated by the participants" taggedAs_ {
      threat => authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      // For simplicity, the test tweaks the root hash of the result message instead of the full informee tree.
      // That way, confirmation responses don't have to be modified.
      val hashLens =
        GenLens[ConfirmationResultMessage](_.rootHash).replace(RootHash(badHash))

      replacingConfirmationResult(
        daId,
        sequencer1,
        mediator1,
        // Approve the request, but use a flawed root hash
        signedTransformOf(
          traverseMessages[ConfirmationResultMessage](_)
            .modify(
              hashLens >>>
                GenLens[ConfirmationResultMessage](_.verdict).replace(mediatorApprove)
            )(_)
        ),
        // Approve the request using the correct root hash
        withMediatorVerdict(mediatorApprove),
      ) {
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          // Run this test more than once, because there is a rare interleaving where the alarm does not occur.
          // By running it several times, the alarm should occur at least once.
          (0 until 4).foreach(_ => submitSimpleP1Cmd()),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonError(
                  SyncServiceAlarm,
                  _ should fullyMatch regex raw"Received a confirmation result message at \S+ from [ \S]+ for RequestId\(\S+\) with an invalid root hash \S+ instead of \S+\. Discarding message\.\.\.",
                ),
                "failed root hash check",
              )
            ),
            // These errors occur in the rare case that the correct verdict is sequenced first:
            Seq(
              _.shouldBeCantonError(
                SyncServiceAlarm,
                _ should startWith("Failed to add result for RequestId"),
              ),
              _.warningMessage should include("Failed to process result RequestNotFound"),
            ),
          ),
        )
      }
    }
  }

  "all honest participants discard the transaction result message with a security alert" when_ {
    mitigation =>
      "the signature of the transaction result message does not verify under the mediator's public key" taggedAs_ {
        threat => authenticityAttack(threat, mitigation)
      } in { implicit env =>
        import env.*

        replacingConfirmationResult(
          daId,
          sequencer1,
          mediator1,
          // Approve with invalid signature (i.e., using participant1's private key).
          withMediatorVerdict(mediatorApprove, participant1),
          // Approve with the correct signature
          withMediatorVerdict(mediatorApprove),
        ) {
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            // Run this test more than once, because there is a rare interleaving where the alarm does not occur.
            // By running it several times, the alarm should occur at least once.
            for (_ <- 0 until 10 if loggerFactory.numberOfRecordedEntries == 0) {
              submitSimpleP1Cmd()
            },
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.shouldBeCantonError(
                    SyncServiceAlarm,
                    _ should fullyMatch regex raw"Received a confirmation result at \S+ for RequestId\(\S+\) with an " +
                      raw"invalid signature for [ \S]+\. Discarding message\.\.\. Details: MultipleErrors\(\s+" +
                      raw"message = MediatorGroup\(\S+\) signature threshold not reached,\s+" +
                      raw"errors = SignatureWithWrongKey\(\s+Key \S+ used to generate signature is not a valid key for MediatorGroup\(\S+\)\.\s+" +
                      raw"Valid keys are\s+List\(\S+\)\s+\)\s+\)",
                  ),
                  "failed signature verification",
                )
              )
            ),
          )
        }
      }
  }

  "the mediator rejects the request" when_ { mitigation =>
    "a confirming participant rejects the request" taggedAs_ { threat =>
      authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      replacingConfirmationResponses(
        participant1,
        sequencer1,
        daId,
        withLocalVerdict(localReject),
      ) {
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submitSimpleP1Cmd(),
          _.shouldBeCommandFailure(localReject.code),
        )
      }
    }

    "a confirming participant does not send a confirmation response" taggedAs_ { threat =>
      authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      replacingConfirmationResponses(participant1, sequencer1, daId) {
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submitSimpleP1Cmd(),
          _.warningMessage should startWith regex "Response message for request .* timed out at",
          _.shouldBeCommandFailure(MediatorError.Timeout),
        )
      }
    }
  }

  "the mediator discards the confirmation response with a security alert" when_ { mitigation =>
    "a confirmation response has an invalid signature" taggedAs_ { threat =>
      authenticityAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      replacingConfirmationResponses(
        participant1,
        sequencer1,
        daId,
        // Approve with an invalid signature (i.e. by mediator instead of participant)
        // The sequencer checks the signature of the `ConfirmationResponse` envelopes against the sender
        // So we set the sender and sign the submission request appropriately here.
        withLocalVerdict(localApprove, mediator1),
        // Reject with a valid signature
        withLocalVerdict(localReject),
      ) {
        loggerFactory.assertThrowsAndLogsUnordered[CommandFailure](
          submitSimpleP1Cmd(),
          _.warningMessage should include("Unexpected message from MED"),
          _.shouldBeCantonError(
            MediatorError.MalformedMessage,
            _ should include("invalid signature"),
          ),
          _.shouldBeCommandFailure(localReject.code),
        )
      }
    }

    "a confirming participant does not host one of the confirming parties" taggedAs_ { threat =>
      authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      replacingConfirmationResponses(
        participant1,
        sequencer1,
        daId,
        // Approve by participant2 who does not host a confirming party
        signedTransformWithSender(
          traverseMessages[ConfirmationResponses](_)
            .modify(
              ConfirmationResponses.senderUnsafe.replace(participant2.id) >>>
                ConfirmationResponses.localVerdictUnsafe.replace(localApprove)
            )(_),
          senderRef = participant2,
          // Using current snapshot because participant2's crypto client does not yet have a snapshot for the request id.
          useCurrentSnapshot = true,
          Some(environment.now),
        ),
        // Authorized reject
        withLocalVerdict(localReject),
      ) {
        def checkMessage(msg: String): Assertion =
          msg should fullyMatch regex raw"Received an unauthorized confirmation response at .* by ${participant2.id} for request .* on behalf of Set\(${party1.toLf}\)\. Discarding response\.\.\."

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          submitSimpleP1Cmd(),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonError(MediatorError.MalformedMessage, checkMessage),
                "authorization error",
              ),
              (_.shouldBeCommandFailure(localReject.code), "injected rejection"),
            )
          ),
        )
      }
    }

    "the confirming participant is not authorized to confirm on behalf of the confirming parties" taggedAs_ {
      threat => authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      replacingConfirmationResponses(
        participant1,
        sequencer1,
        daId,
        // Approve by participant3 who is not authorized to confirm
        signedTransformWithSender(
          traverseMessages[ConfirmationResponses](_)
            .modify(
              ConfirmationResponses.senderUnsafe.replace(participant3.id) >>>
                ConfirmationResponses.localVerdictUnsafe.replace(localApprove)
            )(_),
          senderRef = participant3,
        ),
        // Authorized reject
        withLocalVerdict(localReject),
      ) {
        def checkMessage(msg: String): Assertion =
          msg should fullyMatch regex raw"Received an unauthorized confirmation response at .* by ${participant3.id} for request .* on behalf of Set\(.*\)\. Discarding response\.\.\."

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submitSimpleP1Cmd(List(party13Obs)),
          _.shouldBeCantonError(MediatorError.MalformedMessage, checkMessage),
          _.shouldBeCantonError(MediatorError.MalformedMessage, checkMessage),
          _.shouldBeCommandFailure(localReject.code),
        )
      }
    }

    "the root hash of the confirmation response does not match the root hash of the request" taggedAs_ {
      threat => authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      replacingConfirmationResponses(
        participant1,
        sequencer1,
        daId,
        // Approve with incorrect root hash
        signedTransformOf(
          traverseMessages[ConfirmationResponses](_)
            .modify(
              ConfirmationResponses.rootHashUnsafe.replace(RootHash(badHash)) >>>
                ConfirmationResponses.localVerdictUnsafe.replace(localApprove)
            )(_)
        ),
        // Reject with correct root hash
        withLocalVerdict(localReject),
      ) {
        def checkMessage(msg: String): Assertion =
          msg should fullyMatch regex raw"Received a confirmation response at .* by ${participant1.id} for request .* with an invalid root hash .* instead of .*\. Discarding response\.\.\."

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submitSimpleP1Cmd(),
          _.shouldBeCantonError(MediatorError.MalformedMessage, checkMessage),
          _.shouldBeCantonError(MediatorError.MalformedMessage, checkMessage),
          _.shouldBeCommandFailure(localReject.code),
        )
      }
    }

    "the view position does not exist in the full informee tree of the request" taggedAs_ {
      threat =>
        authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      replacingConfirmationResponses(
        participant1,
        sequencer1,
        daId,
        // Approve with incorrect view hash
        signedTransformOf(
          traverseMessages[ConfirmationResponses](_)
            .modify(
              ConfirmationResponses.viewPositionOUnsafe.replace(Some(ViewPosition.root)) >>>
                ConfirmationResponses.localVerdictUnsafe.replace(localApprove)
            )(_)
        ),
        // Reject with correct view hash
        withLocalVerdict(localReject),
      ) {
        def checkMessage(msg: String): Assertion =
          msg should fullyMatch regex raw"Received a confirmation response at .* by ${participant1.id} for request .* with an unknown view position .*\. Discarding response\.\.\."

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submitSimpleP1Cmd(),
          _.shouldBeCantonError(MediatorError.MalformedMessage, checkMessage),
          _.shouldBeCantonError(MediatorError.MalformedMessage, checkMessage),
          _.shouldBeCommandFailure(localReject.code),
        )
      }
    }

    "a confirming party has not been declared in the full informee tree" taggedAs_ { threat =>
      authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      replacingConfirmationResponses(
        participant1,
        sequencer1,
        daId,
        // Approve by participant2 on behalf of party2. Party2 is not a stakeholder.
        signedTransformWithSender(
          traverseMessages[ConfirmationResponses](_)
            .modify(
              ConfirmationResponses.senderUnsafe.replace(participant2.id) >>>
                ConfirmationResponses.confirmingPartiesUnsafe.replace(Set(party2.toLf)) >>>
                ConfirmationResponses.localVerdictUnsafe.replace(localApprove)
            )(_),
          senderRef = participant2,
          // Using current snapshot because participant2's crypto client does not yet have a snapshot for the request id.
          useCurrentSnapshot = true,
          Some(environment.now),
        ),
        // Reject on behalf of stakeholder party1.
        withLocalVerdict(localReject),
      ) {
        def checkMessage(msg: String): Assertion =
          msg should fullyMatch regex raw"Received a confirmation response at .* by ${participant2.id} for request .* with unexpected confirming parties Set\(${party2.toLf}\)\. Discarding response\.\.\."

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submitSimpleP1Cmd(),
          _.shouldBeCantonError(MediatorError.MalformedMessage, checkMessage),
          _.shouldBeCantonError(MediatorError.MalformedMessage, checkMessage),
          _.shouldBeCommandFailure(localReject.code),
        )
      }
    }
  }

  "the mediator discards a confirmation response with a security alert in case of contradictory verdicts" when_ {
    mitigation =>
      // The mediator does not alert on duplicate responses with the same verdict, but warn in the case of a duplicate response with contradictory verdicts.
      "the mediator receives several responses from the same confirming party for the same view and request" taggedAs_ {
        threat =>
          authorizationAttack(threat, mitigation)
      } in { implicit env =>
        import env.*

        def updateResponse(
            confirmingParty: PartyId,
            localVerdict: LocalVerdict,
        ): SignedMessageTransform[ConfirmationResponses] =
          signedTransformOf(
            traverseMessages[ConfirmationResponses](_)
              .modify(
                ConfirmationResponses.confirmingPartiesUnsafe.replace(Set(confirmingParty.toLf)) >>>
                  ConfirmationResponses.localVerdictUnsafe.replace(localVerdict)
              )(_)
          )

        replacingConfirmationResponses(
          participant1,
          sequencer1,
          daId,
          // Valid approve by party1
          updateResponse(party1, localApprove),
          // Duplicate approve by party1, to be discarded
          updateResponse(party1, localApprove),
          // Duplicate Reject by party1, to be discarded and logged
          updateResponse(party1, localReject.toLocalReject(testedProtocolVersion)),
          // Valid reject by party1Bis
          updateResponse(party1Bis, localReject.toLocalReject(testedProtocolVersion)),
        ) {
          loggerFactory.assertThrowsAndLogsUnorderedOptional[CommandFailure](
            submitSimpleP1Cmd(actAs = List(party1, party1Bis)),
            LogEntryOptionality.Required -> (_.shouldBeCommandFailure(localReject.code)),
            // The duplicate receipts may sometimes be delivered together to participant1
            LogEntryOptionality.Optional -> (_.warningMessage should include(
              "Ignoring duplicate SequencedSubmission"
            )),
            LogEntryOptionality.Required -> ((logEntry: LogEntry) => {
              logEntry.shouldBeCantonErrorCode(ParticipantEquivocation.code)
              logEntry.warningMessage should include(
                s"Ignoring a rejection verdict for ${participant1.id} because it has already responded for party ${party1.toLf} with an approval verdict"
              )
            }),
          )
        }
      }
  }

  "the mediator discards a confirmation response without a security alert" when_ { mitigation =>
    "the request id is unknown" taggedAs_ { threat =>
      authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      replacingConfirmationResponses(
        participant1,
        sequencer1,
        daId,
        // Approve with invalid requestId
        signedTransformOf(
          traverseMessages[ConfirmationResponses](_)
            .modify(
              // Choose immediate successor of requestId, because:
              // - It's lower than the timestamp of this message. So a topology snapshot will be available.
              // - It's not related to a known requestId, because requestId is the highest known request id.
              ConfirmationResponses.requestIdUnsafe.modify(requestId =>
                RequestId(requestId.unwrap.immediateSuccessor)
              ) >>>
                ConfirmationResponses.localVerdictUnsafe.replace(localApprove)
            )(_)
            // Change the topology timestamp in the same way as the requestId
            .focus(_.topologyTimestamp)
            .modify(_.map(_.immediateSuccessor))
        ),
        // Reject with correct requestId
        withLocalVerdict(localReject),
      ) {
        def checkMessage(msg: String): Assertion =
          msg should startWith regex raw"Received a confirmation response at .* by ${participant1.id} with an unknown request id .*\. Discarding response\.\.\."

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submitSimpleP1Cmd(),
          _.shouldBeCantonError(MediatorError.InvalidMessage, checkMessage),
          _.shouldBeCommandFailure(localReject.code),
        )
      }
    }
  }

  "the mediator rejects the request with a security alert" when_ { mitigation =>
    "the mediator receives a view with confirmation threshold below the minimum threshold" taggedAs_ {
      threat =>
        authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      val cmd = simpleP1Cmd()
      val cmdWithMetadata =
        CommandsWithMetadata(cmd, actAs = Seq(party1), ledgerTime = environment.now.toLf)

      runRequestWithZeroThreshold(cmdWithMetadata)
    }

    def runRequestWithZeroThreshold(
        cmdWithMetadata: CommandsWithMetadata
    )(implicit
        env: TestConsoleEnvironment
    ): Assertion = {
      import env.*

      def checkMessage(message: String): Assertion =
        message should fullyMatch regex raw"Received a mediator confirmation request with id .* for transaction view at .*, " +
          raw"where no quorum of the list satisfies the minimum threshold\. Rejecting request\.\.\."
      val (_, events) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        trackingLedgerEvents(Seq(participant1, participant2), Seq.empty)(
          maliciousP1
            .submitCommand(
              cmdWithMetadata,
              transactionTreeInterceptor = setOneThresholdToZero,
            )
            .futureValueUS
        ),
        LogEntry.assertLogSeq(
          Seq(
            (_.shouldBeCantonError(MediatorError.MalformedMessage, checkMessage), "mediator"),
            (
              _.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.ModelConformance),
              "participants",
            ),
          ),
          Seq(
            _.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.MalformedRequest)
          ),
        ),
      )

      events
        .assertExactlyOneCompletion(participant1)
        .status
        .value
        .code shouldBe Code.INVALID_ARGUMENT.value()
      events.assertNoTransactions()
    }

    lazy val setOneThresholdToZero: GenTransactionTree => GenTransactionTree =
      GenTransactionTree.rootViewsUnsafe
        .andThen(firstElement[TransactionView])
        .andThen(TransactionView.Optics.viewCommonDataUnsafe)
        .andThen(MerkleTree.tryUnwrap[ViewCommonData])
        .andThen(GenLens[ViewCommonData](_.viewConfirmationParameters))
        .modify { oldViewConfirmationParameters =>
          ViewConfirmationParameters.tryCreate(
            oldViewConfirmationParameters.informees,
            oldViewConfirmationParameters.quorums.map(_.copy(threshold = NonNegativeInt.zero)),
          )
        }

    "the mediator receives an informee tree that is not full (i.e., a view common data is blinded)" taggedAs_ {
      threat =>
        authorizationAttack(threat, mitigation)
    } in { implicit env =>
      import env.*

      val cmd = simpleP1Cmd()
      val cmdWithMetadata =
        CommandsWithMetadata(cmd, actAs = Seq(party1), ledgerTime = environment.now.toLf)

      def blindOneViewCommonDataInInformeeTree(
          confirmationRequest: TransactionConfirmationRequest
      ): TransactionConfirmationRequest =
        confirmationRequest
          .focus(_.informeeMessage.fullInformeeTree)
          .andThen(FullInformeeTree.genTransactionTreeUnsafe)
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(firstElement[TransactionView])
          .andThen(TransactionView.Optics.viewCommonDataUnsafe)
          .modify(_.blindFully)

      def checkError(code: ErrorCode, loggerName: String, member: String, rx: String)(
          logEntry: LogEntry
      ): Assertion =
        logEntry.shouldBeCantonError(
          code,
          _ should fullyMatch regex rx,
          loggerAssertion = _ should include regex raw"\.$loggerName:.*/$member",
        )

      val blindingErrorRx =
        raw"OtherError\(Unable to create full informee tree: The view common data in a full informee tree must be unblinded. Found BlindedNode\(\S+\)\.\)"
      val rejectionRx =
        raw"Received an envelope at \S+ that cannot be opened\. Discarding envelope\.\.\. Reason: $blindingErrorRx"
      val unknownRequestRx =
        raw"Received a confirmation response at \S+ by PAR::participant1::\S+ with an unknown request id RequestId(\S+). Discarding response..."

      val (_, trackingResult) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        trackingLedgerEvents(Seq(participant1), Seq(party1)) {
          maliciousP1
            .submitCommand(
              cmdWithMetadata,
              confirmationRequestInterceptor = blindOneViewCommonDataInInformeeTree,
            )
            .futureValueUS
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              // The mediator notices the malformed full informee tree
              checkError(
                MediatorError.MalformedMessage,
                "Mediator",
                "mediator=mediator1",
                rejectionRx,
              ),
              "mediator alert",
            ),
            (
              // The mediator receives responses that don't match any ongoing transaction confirmation request (since it was rejected).
              // This is logged twice.
              checkError(
                MediatorError.InvalidMessage,
                "ConfirmationRequestAndResponseProcessor",
                "mediator=mediator1",
                unknownRequestRx,
              ),
              "confirmation response",
            ),
          )
        ),
      )

      trackingResult
        .assertExactlyOneCompletion(participant1)
        .status
        .value
        .code shouldBe Code.INVALID_ARGUMENT.value()
      trackingResult.assertNoTransactions()

      // Flush participant1
      assertPingSucceeds(participant1, participant1)

    }

    "the mediator receives a full informee tree with non-unique view hashes" taggedAs_ (threat =>
      authorizationAttack(threat, mitigation)
    ) in { implicit env =>
      import env.*

      val cid = submitP1CreateCmd(
        mkUniversal(
          maintainers = List(party1),
          actors = List(party1),
        ).create.commands.asScala.toSeq
      )

      val cmd = cid
        .exerciseTouch(JList.of(): JList[String])
        .commands
        .asScala
        .toSeq
        .map(c => Command.fromJavaProto(c.toProtoCommand))

      // Submitting the same exeN command twice so that we have two identical actions at the root of the transaction
      // Happy case:
      participant1.ledger_api.commands.submit(Seq(party1), cmd ++ cmd)

      // Unhappy case:
      val (_, events) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        trackingLedgerEvents(Seq(participant1), Seq.empty) {

          val cmdWithMetadata =
            CommandsWithMetadata(
              cmd,
              actAs = Seq(party1),
              ledgerTime = environment.now.toLf,
            )

          val withNonUniqueHashes: GenTransactionTree => GenTransactionTree =
            GenTransactionTree.rootViewsUnsafe.modify { rootViews =>
              val rootViewsSeq = rootViews.unblindedElements
              val res =
                MerkleSeq.fromSeq(pureCrypto, testedProtocolVersion)(rootViewsSeq ++ rootViewsSeq)
              res
            }

          maliciousP1
            .submitCommand(
              cmdWithMetadata,
              transactionTreeInterceptor = withNonUniqueHashes,
            )
            .futureValueUS
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCantonError(
                MalformedMessage,
                _ should fullyMatch regex raw"Received an envelope at \S+ that cannot be opened\. " +
                  raw"Discarding envelope\.\.\. Reason: " +
                  raw"OtherError\(Unable to create transaction tree: " +
                  raw"A transaction tree must contain a hash at most once. Found the hash \S+ twice\.\)",
                loggerAssertion = _ should include("mediator=mediator1"),
              ),
              "mediator parse error",
            ),
            (
              _.shouldBeCantonError(
                InvalidMessage,
                _ should include("with an unknown request id"),
                loggerAssertion = _ should include("mediator=mediator1"),
              ),
              "mediator invalid message",
            ),
            (
              _.warningMessage should include regex raw"Unable to create transaction tree: " +
                raw"A transaction tree must contain a hash at most once\. Found the hash \S+ twice\.",
              "participant deserialize error",
            ),
          )
        ),
      )

      events.assertNoCompletionsExceptFor(participant1)
      events.assertNoTransactions()
    }
  }

  "every honest participant rolls back the request with a security alert" when_ { mitigation =>
    "a request has a declared confirming party that is not allowed to confirm" taggedAs_ (threat =>
      authorizationAttack(
        threat,
        mitigation,
      )
    ) in { implicit env =>
      import env.*

      testInvalidConfirmingParties(
        List(party1),
        List.empty,
        // Add party2 as confirming party and leave threshold as is.
        firstViewCommonData
          .modify(
            _.focus(_.viewConfirmationParameters)
              .modify { oldViewConfirmationParameters =>
                // there is only one quorum Map(participant1:: -> weight:1), threshold = 1
                val quorum = oldViewConfirmationParameters.quorums.head
                ViewConfirmationParameters.tryCreate(
                  oldViewConfirmationParameters.informees + party2.toLf,
                  Seq(
                    Quorum(
                      oldViewConfirmationParameters.quorums.flatMap { case Quorum(confirmers, _) =>
                        confirmers
                      }.toMap ++
                        Map(party2.toLf -> 1),
                      quorum.threshold,
                    )
                  ),
                )

              }
          ),
        LogEntry.assertLogSeq(
          assertPhase3Alert(assertViewReconstructionErrorForLogger, "view reconstruction error")(
            participant1,
            participant2,
          )
        ),
      )
    }

    def testInvalidConfirmingParties(
        maintainers: List[PartyId],
        observers: List[PartyId],
        interceptor: GenTransactionTree => GenTransactionTree,
        logAssertion: Seq[LogEntry] => Assertion,
    )(implicit env: TestConsoleEnvironment): Assertion = {
      import env.*

      val cmd =
        CommandsWithMetadata(
          mkUniversal(
            maintainers = maintainers,
            actors = maintainers,
            observers = observers,
          ).create.commands.asScala.toSeq
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          maintainers,
          ledgerTime = environment.now.toLf,
        )

      // We need to disable the check ensuring the mediator does not approve a transaction we
      // have rejected because this test will trigger it.
      // TODO(i15395): to be adapted when a more graceful check is implemented
      ProtocolProcessor.withApprovalContradictionCheckDisabled(loggerFactory) {
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          replacingConfirmationResult(
            daId,
            sequencer1,
            mediator1,
            withMediatorVerdict(mediatorApprove),
          ) {
            val (_, events) = trackingLedgerEvents(participants.all, Seq.empty)(
              maliciousP1
                .submitCommand(
                  cmd,
                  transactionTreeInterceptor = interceptor,
                )
                .futureValueUS
            )

            events.assertNoCompletionsExceptFor(participant1)
            events.assertNoTransactions()
          }._1,
          logAssertion,
        )
      }
    }

    def assertViewReconstructionErrorForLogger(
        loggerAssertion: String => Assertion
    ): LogEntry => Assertion =
      _.shouldBeCantonError(
        LocalRejectError.MalformedRejects.ModelConformance,
        _ should startWith regex raw"(?s)Rejected transaction due to a failed model conformance check: .*ViewReconstructionError\(",
        loggerAssertion = loggerAssertion,
      )

    "a confirming party is not declared in a request" taggedAs_ (threat =>
      authorizationAttack(
        threat,
        mitigation,
      )
    ) in { implicit env =>
      import env.*

      testInvalidConfirmingParties(
        List(party1, party1Bis),
        List(party2),
        // Remove party1 as confirming party and adjust threshold so that a reject by party1 would be ignored.
        firstViewCommonData
          .modify(
            _.focus(_.viewConfirmationParameters)
              .replace {
                ViewConfirmationParameters.tryCreate(
                  Set(party1Bis.toLf, party2.toLf),
                  Seq(
                    Quorum(
                      Map(party1Bis.toLf -> 1),
                      NonNegativeInt.one,
                    )
                  ),
                )
              }
          ),
        LogEntry.assertLogSeq(
          assertPhase3Alert(assertViewReconstructionErrorForLogger, "view reconstruction error")(
            participant1,
            participant2,
          )
        ),
      )
    }

    "a request has a declared confirming party with an incorrect weight" taggedAs_ (threat =>
      authorizationAttack(
        threat,
        mitigation,
      )
    ) in { implicit env =>
      import env.*

      testInvalidConfirmingParties(
        List(party1, party1Bis),
        List(party2),
        // Convert party1Bis to an informee and adjust the threshold so that a reject by party2 would have no effect.
        firstViewCommonData
          .modify(
            _.focus(_.viewConfirmationParameters)
              .replace {
                ViewConfirmationParameters.tryCreate(
                  Set(party1.toLf, party1Bis.toLf, party2.toLf),
                  Seq(
                    Quorum(
                      Map(party1.toLf -> 1),
                      NonNegativeInt.one,
                    )
                  ),
                )
              }
          ),
        LogEntry.assertLogSeq(
          assertPhase3Alert(assertViewReconstructionErrorForLogger, "view reconstruction error")(
            participant1,
            participant2,
          )
        ),
      )
    }

    "a request has a view with an incorrect threshold" taggedAs_ (threat =>
      authorizationAttack(
        threat,
        mitigation,
      )
    ) in { implicit env =>
      import env.*

      testInvalidConfirmingParties(
        List(party1, party1Bis),
        List(party2),
        // Lower the threshold from 2 to 1 so that a reject by one party would have no effect.
        firstViewCommonData.modify(
          _.focus(_.viewConfirmationParameters)
            .modify { oldViewConfirmationParameters =>
              ViewConfirmationParameters.tryCreate(
                oldViewConfirmationParameters.informees,
                oldViewConfirmationParameters.quorums.map(_.copy(threshold = NonNegativeInt.one)),
              )
            }
        ),
        LogEntry.assertLogSeq(
          assertPhase3Alert(assertViewReconstructionErrorForLogger, "view reconstruction error")(
            participant1,
            participant2,
          )
        ),
      )
    }

    "a request contains a view with no declared confirming parties and a zero threshold" taggedAs_ (
      threat =>
        authorizationAttack(
          threat,
          mitigation,
        )
    ) in { implicit env =>
      import env.*

      testInvalidConfirmingParties(
        List(party1),
        List(party2),
        // Convert confirming parties to informees and set threshold to zero so that the mediator may vacuously approve.
        firstViewCommonData.modify(
          _.focus(_.viewConfirmationParameters)
            .replace {
              ViewConfirmationParameters.tryCreate(
                Set(party1.toLf, party2.toLf),
                Seq(Quorum.empty),
              )
            }
        ),
        LogEntry.assertLogSeq(
          assertPhase3Alert(assertViewReconstructionErrorForLogger, "view reconstruction error")(
            participant1,
            participant2,
          ) :+
            // Alert at Mediator
            (
              _.shouldBeCantonError(
                MediatorError.MalformedMessage,
                _ should fullyMatch regex raw"Received a mediator confirmation request with id RequestId\S+ for transaction view at ViewPosition\S+, where no quorum of the list satisfies the minimum threshold\. Rejecting request\.\.\.",
              ),
              "mediator",
            )
        ),
      )
    }
  }

  "Ensure that the signatories and actors are the confirming parties" in { implicit env =>
    import env.*
    lazy val nodes = Table(
      ("node", "expected confirming parties"),
      (
        ExampleTransactionFactory.createNode(mock[LfContractId], signatories = Set(party1.toLf)),
        Set(party1),
      ),
      (
        ExampleTransactionFactory.exerciseNode(
          mock[LfContractId],
          signatories = Set(party1.toLf),
          actingParties = Set(party2.toLf),
        ),
        Set(party1, party2),
      ),
      (
        ExampleTransactionFactory.exerciseNode(
          mock[LfContractId],
          consuming = false,
          signatories = Set(party1.toLf),
          actingParties = Set(party2.toLf),
        ),
        Set(party1, party2),
      ),
      (
        ExampleTransactionFactory.fetchNode(
          mock[LfContractId],
          actingParties = Set(party1.toLf),
          signatories = Set(party2.toLf),
        ),
        Set(party1, party2),
      ),
      (
        ExampleTransactionFactory
          .lookupByKeyNode(
            ExampleTransactionFactory.defaultGlobalKey,
            maintainers = Set(party1.toLf),
          ),
        Set(party1),
      ),
    )

    forEvery(nodes) { case (node, expectedConfirmingParties) =>
      val (informeesWithParticipantData, _) =
        TransactionViewDecompositionFactory
          .informeesParticipantsAndThreshold(
            node,
            ExampleTransactionFactory.defaultTopologySnapshot,
          )
          .futureValueUS
      val informees = informeesWithParticipantData.fmap(_._2)

      forEvery(expectedConfirmingParties) { p =>
        informees should contain(p.toLf -> NonNegativeInt.one)
      }
    }
  }

  "the participant rejects the request with a security alert" when_ { mitigation =>
    "a required authorizer of a root action is not declared as submitting party" taggedAs_ (
      threat => authorizationAttack(threat, mitigation)
    ) in { implicit env =>
      import env.*

      val cmd = CommandsWithMetadata(
        simpleCreateCmd(List(party1, party2)),
        Seq(party1),
        ledgerTime = environment.now.toLf,
      )

      def checkMessage(msg: String): Assertion =
        msg should include regex
          raw"""Received a request with id \S+ with a view that is not correctly authorized\. Rejecting request\.\.\.
                 |Missing authorization for participant2\S+ through the submitting parties\.""".stripMargin

      val (_, events) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        checkingConfirmationResponses(Seq(participant1, participant2), sequencer1) {
          trackingLedgerEvents(participants.all, Seq.empty) {
            maliciousP1.submitCommand(cmd).futureValueUS
          }
        } { responses =>
          forEvery(responses) { case (_, response) =>
            inside(response.loneElement.localVerdict) { case LocalReject(reason, true) =>
              reason.message shouldBe alertMessage
            }
          }

          responses.keySet shouldBe Set(participant1.id, participant2.id)
        },
        LogEntry.assertLogSeq(
          assertPhase3Alert(
            loggerAssertion =>
              _.shouldBeCantonError(
                LocalRejectError.MalformedRejects.MalformedRequest,
                checkMessage,
                loggerAssertion = loggerAssertion,
              ),
            "authorization error",
          )(participant1, participant2)
        ),
      )

      events.assertNoCompletionsExceptFor(participant1)
      events.assertNoTransactions()
    }

    "the declared submitting participant is not authorized to submit on behalf of the submitting parties" taggedAs_ (
      threat => authorizationAttack(threat, mitigation)
    ) in { implicit env =>
      import env.*

      val cmd = CommandsWithMetadata(
        simpleCreateCmd(List(party1, party2)),
        Seq(party1, party2),
        ledgerTime = environment.now.toLf,
      )

      def checkMessage(msg: String): Assertion =
        msg should include regex
          raw"""Received a request with id \S+ with a view that is not correctly authorized\. Rejecting request\.\.\.
                 |The submitting participant PAR::participant1\S+\. is not authorized to submit on behalf of the submitting parties participant2\S+\.""".stripMargin

      val (_, events) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        checkingConfirmationResponses(participants.all, sequencer1) {
          trackingLedgerEvents(participants.all, Seq.empty) {
            maliciousP1.submitCommand(cmd).futureValueUS
          }
        } { responses =>
          forEvery(responses) { case (_, response) =>
            inside(response.loneElement.localVerdict) { case LocalReject(reason, true) =>
              reason.message shouldBe alertMessage
            }
          }

          responses.keySet shouldBe Set(participant1.id, participant2.id)
        },
        LogEntry.assertLogSeq(
          assertPhase3Alert(
            loggerAssertion =>
              _.shouldBeCantonError(
                LocalRejectError.MalformedRejects.MalformedRequest,
                checkMessage,
                loggerAssertion = loggerAssertion,
              ),
            "authorization error",
          )(participant1, participant2)
        ),
      )

      events.assertNoCompletionsExceptFor(participant1)
      events.assertNoTransactions()
    }

    "the submitting party has decentralized their ACS management" taggedAs_ (threat =>
      authorizationAttack(threat, mitigation)
    ) in { implicit env =>
      import env.*

      // Create a multi-hosted party with confirmation threshold > 1
      // Then no hosting participant may submit transactions even if they have submission permission.
      val hostingParticipants = NonEmpty(Seq, participant1, participant2)
      val decentralizedParties = hostingParticipants.map(
        _.topology.party_to_participant_mappings
          .propose(
            party = PartyId(participant1.id.uid.tryChangeId("decentralized-acs-party")),
            newParticipants = hostingParticipants.map(_.id -> ParticipantPermission.Submission),
            threshold = 2,
            store = daId,
          )
          .transaction
          .mapping
          .partyId
      )
      decentralizedParties.distinct.forgetNE should have size 1
      val decentralizedParty = decentralizedParties.head1
      // Wait until the party shows up on all hosting participants
      // In particular, this ensures that participant1 has processed the topology transaction from participant2
      // Otherwise, the command submission would fail too, but because the party is not known to be hosted on the synchronizer
      // rather than because of the confirmation threshold.
      eventually() {
        hostingParticipants.foreach {
          _.parties.list(filterParty = decentralizedParty.filterString).map(_.party) should
            contain(decentralizedParty)
        }
      }

      // An honest participant should refuse to submit the transaction in Phase 1
      val createCommand = simpleCreateCmd(List(decentralizedParty))
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.commands.submit(Seq(decentralizedParty), createCommand),
        _.errorMessage should (include(NoSynchronizerOnWhichAllSubmittersCanSubmit.id) and include(
          "This participant cannot submit as the given submitter on any connected synchronizer"
        )),
      )

      // A malicious participant's submission should be rejected in Phase 3
      val cmd = CommandsWithMetadata(
        createCommand,
        Seq(decentralizedParty),
        ledgerTime = environment.now.toLf,
      )

      def checkMessage(msg: String): Assertion =
        msg should include regex
          raw"""Received a request with id \S+ with a view that is not correctly authorized\. Rejecting request\.\.\.
                 |The submitting participant PAR::participant1\S+\. is not authorized to submit on behalf of the submitting parties decentralized-acs-party\S+\.""".stripMargin

      val (_, events) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        checkingConfirmationResponses(participants.all, sequencer1) {
          trackingLedgerEvents(participants.all, Seq.empty) {
            maliciousP1.submitCommand(cmd).futureValueUS
          }
        } { responses =>
          forEvery(responses) { case (_, response) =>
            inside(response.loneElement.localVerdict) { case LocalReject(reason, true) =>
              reason.message shouldBe alertMessage
            }
          }

          responses.keySet shouldBe Set(participant1.id, participant2.id)
        },
        LogEntry.assertLogSeq(
          assertPhase3Alert(
            loggerAssertion =>
              _.shouldBeCantonError(
                LocalRejectError.MalformedRejects.MalformedRequest,
                checkMessage,
                loggerAssertion = loggerAssertion,
              ),
            "authorization error",
          )(participant1, participant2)
        ),
      )

      events.assertNoCompletionsExceptFor(participant1)
      events.assertNoTransactions()

    }

    def testRequestSignature(
        badSignature: Option[Signature],
        errorLogAssertion: Seq[LogEntry] => Assertion,
    )(implicit
        env: TestConsoleEnvironment
    ): Unit = {
      import env.*

      val cmd = CommandsWithMetadata(
        simpleCreateCmd(List(party1)),
        Seq(party1),
        ledgerTime = environment.now.toLf,
      )

      val removeSubmitterSignature = GenLens[TransactionConfirmationRequest](_.viewEnvelopes)
        .andThen(Traversal.fromTraverse[Seq, OpenEnvelope[TransactionViewMessage]])
        .andThen(GenLens[OpenEnvelope[TransactionViewMessage]](_.protocolMessage))
        .modify(transactionViewMessage =>
          transactionViewMessage
            .asInstanceOf[EncryptedViewMessage[TransactionViewType]]
            .focus(_.submittingParticipantSignature)
            .replace(badSignature)
        )

      val (_, maliciousCaseEvents) =
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          checkingConfirmationResponses(participants.all, sequencer1)(
            trackingLedgerEvents(participants.all, Seq.empty) {
              maliciousP1
                .submitCommand(
                  cmd,
                  confirmationRequestInterceptor = removeSubmitterSignature,
                )
                .futureValueUS
            }
          ) { responses =>
            inside(
              responses(participant1).loneElement.localVerdict
            ) { case LocalReject(reason, true) =>
              reason.message shouldBe alertMessage
            }
          },
          errorLogAssertion,
        )
      maliciousCaseEvents.assertNoCompletionsExceptFor(participant1)
      maliciousCaseEvents.assertNoTransactions()
    }

    "the transaction view message for a root view has no signature" taggedAs_ (threat =>
      authenticityAttack(threat, mitigation)
    ) in { implicit env =>
      import env.*

      def assertAuthenticationErrorMessage(msg: String): Assertion =
        msg should include regex
          raw"""Received a request with id \S+ with a view that is not correctly authenticated\. Rejecting request\.\.\.
                 |View ViewPosition\(\S+\) is missing a signature\.""".stripMargin

      testRequestSignature(
        None,
        LogEntry.assertLogSeq(
          assertPhase3Alert(
            loggerAssertion =>
              entry => {
                assertAuthenticationErrorMessage(entry.message)
                entry.message should (startWith(
                  LocalRejectError.MalformedRejects.MalformedRequest.id
                ) or
                  startWith(LocalRejectError.MalformedRejects.Payloads.id))
                loggerAssertion(entry.loggerName)
              },
            "authentication error: missing signature",
          )(participant1)
        ),
      )
    }

    "the transaction view message for a root view has an invalid signature" taggedAs_ (threat =>
      authenticityAttack(threat, mitigation)
    ) in { implicit env =>
      import env.*

      def assertAuthenticationErrorMessage(msg: String): Assertion =
        msg should include regex
          raw"""Received a request with id \S+ with a view that is not correctly authenticated\. Rejecting request\.\.\.
                 |View ViewPosition\(\S+\) has an invalid signature: InvalidSignature(\S+)""".stripMargin

      val cryptoSnapshot = adHocSnapshot
      val hash = cryptoSnapshot.pureCrypto
        .build(HashPurpose.SubmissionRequestSignature)
        .addString("hamburger")
        .finish()
      val signature =
        cryptoSnapshot
          .sign(hash, SigningKeyUsage.ProtocolOnly, Some(environment.now))
          .failOnShutdown
          .futureValue

      testRequestSignature(
        Some(signature),
        LogEntry.assertLogSeq(
          assertPhase3Alert(
            loggerAssertion =>
              entry => {
                assertAuthenticationErrorMessage(entry.message)
                entry.message should (startWith(
                  LocalRejectError.MalformedRejects.MalformedRequest.id
                ) or
                  startWith(LocalRejectError.MalformedRejects.Payloads.id))
                loggerAssertion(entry.loggerName)
              },
            "authentication error: invalid signature",
          )(participant1)
        ),
      )
    }

    "a participant hosts a required authorizer of a non-root action" when_ { threat1 =>
      "it has not received the parent action" taggedAs_ (threat2 =>
        authorizationAttack(s"$threat1 and $threat2", mitigation)
      ) in { implicit env =>
        import env.*

        // Preparation: Create a contract
        val cid = submitP1CreateCmd(
          simpleCreateCmd(List(party1))
            .map(c => javaapi.data.Command.fromProtoCommand(toJavaProto(c)))
        )

        // Create a new version of the contract with party2 as signatory
        // without authorization from party2.
        val unhappyCaseCommand = CommandsWithMetadata(
          cid
            .exerciseReplace(
              List(party2.toProtoPrimitive).asJava,
              List.empty.asJava,
              List.empty.asJava,
              List(party2.toProtoPrimitive).asJava,
              List.empty.asJava,
            )
            .commands
            .asScala
            .toSeq
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          Seq(party1),
          ledgerTime = environment.now.toLf,
        )

        def assertAuthorizationError(msg: String): Assertion = msg should include regex
          raw"""Received a request with id \S+ with a view that is not correctly authorized\. Rejecting request\.\.\.
                 |Missing authorization for participant2\S+, ViewPosition\(Seq\("", ""\)\)\.""".stripMargin

        val (_, maliciousCaseEvents) =
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            checkingConfirmationResponses(participants.all, sequencer1)(
              trackingLedgerEvents(participants.all, Seq.empty) {
                maliciousP1.submitCommand(unhappyCaseCommand).futureValueUS
              }
            ) { responses =>
              inside(
                responses(participant2).loneElement.localVerdict
              ) { case LocalReject(reason, true) =>
                reason.message shouldBe alertMessage
              }
            },
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.shouldBeCantonError(
                    LocalRejectError.MalformedRejects.MalformedRequest,
                    assertAuthorizationError,
                    loggerAssertion =
                      _ should include regex "TransactionConfirmationResponsesFactory.*participant=participant2",
                  ),
                  "authorization error TransactionConfirmationResponsesFactory.*participant=participant2",
                ),
                ( // As a byproduct, participant1 detects that the transaction is not model conformant.
                  // This error could be avoided by disabling the model conformance check at the malicious participant1,
                  // but tolerating the error seems easier.
                  _.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.ModelConformance),
                  "model conformance error",
                ),
              ),
              Seq(
                // Optional, because the mediator could reject the request earlier due to the model conformance error.
                _.shouldBeCantonError(
                  LocalRejectError.MalformedRejects.MalformedRequest,
                  assertAuthorizationError,
                  loggerAssertion =
                    _ should include regex "(FinalizedResponse|ResponseAggregation).*mediatorx=mediator1",
                ),
                _.shouldBeCantonError(
                  LocalRejectError.MalformedRejects.MalformedRequest,
                  assertAuthorizationError,
                  loggerAssertion =
                    _ should include regex "TransactionProcessingSteps.*participant=participant1",
                ),
                _.shouldBeCantonError(
                  LocalRejectError.MalformedRejects.MalformedRequest,
                  assertAuthorizationError,
                  loggerAssertion =
                    _ should include regex "TransactionProcessingSteps.*participant=participant2",
                ),
              ),
            ),
          )

        maliciousCaseEvents.assertNoCompletionsExceptFor(participant1)
        maliciousCaseEvents.assertNoTransactions()
      }

      "the parent action paired with its required authorizers is not well-authorized" taggedAs_ (
        threat2 => authorizationAttack(s"$threat1 and $threat2", mitigation)
      ) in { implicit env =>
        import env.*

        // Preparation: Create a contract
        val cid = submitP1CreateCmd(
          mkUniversal(
            maintainers = List(party1),
            actors = List(party1),
            observers = List(party2),
          ).create.commands.asScala.toSeq
        )

        // Create a new version of the contract with party2 as signatory
        // without authorization from party2.
        val unhappyCaseCommand = CommandsWithMetadata(
          cid
            .exerciseReplace(
              List(party2.toProtoPrimitive).asJava,
              List.empty.asJava,
              List(party3.toProtoPrimitive).asJava,
              List(party2.toProtoPrimitive).asJava,
              List.empty.asJava,
            )
            .commands
            .asScala
            .toSeq
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          Seq(party1),
          ledgerTime = environment.now.toLf,
        )

        val (_, maliciousCaseEvents) =
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            checkingConfirmationResponses(participants.all, sequencer1)(
              trackingLedgerEvents(participants.all, Seq.empty) {
                maliciousP1.submitCommand(unhappyCaseCommand).futureValueUS
              }
            ) { responses =>
              val verdicts = responses(participant2).map(_.localVerdict)
              forEvery(verdicts)(
                _.asInstanceOf[LocalReject].isMalformed shouldBe true
              )
              verdicts.size shouldBe 2
            },
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.ModelConformance),
                  "model conformance error",
                )
              )
            ),
          )

        maliciousCaseEvents.assertNoCompletionsExceptFor(participant1)
        maliciousCaseEvents.assertNoTransactions()
      }

      "the parent action is not suitable for committing (i.e. incorrect threshold)" taggedAs_ (
        threat2 => authorizationAttack(s"$threat1 and $threat2", mitigation)
      ) in { implicit env =>
        import env.*

        // Preparation: Create a contract
        val cid = submitP1CreateCmd(
          simpleCreateCmd(List(party1))
            .map(c => javaapi.data.Command.fromProtoCommand(toJavaProto(c)))
        )

        // Create a new version of the contract with party2 as signatory
        // without authorization from party2.
        val unhappyCaseCommand = CommandsWithMetadata(
          cid
            .exerciseReplace(
              List(party1.toProtoPrimitive).asJava,
              List.empty.asJava,
              List(party2.toProtoPrimitive).asJava,
              List(party1.toProtoPrimitive).asJava,
              List.empty.asJava,
            )
            .commands
            .asScala
            .toSeq
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          Seq(party1),
          ledgerTime = environment.now.toLf,
        )

        val (_, maliciousCaseEvents) =
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            checkingConfirmationResponses(participants.all, sequencer1)(
              trackingLedgerEvents(participants.all, Seq.empty) {
                maliciousP1
                  .submitCommand(
                    unhappyCaseCommand,
                    transactionTreeInterceptor = firstViewCommonData
                      .andThen(GenLens[ViewCommonData](_.viewConfirmationParameters))
                      .modify { oldViewConfirmationParameters =>
                        ViewConfirmationParameters.tryCreate(
                          oldViewConfirmationParameters.informees,
                          oldViewConfirmationParameters.quorums
                            .map(_.copy(threshold = NonNegativeInt.tryCreate(2))),
                        )
                      },
                  )
                  .futureValueUS
              }
            ) { responses =>
              val verdicts = responses(participant1).map(_.localVerdict)
              forEvery(verdicts)(
                _.asInstanceOf[LocalReject].isMalformed shouldBe true
              )
              verdicts.size shouldBe 2
            },
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.ModelConformance),
                  "model conformance error",
                ),
                (
                  _.shouldBeCantonErrorCode(MediatorError.MalformedMessage),
                  "zero threshold",
                ),
              )
            ),
          )

        maliciousCaseEvents.assertNoCompletionsExceptFor(participant1)
        maliciousCaseEvents.assertNoTransactions()
      }
    }
  }

  private def submitSimpleP1Cmd(
      actAs: List[PartyId] = List(party1)
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val cmd = simpleP1Cmd(actAs)

    participant1.ledger_api.commands.submit(actAs, cmd)
  }

  private def simpleP1Cmd(maintainers: List[PartyId] = List(party1)): Seq[Command] =
    mkUniversal(
      maintainers = maintainers,
      actors = maintainers,
    ).createAnd
      .exerciseArchive()
      .commands
      .asScala
      .toSeq
      .map(c => Command.fromJavaProto(c.toProtoCommand))

  private def simpleCreateCmd(maintainers: List[PartyId]): Seq[Command] =
    mkUniversal(
      maintainers = maintainers,
      actors = maintainers,
    ).create.commands.asScala.toSeq
      .map(c => Command.fromJavaProto(c.toProtoCommand))

  private def submitP1CreateCmd(command: Seq[javaapi.data.Command])(implicit
      env: TestConsoleEnvironment
  ): UniversalContract.ContractId = {
    import env.*

    JavaDecodeUtil
      .decodeAllCreated(UniversalContract.COMPANION)(
        participant1.ledger_api.javaapi.commands.submit(Seq(party1), command)
      )
      .loneElement
      .id
  }

  private def mkUniversal(
      maintainers: List[PartyId],
      signatories: List[PartyId] = List.empty,
      observers: List[PartyId] = List.empty,
      actors: List[PartyId],
  ): UniversalContract = new UniversalContract(
    maintainers.map(_.toProtoPrimitive).asJava,
    signatories.map(_.toProtoPrimitive).asJava,
    observers.map(_.toProtoPrimitive).asJava,
    actors.map(_.toProtoPrimitive).asJava,
  )

  private def adHocSnapshot(implicit
      env: TestConsoleEnvironment
  ): SynchronizerSnapshotSyncCryptoApi = {
    import env.*
    participant1.underlying.value.sync.syncCrypto
      .tryForSynchronizer(daId, defaultStaticSynchronizerParameters)
      .currentSnapshotApproximation
      .futureValueUS
  }

  private def assertPhase3Alert(
      checkLoggerNameAndLogEntry: (String => Assertion) => LogEntry => Assertion,
      description: String,
  )(
      participants: ParticipantReference*
  ): Seq[(LogEntry => Assertion, String)] =
    participants
      .map(p => s"TransactionConfirmationResponsesFactory.*/participant=${p.name}")
      .map(l => checkLoggerNameAndLogEntry(_ should include regex l) -> s"$description at $l")
}

class LedgerAuthorizationReferenceIntegrationTestDefault
    extends LedgerAuthorizationIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

//class LedgerAuthorizationReferenceIntegrationTestPostgres extends LedgerAuthorizationIntegrationTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}
