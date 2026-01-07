// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.syntax.bifunctor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.admin.api.client.data.topology.{
  ListNamespaceDelegationResult,
  ListOwnerToKeyMappingResult,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalParticipantReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.crypto.v30.SignatureFormat.SIGNATURE_FORMAT_RAW
import com.digitalasset.canton.crypto.v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_UNSPECIFIED
import com.digitalasset.canton.crypto.{Fingerprint, Signature, SigningKeyUsage, v30}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.MediatorError.MalformedMessage
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.sequencing.handlers.EnvelopeOpenerError.EnvelopeOpenerDeserializationError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.sequencer.time.TimeAdvancingTopologySubscriber.TimeAdvanceBroadcastMessageIdPrefix
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.topology.TopologyManager.assignExpectedUsageToKeys
import com.digitalasset.canton.topology.TopologyManagerError.TopologyManagerAlarm
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllMappings,
  CanSignSpecificMappings,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Remove, Replace}
import com.digitalasset.canton.topology.{Member, PartyId, QueueBasedSynchronizerOutbox}
import com.digitalasset.canton.util.{ErrorUtil, MaliciousParticipantNode, SingleUseCell}
import com.google.protobuf.ByteString
import org.slf4j.event.Level
import org.slf4j.event.Level.WARN

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.collection.mutable

/** Integration test demonstrating known vulnerabilities of topology management. When fixing a
  * vulnerability, please convert the affected tests into regression tests. When a test breaks by
  * "accident", feel free to set it to "ignore" and inform Gerolf + Matthias.
  */
class InvalidTopologyBroadcastIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with HasProgrammableSequencer {

  var maliciousP1: MaliciousParticipantNode = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, daName)

      maliciousP1 = MaliciousParticipantNode(
        participant1,
        daId,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )
    }

  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  "An attacker" can {
    // positive test to check that the setup works correctly
    "register an authorized key by using the sequencer api" in { implicit env =>
      import env.*

      letParticipant1Impersonate("happy", participant1.id, participant1.namespace.fingerprint)

      participant1.health.ping(participant1)
    }

    "not abuse the registration of several namespace delegations with the same key" in {
      implicit env =>
        import env.*

        val namespace = participant1.id.namespace

        val newKey1 = participant1.keys.secret
          .generate_signing_key("test-ns1", SigningKeyUsage.NamespaceOnly)
        val newKey2 = participant1.keys.secret
          .generate_signing_key("test-ns2", SigningKeyUsage.NamespaceOnly)

        // Register newKey1 as ns root delegation
        registerTopologyMapping(
          NamespaceDelegation.create(namespace, newKey1, CanSignAllMappings).value,
          signedBy = namespace.fingerprint,
          serial = PositiveInt.one,
        )

        // Register newKey1 again, this time without being able to sign namespace delegations.
        // This should replace the previous namespace delegation.
        registerTopologyMapping(
          NamespaceDelegation
            .create(
              namespace,
              newKey1,
              CanSignSpecificMappings(PartyToParticipant),
            )
            .value,
          signedBy = namespace.fingerprint,
          serial = PositiveInt.two,
        )

        // Register another namespace delegation, signed with newKey1
        // This is only allowed if newKey1 was given the rights to sign namespace delegations.
        // This is not the case, therefore it should be rejected.
        waitForMessageToBeProcessed(Left(participant1)) {
          registerTopologyMapping(
            NamespaceDelegation.create(namespace, newKey2, CanSignAllMappings).value,
            signedBy = newKey1.fingerprint,
            serial = PositiveInt.one,
          )
        }

        utils.synchronize_topology()

        participant1.topology.namespace_delegations
          .list(
            daId,
            filterNamespace = namespace.filterString,
            filterTargetKey = Some(newKey1.fingerprint),
          )
          .loneElement
          .context
          .serial shouldBe PositiveInt.two

        participant1.topology.namespace_delegations
          .list(
            daId,
            filterNamespace = namespace.filterString,
            filterTargetKey = Some(newKey2.fingerprint),
          ) shouldBe empty
    }

    // This is now a verification of a fix
    "not register an unauthenticated namespace delegation" in { implicit env =>
      import env.*

      val namespace = sequencer1.id.namespace

      val newKey = participant1.keys.secret
        .generate_signing_key("test-ns-sequencer", SigningKeyUsage.NamespaceOnly)

      val namespaceDelegation =
        NamespaceDelegation.create(namespace, newKey, CanSignAllMappings).value

      // This will be rejected due to an invalid signature.
      registerTopologyMappingWithSignatures(
        namespaceDelegation,
        // Use empty signature with signedBy = namespace to get it into the authorization graph
        _ => NonEmpty(Seq, emptySignature(namespace.fingerprint)),
        PositiveInt.one,
        Replace,
      )

      // This will be rejected, as there is no authorization chain to the root certificate for newKey
      // TODO(#19737): a security alert should be emitted and observed here as well
      registerTopologyMapping(
        namespaceDelegation,
        signedBy = newKey.fingerprint,
        serial = PositiveInt.one,
      )

      utils.synchronize_topology()

      // Check that the namespace delegation for newKey is not available
      participant1.topology.namespace_delegations
        .list(
          daId,
          filterNamespace = namespace.filterString,
          filterTargetKey = Some(newKey.fingerprint),
        ) shouldBe empty
    }

    "not revoke namespace root certificate without authorization" in { implicit env =>
      import env.*

      val namespace = sequencer1.id.namespace

      def getRootCertificates: Seq[ListNamespaceDelegationResult] =
        participant1.topology.namespace_delegations
          .list(
            daId,
            filterNamespace = namespace.filterString,
            filterTargetKey = Some(namespace.fingerprint),
          )

      val rootCertificateResult = getRootCertificates.loneElement

      val oldSerial = rootCertificateResult.context.serial

      val signature = emptySignature(namespace.fingerprint)
      registerTopologyMappingWithSignatures(
        rootCertificateResult.item,
        // Add empty signature with signedBy = namespace.
        // This is sufficient to pass the signature verification.
        _ => NonEmpty(Seq, signature),
        oldSerial.increment,
        Remove,
      )

      utils.synchronize_topology()

      getRootCertificates shouldBe Seq(rootCertificateResult)
    }

    // not a vulnerability anymore
    "not take over namespace with decentralized namespace delegation" in { implicit env =>
      import env.*

      val decentralizedNamespaceDefinition = DecentralizedNamespaceDefinition
        .create(
          mediator1.namespace,
          PositiveInt.one,
          NonEmpty(Set, participant1.namespace),
        )
        .value

      registerTopologyMapping(
        decentralizedNamespaceDefinition,
        participant1.namespace.fingerprint,
        PositiveInt.one,
      )
      utils.synchronize_topology()

      // Check that the decentralized namespace is active
      participant1.topology.decentralized_namespaces
        .list(
          store = daId,
          filterNamespace = mediator1.namespace.filterString,
        ) shouldBe empty
    }

    "not specify a topology timestamp for topology transaction broadcasts" in { implicit env =>
      import env.*

      val topologyTimestamp = CantonTimestamp.now()
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(WARN))(
        {
          waitForMessageToBeProcessed(Left(participant1)) {
            registerTopologyMapping(
              SynchronizerTrustCertificate(
                participant1.id,
                daId,
              ),
              participant1.namespace.fingerprint,
              PositiveInt.two,
              op = Replace,
              topologyTimestamp = Some(topologyTimestamp),
            )
          }

          utils.synchronize_topology()

          // the SynchronizerTrustCertificate(serial=2) should not be processed at all
          Seq[InstanceReference](participant1, mediator1, sequencer1).foreach(
            _.topology.synchronizer_trust_certificates
              .list(store = daId, filterUid = participant1.id.filterString)
              .loneElement
              .context
              .serial shouldBe PositiveInt.one
          )
        },
        { entries =>
          // keep track of which nodes emit topology manager alarms
          val checkedNodes = mutable.Set[String]()
          LogEntry.assertLogSeq(
            Seq(
              (
                entry => {
                  entry.shouldBeCantonError(
                    TopologyManagerAlarm.code,
                    _ should include regex raw"Discarding a topology broadcast with sc=\d+ at \S+ with explicit topology timestamp $topologyTimestamp",
                  )
                  val nodeName = entry.mdc.getOrElse(
                    "participant",
                    entry.mdc.getOrElse("sequencer", entry.mdc.getOrElse("mediator", "")),
                  )
                  checkedNodes += nodeName
                  succeed
                },
                "topology alarm",
              )
            )
          )(entries)
          // we expect topology manager alarms from these nodes
          checkedNodes shouldBe Set(participant1.name, sequencer1.name, mediator1.name)
        },
      )
    }

    s"not cause a ledger fork by adding other recipients in addition to the broadcast address $AllMembersOfSynchronizer" in {
      implicit env =>
        import env.*

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          waitForMessageToBeProcessed(Left(participant1)) {
            registerTopologyMapping(
              SynchronizerTrustCertificate(
                participant1.id,
                daId,
              ),
              participant1.namespace.fingerprint,
              PositiveInt.two,
              op = Replace,
              // participant1 will see recipients: participant1, AllMembersOfSynchronizer
              // all other members see recipients: AllMembersOfSynchronizer
              // This is to test that as long as a node sees the recipient AllMembersOfSynchronizer,
              // it can be sure that actually all other active members (active at this topology transaction's
              // sequenced time) will receive it as well.
              recipient = Recipients.recipientGroups(
                NonEmpty(
                  Seq,
                  NonEmpty(
                    Set,
                    MemberRecipient(participant1.id.member): Recipient,
                  ),
                  NonEmpty(
                    Set,
                    AllMembersOfSynchronizer: Recipient,
                  ),
                )
              ),
            )
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include(
                  s"Unexpected message from ${participant1.id} to participants,broadcast"
                ),
                "sequencer warning about unexpected recipient pattern",
              )
            )
          ),
        )
    }

    "not send topology transactions only to a subset of members" in { implicit env =>
      import env.*

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          Seq[Either[LocalParticipantReference, LocalSequencerReference]](
            Left(participant1),
            Right(sequencer1),
          ).foreach { nodeToWaitFor =>
            waitForMessageToBeProcessed(nodeToWaitFor) {
              registerTopologyMapping(
                SynchronizerTrustCertificate(
                  participant1.id,
                  daId,
                ),
                participant1.namespace.fingerprint,
                PositiveInt.two,
                op = Replace,
                recipient = Recipients.cc(
                  (nodeToWaitFor: Either[InstanceReference, InstanceReference]).merge.id.member
                ),
              )
            }
          }

          utils.synchronize_topology()

          // the SynchronizerTrustCertificate(serial=2) should not be processed at all
          Seq[InstanceReference](participant1, sequencer1).foreach { node =>
            node.topology.synchronizer_trust_certificates
              .list(store = daId, filterUid = participant1.id.filterString)
              .loneElement
              .context
              .serial shouldBe PositiveInt.one
          }
        },
        entries => {
          // keep track of which nodes emit topology manager alarms
          val checkedNodes = mutable.Set[String]()
          LogEntry.assertLogSeq(
            Seq(
              (
                entry => {
                  entry.shouldBeCantonError(
                    TopologyManagerAlarm.code,
                    _.should(
                      include regex raw"Discarding a topology broadcast with sc=\d+ at \S+ with invalid recipients: .*" and
                        (include(MemberRecipient(participant1.id).toString) or include(
                          MemberRecipient(sequencer1.id).toString
                        ))
                    ),
                  )
                  val nodeName =
                    entry.mdc.getOrElse("participant", entry.mdc.getOrElse("sequencer", ""))
                  checkedNodes += nodeName
                  succeed
                },
                "invalid recipient",
              ),
              (
                entry => {
                  entry.warningMessage should include(
                    s"Unexpected message from ${participant1.id} to sequencers"
                  )
                },
                "sequencer warning about unexpected recipient pattern",
              ),
            )
          )(entries)
          // we expect topology manager alarms from these nodes
          checkedNodes shouldBe Set(participant1.name, sequencer1.name)
        },
      )
    }
    "not send OTK or PTK with more than the allowed keys" in { implicit env =>
      import env.*

      def submitAndAssertWarning(mapping: TopologyMapping): Unit =
        clue(s"submitting toxic ${mapping.code}") {
          loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            registerTopologyMapping(
              mapping,
              signedBy = participant1.namespace.fingerprint,
              serial = PositiveInt.two,
              op = Replace,
              recipient = Recipients.cc(AllMembersOfSynchronizer),
            ),
            LogEntry.assertLogSeq(
              Seq(
                (
                  entry => {
                    entry.shouldBeCantonErrorCode(SyncServiceAlarm)
                    entry.warningMessage should include(
                      s"At most ${OwnerToKeyMapping.MaxKeys} can be specified"
                    )
                  },
                  "warning on participant",
                ),
                (
                  entry => {
                    entry.shouldBeCantonErrorCode(MalformedMessage)
                    entry.warningMessage should include(
                      s"At most ${OwnerToKeyMapping.MaxKeys} can be specified"
                    )
                  },
                  "warning on mediator",
                ),
                (
                  entry => {
                    entry.shouldBeCantonErrorCode(EnvelopeOpenerDeserializationError)
                    entry.warningMessage should include(
                      s"At most ${OwnerToKeyMapping.MaxKeys} can be specified"
                    )
                  },
                  "warning on mediator",
                ),
              )
            ),
          )
        }

      val tooManyKeys = Set.fill(OwnerToKeyMapping.MaxKeys + 1)(
        participant1.keys.secret.generate_signing_key(usage = SigningKeyUsage.ProtocolOnly)
      )

      // set up the toxic OTK and PTK mappings
      val otkWithTooManyKeys = {
        val otk = participant1.topology.owner_to_key_mappings
          .list(daId, filterKeyOwnerUid = participant1.filterString)
          .loneElement
          .item
        // smuggle more than the allowed number of keys into the OTK by bypassing the validation via the unsafe lens.
        // an attacker would just construct the proto directly, but we're not working with proto here.
        OwnerToKeyMapping.keysUnsafe.modify(_ ++ tooManyKeys)(otk)
      }

      submitAndAssertWarning(otkWithTooManyKeys)

      @nowarn("cat=deprecation")
      val ptkWithTooManyKeys = {
        // create the base PTK first, because submitAndAssertWarning hardcodes serial=2
        val ptk = participant1.topology.party_to_key_mappings
          .propose(
            partyId = PartyId.tryCreate("ptk", participant1.namespace),
            threshold = PositiveInt.one,
            signingKeys = NonEmpty.from(tooManyKeys.take(1)).value.toSeq,
          )
          .mapping
        // smuggle more than the allowed number of keys into the PTK by bypassing the validation via the unsafe lens.
        // an attacker would just construct the proto directly, but we're not working with proto here.
        ptk.copySigningKeysUnsafe(
          ptk.signingKeysWithThreshold.copyKeysUnsafe(NonEmpty.from(tooManyKeys).value)
        )
      }

      submitAndAssertWarning(ptkWithTooManyKeys)
    }
  }

  "A node" should {
    "detect a timeout when broadcasting topology transactions" in { implicit env =>
      import env.*

      // Arbitrary number of time advancements we want to drop.
      // This thereby tests that the sequencer retries sending its time advancement broadcast.
      val timeAdvancementsToDrop = 3

      val s1 = getProgrammableSequencer(sequencer1.name)
      val droppedBatch = new SingleUseCell[Batch[ClosedEnvelope]]
      val droppedTimeAdvancement = new AtomicReference[Option[(CantonTimestamp, Int)]](None)
      s1.setPolicy("drop topology broadcasts")(
        SendPolicy.processTimeProofs(implicit traceContext => {
          case r if r.messageId.unwrap.startsWith(TimeAdvanceBroadcastMessageIdPrefix) =>
            val Some((_, retryCount)) = droppedTimeAdvancement.updateAndGet {
              case None => Some(r.maxSequencingTime -> 0)
              case Some((previousTime, count)) =>
                ErrorUtil.requireState(
                  previousTime == r.maxSequencingTime.immediatePredecessor,
                  "dropped time advancement is not the same as retried time advancement",
                )
                Some(r.maxSequencingTime -> (count + 1))
            }: @unchecked
            if (retryCount > timeAdvancementsToDrop) SendDecision.Process
            else SendDecision.Drop
          case r if r.batch.allRecipients.contains(AllMembersOfSynchronizer) =>
            droppedBatch.putIfAbsent(r.batch) match {
              case None =>
                // this is the first topology broadcast, so we drop it to simulate the message getting lost
                SendDecision.Drop
              case Some(droppedBatch) =>
                assert(r.batch == droppedBatch, "dropped batch is not the same as retried batch")
                SendDecision.Process
            }
          case _ => SendDecision.Process
        })
      )

      val beforeUpdate =
        sequencer1.topology.synchronizer_parameters.get_dynamic_synchronizer_parameters(daId)
      loggerFactory.assertLogs(
        SuppressionRule.forLogger[QueueBasedSynchronizerOutbox] && SuppressionRule.Level(
          Level.WARN
        )
      )(
        within = sequencer1.topology.synchronizer_parameters.propose_update(
          daId,
          params => params.update(maxRequestSize = params.maxRequestSize.increment.toNonNegative),
        ),
        _.warningMessage should include regex s"The synchronizer .* failed the following topology transactions",
      )
      sequencer1.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(daId)
        .maxRequestSize shouldBe beforeUpdate.maxRequestSize.increment.toNonNegative
    }
  }

  private def preheadSequencerCounterOnSequencer(sequencer: LocalSequencerReference) =
    sequencer.underlying.value.sequencer.topologyManagerSequencerCounterTrackerStore.preheadSequencerCounter.futureValueUS.value.counter

  private def cleanSequencerCounterOnParticipant(
      participant: LocalParticipantReference
  )(implicit env: TestConsoleEnvironment): SequencerCounter =
    participant.testing.state_inspection
      .lookupCleanSequencerCounter(env.daId)
      .value
      .futureValueUS
      .value

  private def waitForMessageToBeProcessed(
      node: Either[LocalParticipantReference, LocalSequencerReference]
  )(action: => Unit)(implicit env: TestConsoleEnvironment): Unit = {
    def currentCounter(): SequencerCounter =
      node.bimap(cleanSequencerCounterOnParticipant(_), preheadSequencerCounterOnSequencer).merge

    val before = currentCounter()
    action
    eventually() {
      currentCounter() should be > before
    }
  }

  def emptySignature(signedBy: Fingerprint): Signature =
    Signature
      .fromProtoV30(
        v30.Signature(
          SIGNATURE_FORMAT_RAW,
          ByteString.EMPTY,
          signedBy.toProtoPrimitive,
          SIGNING_ALGORITHM_SPEC_UNSPECIFIED,
          signatureDelegation = None,
        )
      )
      .value

  /** Lets participant1 impersonate some member by registering new keys for the member. Verifies
    * that the new keys show up in the synchronizer topology state.
    *
    * @param purpose
    *   used in the name of the new keys
    * @param member
    *   the member to impersonate
    * @param signedBy
    *   fingerprint the key to be used for signing the topology transaction
    */
  def letParticipant1Impersonate(purpose: String, member: Member, signedBy: Fingerprint)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val newSigningKey = participant1.keys.secret
      .generate_signing_key(s"test-$purpose-sign", SigningKeyUsage.ProtocolOnly)
    val newEncryptionKey = participant1.keys.secret.generate_encryption_key(s"test-$purpose-enc")

    def getOwnerToKeyMapping: ListOwnerToKeyMappingResult =
      participant1.topology.owner_to_key_mappings
        .list(
          daId,
          filterKeyOwnerUid = member.filterString,
        )
        .loneElement

    val oldOwnerToKeyMappingResult = getOwnerToKeyMapping
    val newOwnerToKeyMapping =
      OwnerToKeyMapping.tryCreate(
        member,
        oldOwnerToKeyMappingResult.item.keys :+ newSigningKey :+ newEncryptionKey,
      )

    val keysToSign = NonEmpty.mk(Set, signedBy, newSigningKey.fingerprint)
    val keyIdsWithUsage = assignExpectedUsageToKeys(
      newOwnerToKeyMapping,
      keysToSign,
      forSigning = true,
    )
    val mkSignatures = (tx: TopologyTransaction[?, ?]) =>
      keysToSign.map { keyId =>
        participant1.crypto.privateCrypto
          .sign(tx.hash.hash, keyId, keyIdsWithUsage(keyId))
          .futureValueUS
          .value
      }

    registerTopologyMappingWithSignatures(
      newOwnerToKeyMapping,
      mkSignatures.andThen(_.toSeq),
      oldOwnerToKeyMappingResult.context.serial.increment,
      Replace,
    )

    eventually()(getOwnerToKeyMapping.item shouldBe newOwnerToKeyMapping)
  }

  def registerTopologyMapping(
      mapping: TopologyMapping,
      signedBy: Fingerprint,
      serial: PositiveInt,
      op: TopologyChangeOp = TopologyChangeOp.Replace,
      topologyTimestamp: Option[CantonTimestamp] = None,
      recipient: Recipients = Recipients.cc(AllMembersOfSynchronizer),
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    registerTopologyMappingWithSignatures(
      mapping,
      tx =>
        NonEmpty(
          Seq,
          participant1.crypto.privateCrypto
            .sign(
              tx.hash.hash,
              signedBy,
              SigningKeyUsage.NamespaceOnly,
            )
            .futureValueUS
            .value,
        ),
      serial,
      op,
      topologyTimestamp,
      recipient,
    )
  }

  def registerTopologyMappingWithSignatures(
      mapping: TopologyMapping,
      mkSignatures: TopologyTransaction[?, ?] => NonEmpty[Seq[Signature]],
      serial: PositiveInt,
      op: TopologyChangeOp,
      topologyTimestamp: Option[CantonTimestamp] = None,
      recipients: Recipients = Recipients.cc(AllMembersOfSynchronizer),
  ): Unit = {
    val tx =
      TopologyTransaction(
        op,
        serial,
        mapping,
        testedProtocolVersion,
      )
    val signedTx = SignedTopologyTransaction.withSignatures(
      tx,
      mkSignatures(tx),
      isProposal = false,
      testedProtocolVersion,
    )

    maliciousP1
      .submitTopologyTransactionRequest(
        signedTx,
        topologyTimestamp = topologyTimestamp,
        recipients = recipients,
      )
      .futureValueUS
      .value
  }
}
