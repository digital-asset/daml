// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.syntax.either.*
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.javaapi.data
import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.TagContainer
import com.daml.test.evidence.tag.EvidenceTag
import com.daml.test.evidence.tag.Security.SecurityTest.Property.{Integrity, Privacy}
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.LfValue
import com.digitalasset.canton.admin.api.client.data.DynamicSynchronizerParameters
import com.digitalasset.canton.crypto.{CryptoPureApi, SecureRandomness}
import com.digitalasset.canton.damltests.java.explicitdisclosure.PriceQuotation
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{
  GenTransactionTree,
  LightTransactionViewTree,
  TransactionView,
  ViewHashAndKey,
}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.Preprocessing.PreprocessingFailed
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.computeRandomnessLength
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  EncryptedView,
  EncryptedViewMessage,
  RootHashMessage,
  TransactionConfirmationRequest,
}
import com.digitalasset.canton.protocol.{
  ContractInstance,
  CreatedContract,
  LfFatContractInst,
  LocalRejectError,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.{MaliciousParticipantNode, MaxBytesToDecompress}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.TransactionCoder
import com.digitalasset.daml.lf.value.Value.{ValueRecord, ValueText}
import monocle.macros.syntax.lens.*
import org.scalatest.Tag
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

trait InvalidTransactionConfirmationRequestIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers
    with SecurityTestSuite
    with AccessTestScenario {

  private var maliciousP1: MaliciousParticipantNode = _

  private var party1: PartyId = _
  private var party2: PartyId = _
  private var party3: PartyId = _

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()

  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  private var clock: SimClock = _

  private var synchronizerParameters: DynamicSynchronizerParameters = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonTestsPath)

        maliciousP1 = MaliciousParticipantNode(
          participant1,
          daId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )

        party1 = participant1.adminParty
        party2 = participant2.adminParty
        party3 = participant3.adminParty

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)
        clock = environment.simClock.value
        synchronizerParameters =
          sequencer1.topology.synchronizer_parameters.get_dynamic_synchronizer_parameters(daId)
      }

  // Workaround to avoid false errors reported by IDEA.
  implicit def tagToContainer(tag: EvidenceTag): Tag = new TagContainer(tag)

  "A participant" when {

    "an attempt is made to construct text containing the zero character" must_ { threat =>
      "alarm, discard, and reject the view" taggedAs_ (mitigation =>
        SecurityTest(
          Integrity,
          "virtual shared ledger",
          Attack("a malicious participant", threat, mitigation),
        )
      ) in { implicit env =>
        import env.*

        val badStockName = "Below" + '\u0000'
        val goodStockName = "Above" + '0'

        def quoteWith(name: String): data.Command =
          new PriceQuotation(party1.toProtoPrimitive, name, 100L).create.commands
            .overridePackageId(PriceQuotation.PACKAGE_ID)
            .loneElement

        val v2Cmd = Command.fromJavaProto(quoteWith(goodStockName).toProtoCommand)

        val command = CommandsWithMetadata(
          commands = Seq(v2Cmd),
          actAs = Seq(party1),
          ledgerTime = environment.now.toLf,
        )

        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(party1), Seq(quoteWith(badStockName))),
          e => {
            e.shouldBeCantonErrorCode(PreprocessingFailed)
            e.errorMessage should include("Text contains null character")
          },
        )

        val transactionTreeInterceptor: GenTransactionTree => GenTransactionTree = tree => {
          tree.mapUnblindedRootViews { v =>
            val data = v.viewParticipantData.tryUnwrap
            val contract = data.createdCore.loneElement.contract

            val _arg: LfValue = inside(contract.inst.createArg) { case record: ValueRecord =>
              ValueRecord(
                None,
                ImmArray.from(
                  record.fields.head :: (
                    None,
                    ValueText(badStockName),
                  ) :: record.fields.toList.drop(2)
                ),
              )
            }
            val _create = contract.inst.toCreateNode.copy(arg = _arg)
            val _inst = LfFatContractInst.fromCreateNode(
              _create,
              contract.inst.createdAt,
              contract.inst.authenticationData,
            )
            val _serialization = new TransactionCoder(allowNullCharacters = true)
              .encodeFatContractInstance(_inst)
              .value
            val _createdContract = CreatedContract.tryCreate(
              contract = ContractInstance.createWithSerialization(
                _inst,
                contract.metadata,
                _serialization,
              ),
              consumedInCore = false,
              rolledBack = false,
            )
            val _data = data.copy(createdCore = Seq(_createdContract))
            TransactionView.tryCreate(v.hashOps)(
              v.viewCommonData,
              _data,
              v.subviews,
              testedProtocolVersion,
            )
          }
        }

        val (_, events) =
          loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            trackingLedgerEvents(participants.all, Seq.empty) {
              maliciousP1
                .submitCommand(
                  command = command,
                  transactionTreeInterceptor = transactionTreeInterceptor,
                )
                .futureValueUS
            },
            LogEntry.assertLogSeq(
              mustContainWithClue = Seq(
                (
                  le => {
                    le.loggerName should include("participant=participant1")
                    le.shouldBeCantonErrorCode(MalformedRejects.Payloads)
                    le.warningMessage should include("text contains null character")
                  },
                  "malformed entry",
                )
              ),
              mayContain = Seq(
                // Ignore error logged by the test
                _.loggerName should include(
                  "InvalidTransactionConfirmationRequestIntegrationTestPostgres"
                )
              ),
            ),
          )
        events.assertNoTransactions()
      }

    }

    "a view has missing recipients" must_ { threat =>
      "alarm, discard, and reject the view" taggedAs_ (mitigation =>
        SecurityTest(
          Integrity,
          "virtual shared ledger",
          Attack("a malicious participant", threat, mitigation),
        )
      ) in { implicit env =>
        import env.*

        val rawCmd = new UniversalContract(
          List(party1.toProtoPrimitive).asJava,
          List.empty.asJava,
          List(party2.toProtoPrimitive).asJava,
          List(party1.toProtoPrimitive).asJava,
        ).create.commands
          .overridePackageId(UniversalContract.PACKAGE_ID)
          .asScala
          .toSeq
          .map(c => Command.fromJavaProto(c.toProtoCommand))

        val cmd =
          CommandsWithMetadata(rawCmd, Seq(party1), ledgerTime = environment.now.toLf)

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          replacingConfirmationResult(
            daId,
            sequencer1,
            mediator1,
            withMediatorVerdict(mediatorApprove),
          ) {
            val (_, events2) = trackingLedgerEvents(Seq(participant2), Seq.empty) {
              val (_, events1) = trackingLedgerEvents(Seq(participant1), Seq.empty) {
                maliciousP1
                  .submitCommand(
                    cmd,
                    confirmationRequestInterceptor =
                      allViewRecipients.replace(Recipients.cc(participant1)),
                  )
                  .failOnShutdown
              }

              events1.assertNoTransactions()

              clock.advance(
                (synchronizerParameters.confirmationResponseTimeout + synchronizerParameters.mediatorReactionTimeout).asJava
                  .plusSeconds(1)
              )
            }

            events2.assertNoTransactions()
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonError(
                  SyncServiceAlarm,
                  _ should include("Received no encrypted view message of type TransactionViewType"),
                ),
                "p2 missing view",
              ),
              (
                _.shouldBeCantonError(
                  LocalRejectError.MalformedRejects.BadRootHashMessages,
                  _ should include("Received no encrypted view message of type TransactionViewType"),
                ),
                "p2 root hash message check",
              ),
              (
                _.shouldBeCantonError(SyncServiceAlarm, _ should include("has missing recipients")),
                "p1 recipients check",
              ),
            )
          ),
        )
      }
    }

    "a view has extra recipients" must_ { threat =>
      "alarm and process the view" taggedAs_ (mitigation =>
        SecurityTest(
          Privacy,
          "virtual shared ledger",
          Attack("a malicious participant", threat, mitigation),
        )
      ) in { implicit env =>
        import env.*

        val rawCmd = new UniversalContract(
          List(party1.toProtoPrimitive).asJava,
          List.empty.asJava,
          List.empty.asJava,
          List(party1.toProtoPrimitive).asJava,
        ).create.commands
          .overridePackageId(UniversalContract.PACKAGE_ID)
          .asScala
          .toSeq
          .map(c => Command.fromJavaProto(c.toProtoCommand))

        val cmd = CommandsWithMetadata(rawCmd, Seq(party1), ledgerTime = environment.now.toLf)

        val (_, events) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
          trackingLedgerEvents(participants.all, Seq.empty) {
            maliciousP1
              .submitCommand(
                cmd,
                confirmationRequestInterceptor = allViewRecipients.replace(
                  Recipients.cc(participant1, participant2)
                ),
              )
              .futureValueUS
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonError(
                  SyncServiceAlarm,
                  _ should include("No valid root hash message in batch"),
                ),
                "p2 extra message",
              ),
              (
                _.shouldBeCantonError(
                  SyncServiceAlarm,
                  _ should include("has extra recipients"),
                ),
                "p1 extra recipients",
              ),
            )
          ),
        )

        events.assertStatusOk(participant1)
        events.assertExactlyOneCompletion(participant1)
        events.awaitTransactions(participant1).loneElement
        events.awaitTransactions(participant2) shouldBe empty
      }
    }

    "a subview envelope is missing" must_ { threat =>
      "alarm, discard and reject all views" taggedAs_ (mitigation =>
        SecurityTest(
          Integrity,
          "virtual shared ledger",
          Attack("a malicious participant", threat, mitigation),
        )
      ) in { implicit env =>
        import env.*

        val createCmd = new UniversalContract(
          List(party1.toProtoPrimitive).asJava,
          List.empty.asJava,
          List.empty.asJava,
          List(party1.toProtoPrimitive).asJava,
        ).create.commands.overridePackageId(UniversalContract.PACKAGE_ID).asScala.toSeq
        val cid = JavaDecodeUtil
          .decodeAllCreated(UniversalContract.COMPANION)(
            participant1.ledger_api.javaapi.commands.submit(Seq(party1), createCmd)
          )
          .loneElement
          .id

        val rawCmd = cid
          .exerciseReplace(
            List(party1.toProtoPrimitive).asJava,
            List.empty.asJava,
            List(party2.toProtoPrimitive).asJava,
            List(party2.toProtoPrimitive).asJava,
            List.empty.asJava,
          )
          .commands
          .overridePackageId(UniversalContract.PACKAGE_ID)
          .asScala
          .toSeq
          .map(c => Command.fromJavaProto(c.toProtoCommand))

        val cmd = CommandsWithMetadata(rawCmd, Seq(party1), ledgerTime = environment.now.toLf)
        val (_, events) =
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            trackingLedgerEvents(participants.all, Seq.empty)(
              maliciousP1
                .submitCommand(
                  cmd,
                  confirmationRequestInterceptor =
                    _.focus(_.viewEnvelopes).modify(_.headOption.toList),
                )
                .futureValueUS
            ),
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.shouldBeCantonError(
                    SyncServiceAlarm,
                    _ should include(
                      "Received no encrypted view message of type TransactionViewType"
                    ),
                  ),
                  "p2 missing view",
                ),
                (
                  _.shouldBeCantonError(
                    LocalRejectError.MalformedRejects.BadRootHashMessages,
                    _ should include(
                      "Received no encrypted view message of type TransactionViewType"
                    ),
                  ),
                  "p2 root hash message check",
                ),
                (
                  _.shouldBeCantonError(
                    SyncServiceAlarm,
                    _ should include regex raw"View \S+ lists a subview with hash \S+, but I haven't received any views for this hash",
                  ),
                  "p1 decryption error",
                ),
              )
            ),
          )

        events.assertNoTransactions()
      }
    }

    "the same envelope is sent twice" must_ { threat =>
      "alarm and deduplicate the envelope" taggedAs_ (mitigation =>
        SecurityTest(
          Integrity,
          "virtual shared ledger",
          Attack("a malicious participant", threat, mitigation),
        )
      ) in { implicit env =>
        import env.*

        val rawCmd = new UniversalContract(
          List(party1.toProtoPrimitive).asJava,
          List.empty.asJava,
          List.empty.asJava,
          List(party1.toProtoPrimitive).asJava,
        ).create.commands
          .overridePackageId(UniversalContract.PACKAGE_ID)
          .asScala
          .toSeq
          .map(c => Command.fromJavaProto(c.toProtoCommand))

        val cmd = CommandsWithMetadata(rawCmd, Seq(party1), ledgerTime = environment.now.toLf)
        val (_, events) =
          loggerFactory.assertLoggedWarningsAndErrorsSeq(
            trackingLedgerEvents(participants.all, Seq.empty)(
              maliciousP1
                .submitCommand(
                  cmd,
                  confirmationRequestInterceptor =
                    _.focus(_.viewEnvelopes).modify(envs => envs ++ envs),
                )
                .futureValueUS
            ),
            LogEntry.assertLogSeq(
              Seq(
                (
                  _.warningMessage should include("DuplicateLightViewTree"),
                  "duplicate light view tree",
                )
              )
            ),
          )

        events.assertNoTransactions()
      }
    }

    "the root hash message recipients have an unusual structure" must_ { threat =>
      "alarm and process the request" taggedAs_ (mitigation =>
        SecurityTest(
          Integrity,
          "virtual shared ledger",
          Attack("a malicious participant", threat, mitigation),
        )
      ) in { implicit env =>
        import env.*

        val tx = participant1.ledger_api.javaapi.commands.submit(
          actAs = Seq(party1),
          commands = mkUniversal(
            maintainers = List(party1),
            observers = List(party2, party3),
          ).create.commands.overridePackageId(UniversalContract.PACKAGE_ID).asScala.toSeq,
        )

        val contractId =
          JavaDecodeUtil.decodeAllCreated(UniversalContract.COMPANION)(tx).loneElement.id

        val cmdWithMetadata =
          // This malicious command aims at archiving the contract where party3's participant receives an unusual root hash message
          CommandsWithMetadata(
            contractId
              .exerciseArchive()
              .commands
              .overridePackageId(UniversalContract.PACKAGE_ID)
              .asScala
              .toSeq
              .map(c => Command.fromJavaProto(c.toProtoCommand)),
            actAs = Seq(party1),
            ledgerTime = environment.now.toLf,
          )

        val attackedParticipant = participant3.id

        def modifyP3RootHashMessageRecipients(
            envelope: DefaultOpenEnvelope
        ): DefaultOpenEnvelope = {
          val attackedMember: Recipient = MemberRecipient(attackedParticipant)

          envelope.protocolMessage match {
            case _: RootHashMessage[?] =>
              envelope
                .focus(_.recipients.trees)
                .modify(_.map { tree =>
                  if (tree.recipientGroup.contains(attackedMember))
                    RecipientsTree(NonEmpty(Set, attackedMember), Seq(tree))
                  else tree
                })
            case _ => envelope
          }
        }

        val informeeParticipants = Seq(participant1, participant2, participant3)
        val (_, events) = loggerFactory.assertLogs(
          trackingLedgerEvents(informeeParticipants, Seq.empty) {
            maliciousP1
              .submitCommand(
                cmdWithMetadata,
                envelopeInterceptor = modifyP3RootHashMessageRecipients,
              )
              .futureValueUS
          },
          // The attacked participant raises an alarm
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ should startWith regex raw"\(sequencer counter: \S+, timestamp: \S+\): The root hash message has invalid recipient groups.\nRecipients",
            loggerAssertion = _ should include(s"participant=${attackedParticipant.uid.identifier}"),
          ),
        )

        // All participants have nonetheless archived the contract
        forEvery(informeeParticipants) { p =>
          events.allArchived(UniversalContract.COMPANION)(p).loneElement shouldBe contractId
        }
      }
    }

    // TODO(#15022): After transparency is implemented, the participant should not break.
    "a view has multiple different encryption keys" must_ { threat =>
      "alarm, discard, and reject the view" taggedAs_ (mitigation =>
        SecurityTest(
          Integrity,
          "virtual shared ledger",
          Attack("a malicious participant", threat, mitigation),
        )
      ) in { implicit env =>
        import env.*

        val createCmd = new UniversalContract(
          List(party1.toProtoPrimitive).asJava,
          List.empty.asJava,
          List.empty.asJava,
          List(party1.toProtoPrimitive).asJava,
        ).create.commands.overridePackageId(UniversalContract.PACKAGE_ID).asScala.toSeq
        val cid = JavaDecodeUtil
          .decodeAllCreated(UniversalContract.COMPANION)(
            participant1.ledger_api.javaapi.commands.submit(Seq(party1), createCmd)
          )
          .loneElement
          .id

        val rawCmd = cid
          .exerciseReplace(
            List(party1.toProtoPrimitive).asJava,
            List.empty.asJava,
            List(party2.toProtoPrimitive).asJava,
            List(party2.toProtoPrimitive).asJava,
            List.empty.asJava,
          )
          .commands
          .overridePackageId(UniversalContract.PACKAGE_ID)
          .asScala
          .toSeq
          .map(c => Command.fromJavaProto(c.toProtoCommand))

        val cmd = CommandsWithMetadata(rawCmd, Seq(party1), ledgerTime = environment.now.toLf)

        def replaceRandomnessForLightTransactionViewTree(
            tcr: TransactionConfirmationRequest
        ): TransactionConfirmationRequest = {

          val envelope = tcr.viewEnvelopes.headOption.valueOrFail("retrieve first view envelopes")
          val message = envelope.protocolMessage
          val recipients = envelope.recipients

          val encryptedViewTree = message.encryptedView
          val encryptedViewRandomness =
            message.viewEncryptionKeyRandomness.headOption.valueOrFail("retrieve view key")
          val viewRandomness = participant1.crypto.privateCrypto
            .decrypt(
              encryptedViewRandomness
            )(SecureRandomness.fromByteString(message.viewEncryptionScheme.keySizeInBytes))
            .valueOrFail("decrypt view key")
            .futureValueUS

          val viewKey = pureCrypto
            .createSymmetricKey(viewRandomness, message.viewEncryptionScheme)
            .valueOrFail("create view key")

          val viewTree = EncryptedView
            .decrypt(pureCrypto, viewKey, encryptedViewTree)(
              bytes => {
                LightTransactionViewTree
                  .fromByteString(
                    (pureCrypto, computeRandomnessLength(pureCrypto)),
                    testedProtocolVersion,
                  )(bytes)
                  .leftMap(err => DefaultDeserializationError(err.message))
              },
              MaxBytesToDecompress(synchronizerParameters.maxRequestSize),
            )
            .value

          // change the randomness assigned to the first subview in the view tree
          val subviewHash =
            viewTree.subviewHashesAndKeys.headOption.valueOrFail("retrieve subview").viewHash

          val newLtvt = LightTransactionViewTree.tryCreate(
            viewTree.tree,
            viewTree.subviewHashesAndKeys.updated(
              0,
              ViewHashAndKey(
                subviewHash,
                pureCrypto.generateSecureRandomness(
                  EncryptedViewMessage.computeRandomnessLength(pureCrypto)
                ),
              ),
            ),
            testedProtocolVersion,
          )

          val crypto = participant1.underlying.value.sync.syncCrypto
            .tryForSynchronizer(daId, defaultStaticSynchronizerParameters)
            .currentSnapshotApproximation
            .futureValueUS

          val newEncryptedViewMessage = EncryptedViewMessageFactory
            .create(TransactionViewType)(
              newLtvt,
              (viewKey, message.viewEncryptionKeyRandomness),
              crypto,
              Some(environment.now),
              testedProtocolVersion,
            )
            .valueOrFail("create new envelope")
            .futureValueUS
          val newEnvelope = OpenEnvelope(newEncryptedViewMessage, recipients)(testedProtocolVersion)

          tcr.focus(_.viewEnvelopes).modify(_.updated(0, newEnvelope))
        }

        loggerFactory.suppress(SuppressionRule.Level(Level.ERROR)) {
          maliciousP1
            .submitCommand(
              cmd,
              confirmationRequestInterceptor = replaceRandomnessForLightTransactionViewTree,
            )
          eventually() {
            val logEntries = loggerFactory.fetchRecordedLogEntries
            logEntries.size shouldBe >=(3)
            val logMessages = logEntries.map(_.message)
            logMessages.exists(_.contains("Failed to process request")) shouldBe true
            logMessages.exists(
              _.contains(
                "Asynchronous event processing failed for event batch with sequencing timestamps"
              )
            ) shouldBe true
            logMessages should contain("An internal error has occurred.")
            assert(
              logEntries
                .map(_.throwable.value.getMessage)
                .exists(_.contains("has different encryption keys associated with it."))
            )
          }
        }

      }
    }

  }

  def mkUniversal(
      maintainers: List[PartyId] = List.empty,
      signatories: List[PartyId] = List.empty,
      observers: List[PartyId] = List.empty,
      actors: List[PartyId] = List.empty,
  ): UniversalContract = new UniversalContract(
    maintainers.map(_.toProtoPrimitive).asJava,
    signatories.map(_.toProtoPrimitive).asJava,
    observers.map(_.toProtoPrimitive).asJava,
    actors.map(_.toProtoPrimitive).asJava,
  )
}

//class InvalidTransactionConfirmationRequestIntegrationTestDefault
//    extends InvalidTransactionConfirmationRequestIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(new UseBftSequencer(loggerFactory))
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}

class InvalidTransactionConfirmationRequestIntegrationTestPostgres
    extends InvalidTransactionConfirmationRequestIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
