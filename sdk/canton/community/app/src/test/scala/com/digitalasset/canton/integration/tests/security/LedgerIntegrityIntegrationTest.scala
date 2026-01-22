// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.ledger.api.v2.commands.Command
import com.daml.test.evidence.scalatest.ScalaTestSupport.TagContainer
import com.daml.test.evidence.tag.EvidenceTag
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.data.{
  GenTransactionTree,
  MerkleTree,
  TransactionView,
  ViewParticipantData,
}
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.logging.{LogEntry, SuppressingLogger}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.{
  MalformedRequest,
  ModelConformance,
}
import com.digitalasset.canton.protocol.{ExampleContractFactory, InputContract}
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.{HostingParticipant, ParticipantPermission}
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.daml.lf.transaction.CreationTime
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ValueList, ValueParty}
import monocle.macros.GenLens
import org.scalatest.{Assertion, Tag}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

abstract sealed class LedgerIntegrityIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestLensUtils
    with SecurityTestSuite
    with EntitySyntax {

  override val loggerFactory: SuppressingLogger =
    SuppressingLogger(getClass, pollTimeout = 10.seconds)

  // Workaround to avoid false errors reported by IDEA.
  implicit def tagToContainer(tag: EvidenceTag): Tag = new TagContainer(tag)

  private lazy val integrity: SecurityTest =
    SecurityTest(SecurityTest.Property.Integrity, "virtual shared ledger")

  private def integrityAttack(threat: String, mitigation: String)(implicit
      lineNo: sourcecode.Line,
      fileName: sourcecode.File,
  ): SecurityTest =
    integrity.setAttack(Attack("a malicious participant", threat, mitigation))

  // Using AtomicRef, because this gets read from various threads.
  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()

  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  var maliciousP1: MaliciousParticipantNode = _

  // hosted on participant1
  private var party1: PartyId = _
  // hosted on participant 1 and 2 with observation permissions on participant 2
  private var party12: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory),
        ConfigTransforms.useStaticTime,
      )
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .withSetup { implicit env =>
        import env.*

        participants.all.foreach { p =>
          p.synchronizers.connect_local(sequencer1, alias = daName)
          p.dars.upload(CantonTestsPath)
        }

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)

        maliciousP1 = MaliciousParticipantNode(
          participant1,
          daId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )

        PartiesAllocator(Set(participant1, participant2))(
          newParties = Seq("party1" -> participant1, "party12" -> participant2),
          targetTopology = Map(
            "party1" -> Map(
              daId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission))
            ),
            "party12" -> Map(
              daId -> (PositiveInt.one, Set(
                participant1.id -> ParticipantPermission.Submission,
                participant2.id -> ParticipantPermission.Observation,
              ))
            ),
          ),
        )
        party1 = "party1".toPartyId(participant1)
        party12 = "party12".toPartyId(participant2)

        // TODO(#23744): This check should not be necessary but was added to prevent a rare flake that occurs when
        // synchronization fails during the previous _.topology.party_to_participant_mappings.propose_delta.
        // In such cases, the topology is not yet visible at the time of submission, leading to topology changes
        // while the submission is in progress. This can be removed once the underlying issue is fixed.
        eventually() {
          participant1.topology.party_to_participant_mappings
            .list(daId, filterParty = party12.filterString)
            .loneElement
            .item
            .participants should (
            contain(HostingParticipant(participant1.id, ParticipantPermission.Submission)) and
              contain(HostingParticipant(participant2.id, ParticipantPermission.Observation))
          )
        }
      }

  "A participant rejects a request with an alarm" when {
    "input contracts authentication fails" taggedAs integrityAttack(
      threat = "submits a request with incorrect contract data",
      mitigation = "rollback the command",
    ) in { implicit env =>
      import env.*

      val tx =
        participant1.ledger_api.javaapi.commands
          .submit(
            actAs = Seq(party1, party12),
            commands = mkUniversal(
              maintainers = List(party1, party12)
            ).create.commands.overridePackageId(UniversalContract.PACKAGE_ID).asScala.toSeq,
            Some(daId),
          )

      val contractId =
        JavaDecodeUtil.decodeAllCreated(UniversalContract.COMPANION)(tx).loneElement.id
      val contractIdStr = contractId.contractId

      val cmdWithMetadata =
        // This malicious command aims at archiving the contract without the other (party12) signatory's approval
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

      val removeParty12FromSignatoriesInContractArg: Value => Value = {
        case Value.ValueRecord(tycon, fields) =>
          val firstField =
            fields.toList.headOption.valueOrFail(
              "Expected the signatories list as the first field in the argument list"
            )

          firstField._2 match {
            case ValueList(signatories) =>
              val modifiedSignatoriesList = ValueList(
                signatories.toImmArray.filter {
                  case ValueParty(party) => party != party12.toLf
                  case _ => true
                }.toFrontStack
              )

              Value.ValueRecord(
                tycon,
                fields.tail.slowCons(firstField._1 -> modifiedSignatoriesList),
              )

            case other =>
              fail(
                s"Expected a ${classOf[ValueList].getSimpleName} of signatories but got ${other.getClass.getSimpleName}"
              )
          }
        case other => other
      }

      // Craft a different input contract with the party12 signatory removed
      val remove_party12_fromSignatories: GenTransactionTree => GenTransactionTree =
        GenTransactionTree.rootViewsUnsafe
          .andThen(firstElement[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.tryUnwrap[ViewParticipantData])
          .andThen(GenLens[ViewParticipantData](_.coreInputs))
          .modify(_.map {
            case (id, contract) if contractIdStr == id.coid =>
              val contractWithMissingSignatory =
                GenLens[InputContract](_.contract)
                  .modify(c =>
                    ExampleContractFactory.modify[CreationTime](
                      c,
                      arg = Some(removeParty12FromSignatoriesInContractArg(c.inst.createArg)),
                    )
                  )
                  .apply(contract)
              id -> contractWithMissingSignatory
            case (id, contract) => id -> contract
          })

      val expectedLogEntries = {

        def checkForModelConformanceError(loggerName: String, memberName: String)(
            entry: LogEntry
        ): Assertion = {
          entry.shouldBeCantonErrorCode(ModelConformance)
          entry.message should include regex raw"(?s)Rejected transaction due to a failed model conformance.*ValidationFailed"
          entry.loggerName should include regex s"$loggerName.*$memberName"
        }

        def checkForMalformedRequestError(
            loggerName: String,
            memberName: String,
        )(
            entry: LogEntry
        ): Assertion = {
          entry.shouldBeCantonErrorCode(MalformedRequest)
          entry.message should include regex raw"(?s)Received a request.*with a view that is not correctly authorized. Rejecting request.*Missing authorization for party12.*"
          entry.loggerName should include regex s"$loggerName.*$memberName"
        }

        Seq[(LogEntryOptionality, LogEntry => Assertion)](
          LogEntryOptionality.Required -> checkForModelConformanceError(
            "TransactionConfirmationResponsesFactory",
            "participant1",
          ),
          LogEntryOptionality.Required -> checkForModelConformanceError(
            "TransactionConfirmationResponsesFactory",
            "participant2",
          ),
          LogEntryOptionality.Required -> checkForMalformedRequestError(
            "TransactionConfirmationResponsesFactory",
            "participant1",
          ),
          LogEntryOptionality.Required -> checkForMalformedRequestError(
            "TransactionConfirmationResponsesFactory",
            "participant2",
          ),
        )
      }

      loggerFactory.assertLogsUnorderedOptional(
        maliciousP1
          .submitCommand(
            cmdWithMetadata,
            transactionTreeInterceptor = remove_party12_fromSignatories,
          )
          .futureValueUS,
        expectedLogEntries *,
      )

      // Verify that participants are still functional
      assertPingSucceeds(participant1, participant2)
    }
  }

  private def mkUniversal(
      maintainers: List[PartyId],
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

final class ReferenceLedgerIntegrityIntegrationTestPostgres extends LedgerIntegrityIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
