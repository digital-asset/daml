// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.ledger.api.v2.commands.{Command, DisclosedContract}
import com.daml.ledger.api.v2.event.CreatedEvent.toJavaProto
import com.daml.ledger.api.v2.event.{CreatedEvent, Event}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  TransactionFormat,
}
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.{Availability, Integrity}
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.Interpreter
import com.digitalasset.canton.ledger.error.groups.{CommandExecutionErrors, ConsistencyErrors}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.pretty.Implicits.*
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.SequencerBackpressure
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError.NotConnectedToAnySynchronizer
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  Node,
  TransactionCoder,
}
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future, blocking}
import scala.jdk.CollectionConverters.*
import scala.util.{Random, Success, Try}

sealed trait GracefulRejectsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with SecurityTestSuite {

  private val confirmationRequestsMaxRate = NonNegativeInt.tryCreate(6)
  private val maxRequestSize = NonNegativeInt.tryCreate(30000)
  private val ledgerTimeRecordTimeTolerance = 10.seconds

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateSequencerConfig("sequencer2") {
          _.focus(_.parameters.maxConfirmationRequestsBurstFactor)
            .replace(PositiveDouble.tryCreate(1e-5))
        }
      )
      .withSetup { implicit env =>
        import env.*

        synchronizerOwners1.foreach {
          _.topology.synchronizer_parameters
            .propose_update(
              synchronizerId = daId,
              _.update(
                maxRequestSize = maxRequestSize,
                ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
              ),
            )
        }
        synchronizerOwners2.foreach {
          _.topology.synchronizer_parameters.propose_update(
            synchronizerId = acmeId,
            _.update(
              confirmationRequestsMaxRate = confirmationRequestsMaxRate,
              maxRequestSize = maxRequestSize,
              ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
            ),
          )
        }

        Seq(participant1, participant2).synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
      }

  private def submit(
      actAs: Seq[String],
      commands: Seq[javaapi.data.Command],
      disclosedContracts: Seq[DisclosedContract],
  )(implicit env: FixtureParam): Unit = {
    import env.*

    env
      .run(
        env.grpcLedgerCommandRunner.runCommand(
          participant2.name,
          LedgerApiCommands.CommandService.SubmitAndWaitTransaction(
            actAs.map(LfPartyId.assertFromString),
            Seq.empty,
            commands.map(c => Command.fromJavaProto(c.toProtoCommand)),
            workflowId = "123",
            commandId = "",
            deduplicationPeriod = None,
            submissionId = UUID.randomUUID().toString,
            minLedgerTimeAbs = None,
            disclosedContracts = disclosedContracts,
            synchronizerId = None,
            userId = LedgerApiCommands.defaultUserId,
            Seq.empty,
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
            includeCreatedEventBlob = false,
            optTimeout = None,
          ),
          participant2.config.clientLedgerApi,
          participant2.adminToken,
        )
      )
      .discard
  }

  private val maliciousLedgerApiUser = "a malicious ledger api user"
  private val nodeAvailability: SecurityTest =
    SecurityTest(property = Availability, "Canton node")

  "A participant must compress encrypted views" taggedAs nodeAvailability.setAttack(
    Attack(
      actor = maliciousLedgerApiUser,
      threat = "perform a denial of service attack with a large command payload",
      mitigation = "reject a command with an excessively large contract view payload at submission",
    )
  ) in { implicit env =>
    import env.*
    // Since we decompress the views, we need to prevent excessively large views even if they compress well.
    val testId = "A" * 35000

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      createCycleContract(participant1, participant1.id.adminParty, id = testId),
      { logEntry =>
        logEntry.commandFailureMessage should include regex s"MaxViewSizeExceeded\\(view size .bytes. = .*, max request size configured .bytes. = .*\\)."
        logEntry.errorMessage should include("Malformed request")
      },
    )
    withClue("system remains functional after reject") {
      participant1.health.maybe_ping(participant1.id) shouldBe defined
    }

    // let's reduce the size a bit and check that it works
    val testId2 = "A" * 25000
    createCycleContract(participant1, participant1.id.adminParty, id = testId2)
  }

  "A participant" when {
    "a user submits a command with a large number of views" must {
      "reject the command" taggedAs nodeAvailability.setAttack(
        Attack(
          actor = maliciousLedgerApiUser,
          threat = "perform a denial of service attack with a large command payload",
          mitigation = "reject excessively large commands due to multiple encrypted views",
        )
      ) in { implicit env =>
        import env.*
        withClue("large messages are successfully bounced") {
          // Since we're compressing the views, we must use some uncompressable string.
          // A pseudo-random string should not be compressible.
          // Use a fixed seed for reproducibility
          // We also create multiple contracts to avoid hitting the limit on the encryptedView, but on the overall batch size.
          val testId = new Random(1).nextString(1000)
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            createCycleContracts(
              participant1,
              participant1.id.adminParty,
              ids = (1 to 35).map(i => s"$testId $i"),
            ),
            _.warningMessage should include("Failed to submit transaction due to "),
            _.commandFailureMessage should include regex s"Batch size \\(.* bytes\\) is exceeding maximum size \\(.* bytes\\) for synchronizer ${daName.unwrap}",
          )
        }
        withClue("system remains functional after reject") {
          participant1.health.maybe_ping(participant1.id) shouldBe defined
        }
      }
    }

    "a user submits excessively many commands at once" must {
      "reject the commands" taggedAs nodeAvailability
        .setAttack(
          Attack(
            actor = maliciousLedgerApiUser,
            threat = "perform a denial of service attack with excessively many commands",
            mitigation = "reject the commands",
          )
        ) in { implicit env =>
        import env.*

        val alice =
          participant2.parties.enable("Alice")
        val bob =
          participant2.parties.enable("Bob")

        withClue("sending too large command will fail ugly") {
          loggerFactory.suppressWarningsErrorsExceptions[CommandFailure] {
            participant2.ledger_api.javaapi.commands
              .submit(
                Seq(alice),
                Seq.fill(1000)(IouSyntax.testIou(alice, bob).create.commands.loneElement),
              )
          }
        }

        // but we should still be able to submit stuff
        IouSyntax.createIou(participant2)(alice, bob)
      }
    }

    def id(x: Int) = s"test-rate-limit-${"%02d" format x}"

    @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
    def createCycleCommand(id: String)(implicit env: TestConsoleEnvironment): Command = {
      import env.*
      val pkg = participant1.packages.find_by_module("Cycle").headOption.value
      ledger_api_utils.create(
        pkg.packageId,
        "Cycle",
        "Cycle",
        Map("owner" -> participant1.adminParty, "id" -> id),
      )
    }

    "a user submits commands within the synchronizer rate limit" must {
      "process the commands" in { implicit env =>
        import env.*

        // disconnect from da and connect to acme
        clue("disconnecting from da, connecting to acme") {
          participant1.synchronizers.disconnect(daName)
          participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
          participant1.dars.upload(CantonExamplesPath, synchronizerId = acmeId)
        }

        Threading.sleep(200) // Recover quota
        clue("command before starting the test should work") {
          val id0 = id(0)
          val cycle0 = createCycleCommand(id0)
          participant1.ledger_api.commands.submit(
            Seq(participant1.adminParty),
            Seq(cycle0),
            commandId = id0,
          )
        }
      }
    }

    "a user submits commands at an excessive rate" taggedAs nodeAvailability.setAttack(
      Attack(
        actor = maliciousLedgerApiUser,
        threat = "perform a denial of service attack by submitting commands at an excessive rate",
        mitigation = "reject on rate excesses",
      )
    ) in { implicit env =>
      import env.*

      val offset = participant1.ledger_api.state.end()

      val started = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        Future.traverse((1 to 5).toList)(x =>
          Future {
            blocking {
              val idx = id(x)
              val cyclex = createCycleCommand(idx)
              clue(s"submitting cycle $idx") {
                Try(
                  participant1.ledger_api.commands
                    .submit(Seq(participant1.adminParty), Seq(cyclex), commandId = idx)
                )
              }
            }
          }
        ),
        LogEntry.assertLogSeq(
          Seq(
            (
              { entry =>
                entry.shouldBeCantonErrorCode(SequencerBackpressure)
                // Important to use ABORTED, because it is not used by GRPC internally and it indicates that a retry makes sense.
                entry.message should include("ABORTED/")
              },
              "Message at the application",
            ),
            (
              _.warningMessage should include("Submission rate exceeds rate limit"),
              "Message at the participant node",
            ),
          )
        ),
      )

      try {
        clue("waiting for submissions to finish") {
          val res = Await.result(started, 200.seconds)
          val successes = res.collect { case Success(x) => x }
          successes.size should be <= 3 // actually 1, so expect 3 to have some safety margin
        }
      } finally {
        val completions =
          participant1.ledger_api.completions.list(participant1.adminParty, 5, offset)
        logger.debug(show"completions $completions")
      }
    }

    "the command submission quota has recovered" should {
      "continue accepting commands" taggedAs nodeAvailability.setHappyCase(
        "accept commands once the command submission quota has recovered"
      ) in { implicit env =>
        import env.*

        Threading.sleep(200) // we really need to sleep for the quota to recover

        clue("pinging after hitting resource exhausted should still work") {
          val idn = id(10)
          val cyclen = createCycleCommand(idn)
          participant1.ledger_api.commands.submit(
            Seq(participant1.adminParty),
            Seq(cyclen),
            commandId = idn,
          )
        }

        clue("disconnecting from acme, connecting to da") {
          participant1.synchronizers.disconnect(acmeName)
          participant1.synchronizers.reconnect(daName)
        }
      }
    }

    "gracefully rejects from ledger-api server" in { implicit env =>
      import env.*

      val bank =
        participant1.parties.enable(
          "bank",
          synchronizeParticipants = Seq(participant2),
        )
      val otherPartyOnP1 = participant1.parties.enable("otherParty")
      val owner =
        participant2.parties.enable(
          "owner",
          synchronizeParticipants = Seq(participant1),
        )

      def iouCommand(amount: Int) =
        new iou.Iou(
          bank.toProtoPrimitive,
          owner.toProtoPrimitive,
          new iou.Amount(amount.toBigDecimal, "USD"),
          List.empty.asJava,
        ).create.commands.loneElement

      // now, let's submit a transaction which should fail due to an invalid ensure (non-negative amounts)
      assertThrowsAndLogsCommandFailures(
        participant1.ledger_api.javaapi.commands.submit(Seq(bank), Seq(iouCommand(-10))),
        _.shouldBeCommandFailure(
          CommandExecutionErrors.Interpreter.UnhandledException
        ),
      )

      // now, let's try to send malformed inputs
      val malformed = {
        val tmp = iouCommand(10)
        // create empty command
        Command
          .fromJavaProto(tmp.toProtoCommand)
          .focus(_.command)
          .replace(com.daml.ledger.api.v2.commands.Command.Command.Empty)
      }
      clue("submitting malformed request") {
        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.commands.submit(Seq(bank), Seq(malformed)),
          // serialization errors are caught before we get the chance to rewrite the error
          _.commandFailureMessage should include regex "INVALID_ARGUMENT/MISSING_FIELD\\(8,.*\\): The submitted command is missing a mandatory field: command",
        )
      }
      // gracefully reject on an authorization error
      assertThrowsAndLogsCommandFailures(
        participant1.ledger_api.javaapi.commands
          .submit(Seq(otherPartyOnP1), Seq(iouCommand(10))),
        _.shouldBeCommandFailure(CommandExecutionErrors.Interpreter.AuthorizationError),
      )

      val iouTx =
        participant1.ledger_api.javaapi.commands.submit(Seq(bank), Seq(iouCommand(10)))
      val iouCid =
        JavaDecodeUtil
          .decodeAllCreated(iou.Iou.COMPANION)(iouTx)
          .headOption
          .valueOrFail("should be there")

      val exerciseCmd =
        iouCid.id.exerciseTransfer(bank.toProtoPrimitive).commands.asScala.toSeq
      participant2.ledger_api.javaapi.commands.submit(Seq(owner), exerciseCmd)

      // gracefully reject on duplicate spent
      assertThrowsAndLogsCommandFailures(
        participant2.ledger_api.javaapi.commands.submit(Seq(owner), exerciseCmd),
        _.shouldBeCommandFailure(ConsistencyErrors.ContractNotFound),
      )

    }

    "gracefully reject if a command is submitted with no connected synchronizers" in {
      implicit env =>
        import env.*

        clue("disconnecting participant1 from all synchronizers") {
          participant1.synchronizers.list_connected().foreach { item =>
            participant1.synchronizers.disconnect(item.synchronizerAlias.unwrap)
          }
        }

        clue("try to submit a ledger command with no connected synchronizers on participant1") {
          assertThrowsAndLogsCommandFailures(
            createCycleContract(participant1, participant1.id.adminParty, id = "will-fail"),
            _.commandFailureMessage should include(NotConnectedToAnySynchronizer.id),
          )
        }
    }

    "being submitted a command with invalid disclosed contracts" must {
      "reject the request" taggedAs SecurityTest(
        property = Integrity,
        asset = "virtual shared ledger",
        attack = Attack(
          actor = "a malicious ledger api user",
          threat = "submits a request with a mismatching disclosed contract payload",
          mitigation = "the submitting participant rejects the request",
        ),
      ) in { implicit env =>
        import env.*

        def iouCommand(payer: String, owner: String) =
          new iou.Iou(
            payer,
            owner,
            new iou.Amount(10.toBigDecimal, "USD"),
            List.empty.asJava,
          ).create.commands.loneElement

        val beforeSubmission =
          participant2.ledger_api.state.end()
        val charlie =
          participant2.parties.enable("Charlie")
        submit(Seq(charlie.toLf), Seq(iouCommand(charlie.toLf, charlie.toLf)), Seq.empty).discard

        val txs = participant2.ledger_api.updates.transactions_with_tx_format(
          transactionFormat = TransactionFormat(
            Some(
              EventFormat(
                filtersByParty = Map(
                  charlie.toLf -> Filters(
                    Seq(
                      CumulativeFilter(
                        IdentifierFilter.TemplateFilter(
                          TemplateFilter(
                            templateId =
                              Some(Identifier.fromJavaProto(iou.Iou.TEMPLATE_ID.toProto)),
                            includeCreatedEventBlob = true,
                          )
                        )
                      )
                    )
                  )
                ),
                filtersForAnyParty = None,
                verbose = false,
              )
            ),
            TRANSACTION_SHAPE_ACS_DELTA,
          ),
          completeAfter = 1,
          beforeSubmission,
        )

        val createdEvent = inside(txs) {
          case Seq(
                TransactionWrapper(
                  Transaction(
                    _,
                    _,
                    _,
                    _,
                    Seq(Event(Event.Event.Created(createdEvent))),
                    _,
                    _,
                    _,
                    _,
                    _,
                  )
                )
              ) =>
            createdEvent
        }

        val contract = JavaDecodeUtil
          .decodeCreated(iou.Iou.COMPANION)(
            javaapi.data.CreatedEvent.fromProto(toJavaProto(createdEvent))
          )
          .valueOrFail(s"Could not decode a ${classOf[iou.Iou]} from the provided create event")

        val exercise = contract.id.exerciseArchive().commands.loneElement

        def createEventToDisclosedContract(ev: CreatedEvent): DisclosedContract =
          DisclosedContract(
            templateId = ev.templateId,
            contractId = ev.contractId,
            createdEventBlob = ev.createdEventBlob,
            synchronizerId = "",
          )

        val disclosedContract = createEventToDisclosedContract(createdEvent)

        def modifyCreatedEventBlob(createdEventBlob: ByteString)(
            modifyAuthenticationData: Bytes => Bytes = identity,
            modifyCreateTime: CreationTime => CreationTime = identity,
        ): ByteString =
          TransactionCoder
            .decodeFatContractInstance(createdEventBlob)
            .left
            .map(err => s"Failed $err")
            .flatMap { contract =>
              val create = Node.Create(
                coid = contract.contractId,
                templateId = contract.templateId,
                packageName = contract.packageName,
                arg = contract.createArg,
                signatories = contract.signatories,
                stakeholders = contract.stakeholders,
                keyOpt = contract.contractKeyWithMaintainers,
                version = contract.version,
              )
              TransactionCoder
                .encodeFatContractInstance(
                  FatContractInstance.fromCreateNode(
                    create = create,
                    createTime = modifyCreateTime(contract.createdAt),
                    authenticationData = modifyAuthenticationData(contract.authenticationData),
                  )
                )
                .left
                .map(err => s"Failed $err")
            }
            .fold(fail(_), identity)

        clue(s"submitting invalid disclosed contract") {
          val invalidDisclosedContract = disclosedContract.update(
            _.createdEventBlob.modify(
              modifyCreatedEventBlob(_)(modifyAuthenticationData =
                _ => Bytes.fromByteString(ByteString.copyFromUtf8("invalid"))
              )
            )
          )
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            submit(
              Seq(charlie.toLf),
              Seq(exercise),
              Seq(invalidDisclosedContract),
            ),
            _.shouldBeCommandFailure(Interpreter.UpgradeError.AuthenticationFailed),
          )
        }

        clue(s"submitting disclosed contract with mismatching payload/metadata") {
          val invalidDisclosedContract = disclosedContract.update(
            _.createdEventBlob.modify(
              modifyCreatedEventBlob(_)(modifyCreateTime = {
                case CreationTime.CreatedAt(timestamp) =>
                  CreationTime.CreatedAt(timestamp.addMicros(1337L))
                case CreationTime.Now => fail("Invalid creation time")
              })
            )
          )
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            submit(
              Seq(charlie.toLf),
              Seq(exercise),
              Seq(invalidDisclosedContract),
            ),
            _.shouldBeCommandFailure(Interpreter.UpgradeError.AuthenticationFailed),
          )
        }

        clue(s"submitting valid disclosed contract") {
          submit(Seq(charlie.toLf), Seq(exercise), Seq(disclosedContract))
        }
      }
    }
  }

  "gracefully reject if a command is submitted with an invalid party identifier" in {
    implicit env =>
      import env.*

      participant2.dars.upload(CantonExamplesPath)

      def iouCommand(payer: String, owner: String) =
        new iou.Iou(
          payer,
          owner,
          new iou.Amount(10.toBigDecimal, "USD"),
          List.empty.asJava,
        ).create.commands.loneElement

      val payer = participant2.id.adminParty
      val owner = participant1.id.adminParty
      //      def expectedLog(entry: LogEntry, party: String): Assertion = {
      //        entry.shouldBeCantonErrorCode(MalformedInputErrors.InvalidPartyIdentifier)
      //        entry.errorMessage should include(party.take(10))
      //      }
      def expectedLog(entry: LogEntry, errorCode: ErrorCode): Assertion =
        // TODO(#25385): Move party validation before synchronizer routing and re-establish the error assertions
        //        entry.shouldBeCantonErrorCode(MalformedInputErrors.InvalidPartyIdentifier)
        //        entry.errorMessage should include(party.take(10))
        entry.shouldBeCantonErrorCode(errorCode)
      Seq(
        "Alice",
        "::Alice::",
      ).foreach { party =>
        clue(s"submitting invalid actor $party") {
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            submit(Seq(party), Seq(iouCommand(party, owner.toLf)), Seq.empty),
            expectedLog(_, TransactionRoutingError.TopologyErrors.UnknownSubmitters),
          )
        }
        clue(s"submitting invalid informee $party") {
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            submit(Seq(payer.toLf), Seq(iouCommand(payer.toLf, party)), Seq.empty),
            expectedLog(_, TransactionRoutingError.TopologyErrors.UnknownInformees),
          )
        }
      }
  }
}

class GracefulRejectsReferenceIntegrationTestPostgres extends GracefulRejectsIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
}

class GracefulRejectsBftOrderingIntegrationTestPostgres extends GracefulRejectsIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
}
