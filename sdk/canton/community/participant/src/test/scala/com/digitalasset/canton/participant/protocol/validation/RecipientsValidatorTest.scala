// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.ViewPosition.MerkleSeqIndex
import com.digitalasset.canton.data.{CantonTimestamp, Informee, PlainInformee, ViewPosition}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.WrongRecipients
import com.digitalasset.canton.participant.protocol.TestProcessingSteps.TestViewTree
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.{RequestId, RootHash, ViewHash}
import com.digitalasset.canton.sequencing.protocol.{Recipient, Recipients, RecipientsTree}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{Member, ParticipantId, TestingTopology}
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, LfPartyId}

import scala.annotation.tailrec

class RecipientsValidatorTest extends BaseTestWordSpec with HasExecutionContext {

  case class TestInput(viewTree: TestViewTree, recipients: Recipients)

  lazy val validator = new RecipientsValidator[TestInput](_.viewTree, _.recipients, loggerFactory)

  lazy val requestId: RequestId = RequestId(CantonTimestamp.Epoch)

  lazy val inactive: LfPartyId = LfPartyId.assertFromString("inactive::default")

  lazy val party1: LfPartyId = LfPartyId.assertFromString("party1::default")
  lazy val participant1: ParticipantId = ParticipantId("participant1")

  lazy val party2: LfPartyId = LfPartyId.assertFromString("party2::default")
  lazy val participant2: ParticipantId = ParticipantId("participant2")

  lazy val snapshot: TopologySnapshot =
    TestingTopology(topology =
      Map(
        inactive -> Map.empty,
        party1 -> Map(participant1 -> ParticipantPermission.Submission),
        party2 -> Map(participant2 -> ParticipantPermission.Submission),
      )
    ).build(loggerFactory).topologySnapshot()

  def viewHash(i: Int): ViewHash = ViewHash(TestHash.digest(i))

  def mkRootHash(i: Int): RootHash = RootHash(TestHash.digest(i))

  def informeeOf(parties: LfPartyId*): Set[Informee] = parties.map(PlainInformee).toSet

  @tailrec
  final def mkViewPosition(depth: Int, acc: ViewPosition = ViewPosition.root): ViewPosition =
    if (depth <= 0) acc else mkViewPosition(depth - 1, MerkleSeqIndex(List.empty) +: acc)

  @tailrec
  final def mkRecipients[A <: Member](
      groupsViewToRoot: Seq[NonEmpty[Set[A]]],
      acc: Seq[RecipientsTree] = Seq.empty,
  ): Recipients = (groupsViewToRoot: @unchecked) match {
    case Seq() => Recipients(NonEmpty.from(acc).value)
    case Seq(head, tail @ _*) =>
      mkRecipients(tail, Seq(RecipientsTree(head.map(Recipient(_)), acc)))
  }

  def mkGroup(member: Member, members: Member*): NonEmpty[Set[Recipient]] =
    NonEmpty(Set, member, members *).map(Recipient(_))

  def mkInput1(
      informees: Seq[LfPartyId],
      members: NonEmpty[Seq[Member]],
      rootHash: Int = 0,
      viewDepth: Int = 1,
  ): TestInput = {
    val viewTree = TestViewTree(
      viewHash(1),
      mkRootHash(rootHash),
      informeeOf(informees: _*).map(_.party),
      viewPosition = mkViewPosition(viewDepth),
    )
    val recipients = Recipients.cc(members.head1, members.tail1: _*)
    TestInput(viewTree, recipients)
  }

  def mkInput2(
      informees: Seq[LfPartyId],
      recipients: Recipients,
      rootHash: Int = 0,
      viewDepth: Int = 1,
  ): TestInput = {
    val viewTree = TestViewTree(
      viewHash(1),
      mkRootHash(rootHash),
      informeeOf(informees: _*).map(_.party),
      viewPosition = mkViewPosition(viewDepth),
    )
    TestInput(viewTree, recipients)
  }

  "The recipients validator" when {
    "a single view is ok" must {
      "retain the view" in {
        val input = mkInput1(Seq(party1, party2), NonEmpty(Seq, participant1, participant2))

        val (wrongRecipients, goodInputs) = validator
          .retainInputsWithValidRecipients(requestId, Seq(input), snapshot)
          .futureValue

        goodInputs shouldBe Seq(input)
        wrongRecipients shouldBe empty
      }
    }

    "two nested views are ok" must {
      "retain all views" in {
        val parentInput = mkInput1(Seq(party1), NonEmpty(Seq, participant1), viewDepth = 1)
        val childInput = mkInput2(
          Seq(party1, party2),
          mkRecipients(Seq(NonEmpty(Set, participant1, participant2), NonEmpty(Set, participant1))),
          viewDepth = 2,
        )

        val (wrongRecipients, goodInputs) = validator
          .retainInputsWithValidRecipients(
            requestId,
            Seq(parentInput, childInput),
            snapshot,
          )
          .futureValue

        goodInputs shouldBe Seq(parentInput, childInput)
        wrongRecipients shouldBe empty
      }
    }

    "a view has a party without an active participant" must {
      "discard the view" in {
        val input = mkInput1(Seq(inactive, party1), NonEmpty(Seq, participant1))

        val (wrongRecipients, goodInputs) = loggerFactory.assertLogs(
          validator
            .retainInputsWithValidRecipients(requestId, Seq(input), snapshot)
            .futureValue,
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ shouldBe s"""Received a request with id $requestId where the view at ViewPosition("") has informees without an active participant: Set(inactive::default). Discarding ViewPosition("")...""",
          ),
        )

        goodInputs shouldBe empty
        wrongRecipients.loneElement shouldBe WrongRecipients(input.viewTree)
      }
    }

    "views have different root hashes" must {
      "throw an exception" in {
        val input1 = mkInput1(Seq(party1), NonEmpty(Seq, participant1), rootHash = 0)
        val input2 = mkInput1(Seq(party1), NonEmpty(Seq, participant1), rootHash = 1)

        loggerFactory.assertInternalError[IllegalArgumentException](
          validator.retainInputsWithValidRecipients(
            requestId,
            Seq(input1, input2),
            snapshot,
          ),
          _.getMessage should startWith("Views with different root hashes are not supported:"),
        )
      }
    }

    "a subview has a problem" must {
      "also discard the parent view" in {
        val parentInput = mkInput1(Seq(party1), NonEmpty(Seq, participant1), viewDepth = 1)
        val childInput = mkInput2(
          Seq(party1, inactive),
          mkRecipients(Seq(NonEmpty(Set, participant1), NonEmpty(Set, participant1))),
          viewDepth = 2,
        )

        val (wrongRecipients, goodInputs) = loggerFactory.assertLogs(
          validator
            .retainInputsWithValidRecipients(
              requestId,
              Seq(parentInput, childInput),
              snapshot,
            )
            .futureValue,
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ shouldBe s"""Received a request with id $requestId where the view at ViewPosition("", "") has informees without an active participant: Set(inactive::default). Discarding ViewPosition("", "")...""",
          ),
        )

        goodInputs shouldBe empty
        wrongRecipients shouldBe Seq(
          WrongRecipients(parentInput.viewTree),
          WrongRecipients(childInput.viewTree),
        )
      }
    }

    "a view has a missing recipient" must {
      "discard the view" in {
        val input = mkInput1(Seq(party1, party2), NonEmpty(Seq, participant1))

        val (wrongRecipients, goodInputs) = loggerFactory.assertLogs(
          validator
            .retainInputsWithValidRecipients(requestId, Seq(input), snapshot)
            .futureValue,
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ shouldBe s"""Received a request with id $requestId where the view at List("") has missing recipients Set(Recipient(PAR::participant2::default)) for the view at List(""). Discarding List("") with all ancestors...""",
          ),
        )

        goodInputs shouldBe empty
        wrongRecipients.loneElement shouldBe WrongRecipients(input.viewTree)
      }
    }

    "a parent view has a missing recipient" must {
      "discard the view" in {
        val parentInput =
          mkInput1(Seq(party1, party2), NonEmpty(Seq, participant1, participant2), viewDepth = 1)

        val childInput = mkInput2(
          Seq(party1),
          mkRecipients(Seq(NonEmpty(Set, participant1), NonEmpty(Set, participant1))),
          viewDepth = 2,
        )

        val (wrongRecipients, goodInputs) = loggerFactory.assertLogs(
          validator
            .retainInputsWithValidRecipients(
              requestId,
              Seq(parentInput, childInput),
              snapshot,
            )
            .futureValue,
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ shouldBe s"""Received a request with id $requestId where the view at List("", "") has missing recipients Set(Recipient(PAR::participant2::default)) for the view at List(""). Discarding List("") with all ancestors...""",
          ),
        )

        goodInputs.loneElement shouldBe childInput
        wrongRecipients.loneElement shouldBe WrongRecipients(parentInput.viewTree)
      }
    }

    "a view has an extra recipient" must {
      "alarm and process the view" in {
        val input = mkInput1(Seq(party1), NonEmpty(Seq, participant1, participant2))

        val (wrongRecipients, goodInputs) = loggerFactory.assertLogs(
          validator
            .retainInputsWithValidRecipients(requestId, Seq(input), snapshot)
            .futureValue,
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ shouldBe s"""Received a request with id $requestId where the view at List("") has extra recipients Set(Recipient(PAR::participant2::default)) for the view at List(""). Continue processing...""",
          ),
        )

        goodInputs.loneElement shouldBe input
        wrongRecipients shouldBe empty
      }
    }

    "a parent view has an extra recipient" must {
      "alarm and process the view" in {
        val parentInput = mkInput1(Seq(party1), NonEmpty(Seq, participant1), viewDepth = 1)
        val childInput = mkInput2(
          Seq(party1),
          mkRecipients(Seq(NonEmpty(Set, participant1), NonEmpty(Set, participant1, participant2))),
          viewDepth = 2,
        )

        val (wrongRecipients, goodInputs) = loggerFactory.assertLogs(
          validator
            .retainInputsWithValidRecipients(
              requestId,
              Seq(parentInput, childInput),
              snapshot,
            )
            .futureValue,
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ shouldBe s"""Received a request with id $requestId where the view at List("", "") has extra recipients Set(Recipient(PAR::participant2::default)) for the view at List(""). Continue processing...""",
          ),
        )

        goodInputs shouldBe Seq(parentInput, childInput)
        wrongRecipients shouldBe empty
      }
    }

    "the recipients have an extra group" must {
      "discard the group" in {
        val recipients = mkRecipients(Seq(NonEmpty(Set, participant1), NonEmpty(Set, participant2)))
        val input = mkInput2(
          Seq(party1),
          recipients,
          viewDepth = 0,
        )

        val (wrongRecipients, goodInputs) = loggerFactory.assertLogs(
          validator
            .retainInputsWithValidRecipients(requestId, Seq(input), snapshot)
            .futureValue,
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ shouldBe
              s"""Received a request with id $requestId where the view at List() has too many levels of recipients. Continue processing...\n$recipients""",
          ),
        )

        goodInputs.loneElement shouldBe input
        wrongRecipients shouldBe empty
      }
    }

    "the recipients for a parent view are missing" must {
      "discard the parent view" in {
        val parentInput = mkInput1(Seq(party1), NonEmpty(Seq, participant1), viewDepth = 1)
        val childInput = mkInput1(Seq(party1), NonEmpty(Seq, participant1), viewDepth = 2)

        val (wrongRecipients, goodInputs) = loggerFactory.assertLogs(
          validator
            .retainInputsWithValidRecipients(
              requestId,
              Seq(parentInput, childInput),
              snapshot,
            )
            .futureValue,
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ shouldBe s"""Received a request with id $requestId where the view at List("", "") has no recipients group for List(""). Discarding List("") with all ancestors...""",
          ),
        )

        goodInputs.loneElement shouldBe childInput
        wrongRecipients.loneElement shouldBe WrongRecipients(parentInput.viewTree)
      }
    }

    "the view corresponding to a recipients group is missing" must {
      "discard all ancestors of the missing view" in {
        val parentInput = mkInput1(Seq(party1), NonEmpty(Seq, participant1), viewDepth = 0)
        val childInput = mkInput2(
          Seq(party1),
          mkRecipients(
            Seq(
              NonEmpty(Set, participant1),
              NonEmpty(Set, participant1),
              NonEmpty(Set, participant1),
            )
          ),
          viewDepth = 2,
        )

        val (wrongRecipients, goodInputs) = loggerFactory.assertLogs(
          validator
            .retainInputsWithValidRecipients(
              requestId,
              Seq(parentInput, childInput),
              snapshot,
            )
            .futureValue,
          _.shouldBeCantonError(
            SyncServiceAlarm,
            _ shouldBe s"""Received a request with id $requestId where the view at List("") is missing. Discarding all ancestors of List("")...""",
          ),
        )

        goodInputs.loneElement shouldBe childInput
        wrongRecipients.loneElement shouldBe WrongRecipients(parentInput.viewTree)
      }
    }

    "a recipients tree has a non-linear structure" when {
      "there are problems on all paths" must {
        "discard all views" in {
          val parentInput =
            mkInput1(Seq(party1, party2), NonEmpty(Seq, participant1, participant2), viewDepth = 1)

          val childRecipients =
            Recipients(
              NonEmpty(
                Seq,
                RecipientsTree(
                  mkGroup(participant1),
                  Seq(
                    RecipientsTree(mkGroup(participant1), Seq.empty),
                    RecipientsTree(
                      mkGroup(participant1, participant2),
                      Seq.empty,
                    ),
                  ),
                ),
              )
            )
          val childInput = mkInput2(Seq(party1, party2), childRecipients, viewDepth = 2)

          val (wrongRecipients, goodInputs) = loggerFactory.assertLogs(
            validator
              .retainInputsWithValidRecipients(
                requestId,
                Seq(parentInput, childInput),
                snapshot,
              )
              .futureValue,
            _.shouldBeCantonError(
              SyncServiceAlarm,
              _ should startWith(
                s"""Received a request with id $requestId where the view at ViewPosition("", "") has a non-linear recipients tree. Processing all paths of the tree.
                   |Recipients""".stripMargin
              ),
            ),
            _.shouldBeCantonError(
              SyncServiceAlarm,
              _ shouldBe s"""Received a request with id $requestId where the view at List("", "") has missing recipients Set(Recipient(PAR::participant2::default)) for the view at List(""). Discarding List("") with all ancestors...""",
            ),
          )

          goodInputs.loneElement shouldBe childInput
          wrongRecipients.loneElement shouldBe WrongRecipients(parentInput.viewTree)
        }
      }

      "there is a path without problems" must {
        "process all views" in {
          val parentInput =
            mkInput1(Seq(party1, party2), NonEmpty(Seq, participant1, participant2), viewDepth = 1)

          val childRecipients =
            Recipients(
              NonEmpty(
                Seq,
                RecipientsTree(
                  mkGroup(participant1, participant2),
                  Seq(
                    RecipientsTree(mkGroup(participant1), Seq.empty),
                    RecipientsTree(
                      mkGroup(participant1, participant2),
                      Seq.empty,
                    ),
                  ),
                ),
              )
            )
          val childInput = mkInput2(Seq(party1, party2), childRecipients, viewDepth = 2)

          val (wrongRecipients, goodInputs) = loggerFactory.assertLogs(
            validator
              .retainInputsWithValidRecipients(
                requestId,
                Seq(parentInput, childInput),
                snapshot,
              )
              .futureValue,
            _.shouldBeCantonError(
              SyncServiceAlarm,
              _ should startWith(
                s"""Received a request with id $requestId where the view at ViewPosition("", "") has a non-linear recipients tree. Processing all paths of the tree.
                   |Recipients""".stripMargin
              ),
            ),
          )

          goodInputs shouldBe Seq(parentInput, childInput)
          wrongRecipients shouldBe empty
        }
      }
    }
  }
}
