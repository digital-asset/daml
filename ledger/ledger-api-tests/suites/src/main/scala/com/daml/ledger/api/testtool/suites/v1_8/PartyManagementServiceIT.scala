// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  AllocatePartyResponse,
  GetPartiesRequest,
  GetPartiesResponse,
  PartyDetails,
  UpdatePartyDetailsRequest,
  UpdatePartyDetailsResponse,
}
import com.daml.ledger.client.binding
import com.daml.ledger.test.model.Test.{Dummy}
import com.daml.lf.data.Ref
import scalaz.Tag
import scalaz.syntax.tag.ToTagOps
import java.util.regex.Pattern

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.client.binding.Primitive
import com.google.protobuf.field_mask.FieldMask

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

final class PartyManagementServiceIT
    extends LedgerTestSuite
    with PartyManagementServiceUpdateRpcTests
    with PartyManagementServiceAnnotationsValidationTests {

  def unsetResourceVersion[T](t: T): T = {
    val t2: T = t match {
      case u: PartyDetails => u.update(_.localMetadata.resourceVersion := "").asInstanceOf[T]
      case u: AllocatePartyResponse =>
        u.update(_.partyDetails.localMetadata.resourceVersion := "").asInstanceOf[T]
      case u: UpdatePartyDetailsResponse =>
        u.update(_.partyDetails.localMetadata.resourceVersion := "").asInstanceOf[T]
      case other => sys.error(s"could not match $other")
    }
    t2
  }

  def updateRequest(
      party: String,
      annotations: Map[String, String],
      updatePaths: Seq[String],
  ): UpdatePartyDetailsRequest =
    UpdatePartyDetailsRequest(
      partyDetails = Some(
        PartyDetails(
          party = party,
          localMetadata = Some(ObjectMeta(resourceVersion = "", annotations = annotations)),
        )
      ),
      updateMask = Some(FieldMask(updatePaths)),
    )

  def extractUpdatedAnnotations(
      updateResp: UpdatePartyDetailsResponse
  ): Map[String, String] =
    updateResp.partyDetails.get.localMetadata.get.annotations

  protected def withFreshParty[T](
      annotations: Map[String, String] = Map.empty
  )(
      f: PartyDetails => Future[T]
  )(implicit ledger: ParticipantTestContext, ec: ExecutionContext): Future[T] = {
    val req = AllocatePartyRequest(
      localMetadata = Some(
        ObjectMeta(
          resourceVersion = "",
          annotations = annotations,
        )
      )
    )
    for {
      create <- ledger.allocateParty(req)
      v <- f(create.partyDetails.get)
    } yield v
  }

  test(
    "PMNonEmptyParticipantID",
    "Asking for the participant identifier should return a non-empty string",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      participantId <- ledger.participantId()
    } yield {
      assert(participantId.nonEmpty, "The ledger returned an empty participant identifier")
    }
  })

  private val pMAllocateWithHint = "PMAllocateWithHint"
  test(
    pMAllocateWithHint,
    "It should be possible to provide a hint when allocating a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      party <- ledger.allocateParty(
        partyIdHint = Some(pMAllocateWithHint + "_" + Random.alphanumeric.take(10).mkString),
        displayName = Some("Bob Ross"),
      )
    } yield assert(
      Tag.unwrap(party).nonEmpty,
      "The allocated party identifier is an empty string",
    )
  })

  test(
    "PMAllocateWithoutHint",
    "It should be possible to not provide a hint when allocating a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      party <- ledger.allocateParty(partyIdHint = None, displayName = Some("Jebediah Kerman"))
    } yield assert(
      Tag.unwrap(party).nonEmpty,
      "The allocated party identifier is an empty string",
    )
  })

  test(
    "PMAllocateWithLocalMetadataAnnotations",
    "Successfully allocating a party with non-empty annotations",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      allocate1 <- ledger.allocateParty(
        AllocatePartyRequest(
          localMetadata = Some(
            ObjectMeta(annotations = Map("key1" -> "val1", "key2" -> "val2"))
          )
        )
      )
      allocatedParty = allocate1.partyDetails.get.party
      expectedPartyDetails = PartyDetails(
        party = allocatedParty,
        displayName = "",
        isLocal = true,
        localMetadata = Some(
          ObjectMeta(
            resourceVersion = allocate1.partyDetails.get.localMetadata.get.resourceVersion,
            annotations = Map(
              "key1" -> "val1",
              "key2" -> "val2",
            ),
          )
        ),
      )
      _ = assertEquals(
        allocate1,
        expected = AllocatePartyResponse(
          partyDetails = Some(
            expectedPartyDetails
          )
        ),
      )
      get1 <- ledger.getParties(GetPartiesRequest(Seq(allocatedParty)))
      _ = assertEquals(
        get1,
        expected = GetPartiesResponse(
          partyDetails = Seq(
            expectedPartyDetails
          )
        ),
      )
    } yield ()
  })

  private val pMAllocateWithoutDisplayName = "PMAllocateWithoutDisplayName"
  test(
    pMAllocateWithoutDisplayName,
    "It should be possible to not provide a display name when allocating a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      party <- ledger.allocateParty(
        partyIdHint =
          Some(pMAllocateWithoutDisplayName + "_" + Random.alphanumeric.take(10).mkString),
        displayName = None,
      )
    } yield assert(
      Tag.unwrap(party).nonEmpty,
      "The allocated party identifier is an empty string",
    )
  })

  test(
    "PMTestGetPartiesWithInvalidPartyIdentifier",
    "Failing when requesting party using an invalid party name",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      error <- ledger
        .getParties(
          GetPartiesRequest(
            parties = Seq("partyBad#$")
          )
        )
        .mustFail("Invalid party name")
      _ = assertGrpcError(
        error,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some(
          "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: non expected character 0x23 in Daml-LF Party \"partyBad#$\""
        ),
      )
    } yield ()
  })

  // TODO Merge into PMAllocateWithoutDisplayName once the empty-display-name assertion can be
  //      configured based on the Canton feature descriptor,
  test(
    "PMAllocateEmptyExpectMissingDisplayName",
    "A party allocation without display name must result in an empty display name in the queried party details",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      party <- ledger.allocateParty(
        partyIdHint =
          Some(pMAllocateWithoutDisplayName + "_" + Random.alphanumeric.take(10).mkString),
        displayName = None,
      )
      partiesDetails <- ledger.getParties(Seq(party))
    } yield {
      assert(
        Tag.unwrap(party).nonEmpty,
        "The allocated party identifier is an empty string",
      )
      val partyDetails = assertSingleton("Only one party requested", partiesDetails)
      assert(partyDetails.displayName.isEmpty, "The party display name is non-empty")
    }
  })

  test(
    "PMAllocateDuplicateDisplayName",
    "It should be possible to allocate parties with the same display names",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      p1 <- ledger.allocateParty(partyIdHint = None, displayName = Some("Ononym McOmonymface"))
      p2 <- ledger.allocateParty(partyIdHint = None, displayName = Some("Ononym McOmonymface"))
    } yield {
      assert(Tag.unwrap(p1).nonEmpty, "The first allocated party identifier is an empty string")
      assert(Tag.unwrap(p2).nonEmpty, "The second allocated party identifier is an empty string")
      assert(p1 != p2, "The two parties have the same party identifier")
    }
  })

  test(
    "PMRejectionDuplicateHint",
    "A party allocation request with a duplicate party hint should be rejected",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val hint = "party_hint" + "_" + Random.alphanumeric.take(10).mkString
    for {
      party <- ledger.allocateParty(partyIdHint = Some(hint), displayName = None)
      error <- ledger
        .allocateParty(partyIdHint = Some(hint), displayName = None)
        .mustFail("allocating a party with a duplicate hint")
    } yield {
      assert(
        Tag.unwrap(party).nonEmpty,
        "The allocated party identifier is an empty string",
      )
      assertGrpcErrorRegex(
        error,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some(Pattern.compile("Party already exists|PartyToParticipant")),
      )
    }
  })

  test(
    "PMRejectLongPartyHints",
    "A party identifier which is too long should be rejected with the proper error",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      error <- ledger
        .allocateParty(
          partyIdHint = Some(Random.alphanumeric.take(256).mkString),
          displayName = None,
        )
        .mustFail("allocating a party with a very long identifier")
    } yield {
      assertGrpcError(
        error,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some("Party is too long"),
      )
    }
  })

  test(
    "PMRejectInvalidPartyHints",
    "A party identifier that contains invalid characters should be rejected with the proper error",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      error <- ledger
        .allocateParty(
          // Assumption: emojis will never be acceptable in a party identifier
          partyIdHint = Some("\uD83D\uDE00"),
          displayName = None,
        )
        .mustFail("allocating a party with invalid characters")
    } yield {
      assertGrpcError(
        error,
        LedgerApiErrors.RequestValidation.InvalidArgument,
        Some("non expected character"),
      )
    }
  })

  test(
    "PMAllocateOneHundred",
    "It should create unique party names when allocating many parties",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      parties <- ledger.allocateParties(100)
    } yield {
      val nonUniqueNames = parties.groupBy(Tag.unwrap).view.mapValues(_.size).filter(_._2 > 1).toMap
      assert(
        nonUniqueNames.isEmpty,
        s"There are non-unique party names: ${nonUniqueNames
            .map { case (name, count) => s"$name ($count)" }
            .mkString(", ")}",
      )
    }
  })

  test(
    "PMGetPartiesDetails",
    "It should get details for multiple parties, if they exist",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      party1 <- ledger.allocateParty(
        partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString),
        displayName = Some("Alice"),
        localMetadata = Some(
          ObjectMeta(
            annotations = Map("k1" -> "v1")
          )
        ),
      )
      party2 <- ledger.allocateParty(
        partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString),
        displayName = Some("Bob"),
        localMetadata = Some(
          ObjectMeta(
            annotations = Map("k2" -> "v2")
          )
        ),
      )
      partyDetails <- ledger.getParties(
        Seq(party1, party2, binding.Primitive.Party("non-existent"))
      )
      noPartyDetails <- ledger.getParties(Seq(binding.Primitive.Party("non-existent")))
      zeroPartyDetails <- ledger.getParties(Seq.empty)
    } yield {
      assert(
        partyDetails.sortBy(_.displayName).map(unsetResourceVersion) == Seq(
          PartyDetails(
            party = Ref.Party.assertFromString(Tag.unwrap(party1)),
            displayName = "Alice",
            isLocal = true,
            localMetadata = Some(ObjectMeta(annotations = Map("k1" -> "v1"))),
          ),
          PartyDetails(
            party = Ref.Party.assertFromString(Tag.unwrap(party2)),
            displayName = "Bob",
            isLocal = true,
            localMetadata = Some(ObjectMeta(annotations = Map("k2" -> "v2"))),
          ),
        ),
        s"The allocated parties, ${Seq(party1, party2)}, were not retrieved successfully. Instead, got $partyDetails.",
      )
      assert(
        noPartyDetails.isEmpty,
        s"Retrieved some parties when the party specified did not exist: $noPartyDetails",
      )
      assert(
        zeroPartyDetails.isEmpty,
        s"Retrieved some parties when no parties were requested: $zeroPartyDetails",
      )
    }
  })

  test(
    "PMListKnownParties",
    "It should list all known, previously-allocated parties",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      party1 <- ledger.allocateParty(
        partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString),
        displayName = None,
        localMetadata = Some(ObjectMeta(annotations = Map("k1" -> "v1"))),
      )
      party2 <- ledger.allocateParty(
        partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString),
        displayName = None,
      )
      party3 <- ledger.allocateParty(
        partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString),
        displayName = None,
      )
      knownPartyResp <- ledger.listKnownParties_full()
      knownPartyIds = knownPartyResp.partyDetails.map(_.party).map(Primitive.Party(_)).toSet
    } yield {
      val allocatedPartyIds = Set(party1, party2, party3)
      assert(
        allocatedPartyIds subsetOf knownPartyIds,
        s"The allocated party IDs $allocatedPartyIds are not a subset of $knownPartyIds.",
      )
      val fetchedAllocatedPartiesSet = knownPartyResp.partyDetails.collect {
        case details if allocatedPartyIds.contains(Primitive.Party(details.party)) =>
          unsetResourceVersion(details)
      }.toSet
      assertEquals(
        fetchedAllocatedPartiesSet,
        expected = Set(
          PartyDetails(
            party = party1.toString,
            displayName = "",
            isLocal = true,
            localMetadata = Some(ObjectMeta(annotations = Map("k1" -> "v1"))),
          ),
          PartyDetails(
            party = party2.toString,
            displayName = "",
            isLocal = true,
            localMetadata = Some(ObjectMeta(annotations = Map.empty)),
          ),
          PartyDetails(
            party = party3.toString,
            displayName = "",
            isLocal = true,
            localMetadata = Some(ObjectMeta(annotations = Map.empty)),
          ),
        ),
      )
    }
  })

  test(
    "PMGetPartiesIsLocal",
    "GetParties should correctly report whether parties are local or not",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      // Running a dummy transaction seems to be required by the test framework to make parties visible via getParties
      _ <- alpha.create(alice, Dummy(alice))
      _ <- beta.create(bob, Dummy(bob))

      alphaParties <- alpha.getParties(Seq(alice, bob))
      betaParties <- beta.getParties(Seq(alice, bob))
    } yield {
      assert(
        alphaParties.exists(p => p.party == alice.unwrap && p.isLocal),
        "Missing expected party from first participant",
      )
      assert(
        betaParties.exists(p => p.party == bob.unwrap && p.isLocal),
        "Missing expected party from second participant",
      )

      // The following assertions allow some slack to distributed ledger implementations, as they can
      // either publish parties across participants as non-local or bar that from happening entirely.
      // Furthermore, as participants with matching ledger ids expose the "same ledger", such participants
      // are allowed to expose parties as local on multiple participants, and therefore restrict the asserts to
      // participants with different ledger ids.
      if (alpha.endpointId != beta.endpointId && alpha.ledgerId != beta.ledgerId) {
        assert(
          alphaParties.exists(p => p.party == bob.unwrap && !p.isLocal) || !alphaParties.exists(
            _.party == bob.unwrap
          ),
          "Unexpected remote party marked as local found on first participant",
        )
        assert(
          betaParties.exists(p => p.party == alice.unwrap && !p.isLocal) || !betaParties.exists(
            _.party == alice.unwrap
          ),
          "Unexpected remote party marked as local found on second participant",
        )
      }
    }
  })

}
