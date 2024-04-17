// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.util.UUID

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  AllocatePartyResponse,
  GetPartiesRequest,
  GetPartiesResponse,
  ListKnownPartiesRequest,
  ListKnownPartiesResponse,
  PartyDetails,
  UpdatePartyIdentityProviderRequest,
}
import com.daml.ledger.test.java.model.test.Dummy
import com.daml.lf.data.Ref
import java.util.regex.Pattern

import com.daml.ledger.api.testtool.infrastructure.NamePicker
import com.daml.ledger.api.v1.admin.identity_provider_config_service.DeleteIdentityProviderConfigRequest
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.javaapi.data.Party

import scala.concurrent.Future
import scala.util.Random

final class PartyManagementServiceIT extends PartyManagementITBase {
  import CompanionImplicits._

  val namePicker: NamePicker = NamePicker(
    "-_ 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  )

  test(
    "PMUpdatingPartyIdentityProviderNonDefaultIdps",
    "Test reassigning party to a different idp using non default idps",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val idpId1 = ledger.nextIdentityProviderId()
    val idpId2 = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId1)
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId2)
      party <- ledger.allocateParty(identityProviderId = Some(idpId1))
      get1 <- ledger.getParties(
        GetPartiesRequest(parties = Seq(party), identityProviderId = idpId1)
      )
      // Update party's idp id
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderRequest(
          party = party,
          sourceIdentityProviderId = idpId1,
          targetIdentityProviderId = idpId2,
        )
      )
      get2 <- ledger.getParties(
        GetPartiesRequest(parties = Seq(party), identityProviderId = idpId2)
      )
      get3 <- ledger.getParties(
        GetPartiesRequest(parties = Seq(party), identityProviderId = idpId1)
      )
      // Cleanup
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderRequest(
          party = party,
          sourceIdentityProviderId = idpId2,
          targetIdentityProviderId = "",
        )
      )
      _ <- ledger.deleteIdentityProviderConfig(DeleteIdentityProviderConfigRequest(idpId1))
      _ <- ledger.deleteIdentityProviderConfig(DeleteIdentityProviderConfigRequest(idpId2))
    } yield {
      assertEquals(
        "is idp1, request as idp1",
        get1.partyDetails.map(d => d.identityProviderId -> d.party -> d.isLocal),
        Seq(idpId1 -> party.getValue -> true),
      )
      assertEquals(
        "is idp2, request as idp2",
        get2.partyDetails.map(d => d.identityProviderId -> d.party -> d.isLocal),
        Seq(idpId2 -> party.getValue -> true),
      )
      assertEquals(
        "is idp2, request as idp1",
        get3.partyDetails.map(d => d.identityProviderId -> d.party -> d.isLocal),
        // party and isLocal values get blinded
        Seq("" -> party.getValue -> false),
      )

    }
  })

  test(
    "PMUpdatingPartyIdentityProviderWithDefaultIdp",
    "Test reassigning party to a different idp using the default idp",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val idpId1 = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId1)
      // allocate a party in the default idp
      party <- ledger.allocateParty(identityProviderId = None)
      get1 <- ledger.getParties(GetPartiesRequest(parties = Seq(party), identityProviderId = ""))
      // Update party's idp id
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderRequest(
          party = party,
          sourceIdentityProviderId = "",
          targetIdentityProviderId = idpId1,
        )
      )
      get2 <- ledger.getParties(
        GetPartiesRequest(parties = Seq(party), identityProviderId = idpId1)
      )
      // Cleanup - changing party's idp to the default idp so that non default one can be deleted
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderRequest(
          party = party,
          sourceIdentityProviderId = idpId1,
          targetIdentityProviderId = "",
        )
      )
      _ <- ledger.deleteIdentityProviderConfig(DeleteIdentityProviderConfigRequest(idpId1))
    } yield {
      assertEquals(
        "default idp",
        get1.partyDetails.map(d => d.identityProviderId -> d.party -> d.isLocal),
        Seq("" -> party.getValue -> true),
      )
      assertEquals(
        "non default idp",
        get2.partyDetails.map(d => d.identityProviderId -> d.party -> d.isLocal),
        Seq(idpId1 -> party.getValue -> true),
      )
    }
  })

  test(
    "PMUpdatingPartyIdentityProviderNonExistentIdps",
    "Test reassigning party to a different idp when source or target idp doesn't exist",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val idpIdNonExistent = ledger.nextIdentityProviderId()
    for {
      party <- ledger.allocateParty(identityProviderId = None)
      _ <- ledger
        .updatePartyIdentityProviderId(
          UpdatePartyIdentityProviderRequest(
            party = party,
            sourceIdentityProviderId = idpIdNonExistent,
            targetIdentityProviderId = "",
          )
        )
        .mustFailWith(
          "non existent source idp",
          LedgerApiErrors.RequestValidation.InvalidArgument,
        )
      _ <- ledger
        .updatePartyIdentityProviderId(
          UpdatePartyIdentityProviderRequest(
            party = party,
            sourceIdentityProviderId = "",
            targetIdentityProviderId = idpIdNonExistent,
          )
        )
        .mustFailWith(
          "non existent target idp",
          LedgerApiErrors.RequestValidation.InvalidArgument,
        )
    } yield ()
  })

  test(
    "PMUpdatingPartyIdentityProviderMismatchedSourceIdp",
    "Test reassigning party to a different idp using mismatched source idp id",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val idpIdNonDefault = ledger.nextIdentityProviderId()
    val idpIdTarget = ledger.nextIdentityProviderId()
    val idpIdMismatched = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpIdNonDefault)
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpIdMismatched)
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpIdTarget)
      partyDefault <- ledger.allocateParty(identityProviderId = None)
      partyNonDefault <- ledger
        .allocateParty(identityProviderId = Some(idpIdNonDefault))

      _ <- ledger
        .updatePartyIdentityProviderId(
          UpdatePartyIdentityProviderRequest(
            party = partyDefault,
            sourceIdentityProviderId = idpIdMismatched,
            targetIdentityProviderId = idpIdTarget,
          )
        )
        .mustFailWith(
          "mismatched source idp id",
          LedgerApiErrors.Admin.PartyManagement.PartyNotFound,
        )
      _ <- ledger
        .updatePartyIdentityProviderId(
          UpdatePartyIdentityProviderRequest(
            party = partyNonDefault,
            sourceIdentityProviderId = idpIdMismatched,
            targetIdentityProviderId = idpIdTarget,
          )
        )
        .mustFailWith(
          "mismatched source idp id",
          LedgerApiErrors.Admin.PartyManagement.PartyNotFound,
        )
      // cleanup
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderRequest(
          party = partyNonDefault,
          sourceIdentityProviderId = idpIdNonDefault,
          targetIdentityProviderId = "",
        )
      )
      _ <- ledger.deleteIdentityProviderConfig(DeleteIdentityProviderConfigRequest(idpIdNonDefault))
      _ <- ledger.deleteIdentityProviderConfig(DeleteIdentityProviderConfigRequest(idpIdMismatched))
    } yield ()
  })

  test(
    "PMUpdatingPartyIdentityProviderSourceAndTargetIdpTheSame",
    "Test reassigning party to a different idp but source and target idps are the same",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val idpId1 = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId1)
      partyDefault <- ledger.allocateParty(identityProviderId = None)
      partyNonDefault <- ledger.allocateParty(identityProviderId = Some(idpId1))
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderRequest(
          party = partyDefault,
          sourceIdentityProviderId = "",
          targetIdentityProviderId = "",
        )
      )
      get1 <- ledger.getParties(
        GetPartiesRequest(parties = Seq(partyDefault), identityProviderId = "")
      )
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderRequest(
          party = partyNonDefault,
          sourceIdentityProviderId = idpId1,
          targetIdentityProviderId = idpId1,
        )
      )
      get2 <- ledger.getParties(
        GetPartiesRequest(parties = Seq(partyNonDefault), identityProviderId = idpId1)
      )
      // cleanup
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderRequest(
          party = partyNonDefault,
          sourceIdentityProviderId = idpId1,
          targetIdentityProviderId = "",
        )
      )
      _ <- ledger.deleteIdentityProviderConfig(DeleteIdentityProviderConfigRequest(idpId1))
    } yield {
      assertEquals(
        "default idp",
        get1.partyDetails.map(d => d.identityProviderId -> d.party -> d.isLocal),
        Seq("" -> partyDefault.getValue -> true),
      )
      assertEquals(
        "non default idp",
        get2.partyDetails.map(d => d.identityProviderId -> d.party -> d.isLocal),
        Seq(idpId1 -> partyNonDefault.getValue -> true),
      )
    }
  })

  test(
    "PMGetPartiesUsingDifferentIdps",
    "Test getting parties using different idps",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val idpId1 = ledger.nextIdentityProviderId()
    val idpId2 = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId1)
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId2)
      partyDefault <- ledger.allocateParty(identityProviderId = None)
      partyNonDefault <- ledger.allocateParty(identityProviderId = Some(idpId1))
      partyOtherNonDefault <- ledger.allocateParty(identityProviderId = Some(idpId2))
      getAsDefaultIdp <- ledger.getParties(
        GetPartiesRequest(parties = Seq(partyDefault, partyNonDefault), identityProviderId = "")
      )
      getAsNonDefaultIdp <- ledger.getParties(
        GetPartiesRequest(
          parties = Seq(partyDefault, partyNonDefault, partyOtherNonDefault),
          identityProviderId = idpId1,
        )
      )
      // cleanup
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderRequest(
          party = partyNonDefault,
          sourceIdentityProviderId = idpId1,
          targetIdentityProviderId = "",
        )
      )
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderRequest(
          party = partyOtherNonDefault,
          sourceIdentityProviderId = idpId2,
          targetIdentityProviderId = "",
        )
      )
      _ <- ledger.deleteIdentityProviderConfig(DeleteIdentityProviderConfigRequest(idpId1))
    } yield {
      assertEquals(
        "default idp",
        getAsDefaultIdp.partyDetails.map(_.copy(localMetadata = None)).toSet,
        Set(
          PartyDetails(party = partyDefault, isLocal = true, identityProviderId = ""),
          PartyDetails(party = partyNonDefault, isLocal = true, identityProviderId = ""),
        ),
      )
      assertEquals(
        "non default idp",
        getAsNonDefaultIdp.partyDetails.map(_.copy(localMetadata = None)).toSet,
        Set(
          PartyDetails(party = partyDefault, isLocal = false, identityProviderId = ""),
          PartyDetails(party = partyNonDefault, isLocal = true, identityProviderId = idpId1),
          PartyDetails(party = partyOtherNonDefault, isLocal = false, identityProviderId = ""),
        ),
      )
    }
  })

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
      party.getValue.nonEmpty,
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
      party.getValue.nonEmpty,
      "The allocated party identifier is an empty string",
    )
  })

  test(
    "PMAllocateWithLocalMetadataAnnotations",
    "Successfully allocating a party with non-empty annotations",
    enabled = features => features.userAndPartyLocalMetadataExtensions,
    partyAllocation = allocate(NoParties),
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

  test(
    "PMFailToAllocateWhenAnnotationsHaveEmptyValues",
    "Failing to allocate when annotations have empty values",
    enabled = features => features.userAndPartyLocalMetadataExtensions,
    partyAllocation = allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      _ <- ledger
        .allocateParty(
          AllocatePartyRequest(
            localMetadata = Some(
              ObjectMeta(annotations = Map("key1" -> "val1", "key2" -> ""))
            )
          )
        )
        .mustFailWith(
          "allocating a party",
          LedgerApiErrors.RequestValidation.InvalidArgument,
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
      party.getValue.nonEmpty,
      "The allocated party identifier is an empty string",
    )
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
        party.getValue.nonEmpty,
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
      assert(p1.getValue.nonEmpty, "The first allocated party identifier is an empty string")
      assert(p2.getValue.nonEmpty, "The second allocated party identifier is an empty string")
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
        party.getValue.nonEmpty,
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
      val nonUniqueNames = parties.groupBy(_.getValue).view.mapValues(_.size).filter(_._2 > 1).toMap
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
    val useMeta = ledger.features.userAndPartyLocalMetadataExtensions
    for {
      party1 <- ledger.allocateParty(
        partyIdHint = Some("PMGetPartiesDetails_" + Random.alphanumeric.take(10).mkString),
        displayName = Some("Alice"),
        localMetadata = Some(
          ObjectMeta(
            annotations = Map("k1" -> "v1")
          )
        ),
      )
      party2 <- ledger.allocateParty(
        partyIdHint = Some("PMGetPartiesDetails_" + Random.alphanumeric.take(10).mkString),
        displayName = Some("Bob"),
        localMetadata = Some(
          ObjectMeta(
            annotations = Map("k2" -> "v2")
          )
        ),
      )
      partyDetails <- ledger.getParties(
        Seq(party1, party2, new Party("non-existent"))
      )
      noPartyDetails <- ledger.getParties(Seq(new Party("non-existent")))
      zeroPartyDetails <- ledger.getParties(Seq.empty)
    } yield {
      assert(
        partyDetails.sortBy(_.displayName).map(unsetResourceVersion) == Seq(
          PartyDetails(
            party = Ref.Party.assertFromString(party1),
            displayName = "Alice",
            isLocal = true,
            localMetadata =
              if (useMeta) Some(ObjectMeta(annotations = Map("k1" -> "v1"))) else Some(ObjectMeta()),
          ),
          PartyDetails(
            party = Ref.Party.assertFromString(party2),
            displayName = "Bob",
            isLocal = true,
            localMetadata =
              if (useMeta) Some(ObjectMeta(annotations = Map("k2" -> "v2"))) else Some(ObjectMeta()),
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
    val useMeta = ledger.features.userAndPartyLocalMetadataExtensions
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
      knownPartyResp <- ledger.listKnownParties()
      knownPartyIds = knownPartyResp.partyDetails.map(_.party).map(new Party(_)).toSet
    } yield {
      val allocatedPartyIds = Set(party1, party2, party3)
      assert(
        allocatedPartyIds subsetOf knownPartyIds,
        s"The allocated party IDs $allocatedPartyIds are not a subset of $knownPartyIds.",
      )
      val fetchedAllocatedPartiesSet = knownPartyResp.partyDetails.collect {
        case details if allocatedPartyIds.contains(new Party(details.party)) =>
          unsetResourceVersion(details)
      }.toSet
      assertEquals(
        fetchedAllocatedPartiesSet,
        expected = Set(
          PartyDetails(
            party = party1.getValue,
            displayName = "",
            isLocal = true,
            localMetadata =
              if (useMeta) Some(ObjectMeta(annotations = Map("k1" -> "v1"))) else Some(ObjectMeta()),
          ),
          PartyDetails(
            party = party2.getValue,
            displayName = "",
            isLocal = true,
            localMetadata = Some(ObjectMeta(annotations = Map.empty)),
          ),
          PartyDetails(
            party = party3.getValue,
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
      _ <- alpha.create(alice, new Dummy(alice))
      _ <- beta.create(bob, new Dummy(bob))

      alphaParties <- alpha.getParties(Seq(alice, bob))
      betaParties <- beta.getParties(Seq(alice, bob))
    } yield {
      assert(
        alphaParties.exists(p => p.party == alice.getValue && p.isLocal),
        "Missing expected party from first participant",
      )
      assert(
        betaParties.exists(p => p.party == bob.getValue && p.isLocal),
        "Missing expected party from second participant",
      )

      // The following assertions allow some slack to distributed ledger implementations, as they can
      // either publish parties across participants as non-local or bar that from happening entirely.
      // Furthermore, as participants with matching ledger ids expose the "same ledger", such participants
      // are allowed to expose parties as local on multiple participants, and therefore restrict the asserts to
      // participants with different ledger ids.
      if (alpha.endpointId != beta.endpointId && alpha.ledgerId != beta.ledgerId) {
        assert(
          alphaParties.exists(p => p.party == bob.getValue && !p.isLocal) || !alphaParties.exists(
            _.party == bob.getValue
          ),
          "Unexpected remote party marked as local found on first participant",
        )
        assert(
          betaParties.exists(p => p.party == alice.getValue && !p.isLocal) || !betaParties.exists(
            _.party == alice.getValue
          ),
          "Unexpected remote party marked as local found on second participant",
        )
      }
    }
  })

  test(
    "PMPagedListKnownPartiesNewPartyVisibleOnPage",
    "Exercise ListKnownParties rpc: Creating a party makes it visible on a page",
    allocate(NoParties),
    enabled = _.partyManagement.maxPartiesPageSize > 0,
  )(implicit ec => { case Participants(Participant(ledger)) =>
    def assertPartyPresentIn(party: String, list: ListKnownPartiesResponse, msg: String): Unit = {
      assert(list.partyDetails.exists(_.party.startsWith(party)), msg)
    }

    def assertPartyAbsentIn(party: String, list: ListKnownPartiesResponse, msg: String): Unit = {
      assert(!list.partyDetails.exists(_.party.startsWith(party)), msg)
    }

    for {
      pageBeforeCreate <- ledger.listKnownParties(
        ListKnownPartiesRequest(pageToken = "", pageSize = 10)
      )
      // Construct an party-id that with high probability will be the first on the first page
      newPartyId = pageBeforeCreate.partyDetails.headOption
        .flatMap(_.party.split(':').headOption)
        .flatMap(namePicker.lower)
        .getOrElse("@BAD-PARTY@")
      _ = assertPartyAbsentIn(
        newPartyId,
        pageBeforeCreate,
        "new party should be absent before it's creation",
      )
      _ <- ledger.allocateParty(AllocatePartyRequest(newPartyId))
      pageAfterCreate <- ledger.listKnownParties(
        ListKnownPartiesRequest(pageToken = "", pageSize = 10)
      )
      _ = assertPartyPresentIn(
        newPartyId,
        pageAfterCreate,
        "new party should be present after it's creation",
      )
    } yield {
      ()
    }
  })

  test(
    "PMPagedListKnownPartiesNewPartyInvisibleOnNextPage",
    "Exercise ListKnownParties rpc: Adding a party to a previous page doesn't affect the subsequent page",
    allocate(NoParties),
    enabled = _.partyManagement.maxPartiesPageSize > 0,
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val partyId1 = ledger.nextPartyId()
    val partyId2 = ledger.nextPartyId()
    val partyId3 = ledger.nextPartyId()
    val partyId4 = ledger.nextPartyId()

    for {
      // Create 4 parties to ensure we have at least two pages of two parties each
      _ <- ledger.allocateParty(AllocatePartyRequest(partyId1, ""))
      _ <- ledger.allocateParty(AllocatePartyRequest(partyId2, ""))
      _ <- ledger.allocateParty(AllocatePartyRequest(partyId3, ""))
      _ <- ledger.allocateParty(AllocatePartyRequest(partyId4, ""))
      // Fetch the first two full pages
      page1 <- ledger.listKnownParties(ListKnownPartiesRequest(pageToken = "", pageSize = 2))
      page2 <- ledger.listKnownParties(
        ListKnownPartiesRequest(pageToken = page1.nextPageToken, pageSize = 2)
      )
      // Verify that the second page stays the same even after we have created a new party that is lexicographically smaller than the last party on the first page
      newPartyId = (for {
        beforeLast <- page1.partyDetails.dropRight(1).lastOption
        beforeLastName <- beforeLast.party.split(':').headOption
        last <- page1.partyDetails.lastOption
        lastName <- last.party.split(':').headOption
        pick <- namePicker.lowerConstrained(lastName, beforeLastName)
      } yield pick).getOrElse("@BAD-PARTY@")
      _ <- ledger.allocateParty(AllocatePartyRequest(newPartyId))
      page2B <- ledger.listKnownParties(
        ListKnownPartiesRequest(pageToken = page1.nextPageToken, pageSize = 2)
      )
      _ = assertEquals("after creating new party before the second page", page2, page2B)
    } yield {
      ()
    }
  })

  test(
    "PMPagedListKnownPartiesReachingTheLastPage",
    "Exercise ListKnownParties rpc: Listing all parties page by page eventually terminates reaching the last page",
    allocate(NoParties),
    enabled = _.partyManagement.maxPartiesPageSize > 0,
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val pageSize = Math.min(10000, ledger.features.partyManagement.maxPartiesPageSize)

    def fetchNextPage(pageToken: String, pagesFetched: Int): Future[Unit] = {
      for {
        page <- ledger.listKnownParties(
          ListKnownPartiesRequest(pageSize = pageSize, pageToken = pageToken)
        )
        _ = if (page.nextPageToken != "") {
          if (pagesFetched > 10) {
            fail(
              s"Could not reach the last page even after fetching ${pagesFetched + 1} pages of size $pageSize each"
            )
          }
          fetchNextPage(pageToken = page.nextPageToken, pagesFetched = pagesFetched + 1)
        }
      } yield ()
    }

    fetchNextPage(pageToken = "", pagesFetched = 0)
  })

  test(
    "PMPagedListKnownPartiesWithInvalidRequest",
    "Exercise ListKnownParties rpc: Requesting invalid pageSize or pageToken results in an error",
    allocate(NoParties),
    enabled = _.partyManagement.maxPartiesPageSize > 0,
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      // Using not Base64 encoded string as the page token
      onBadTokenError <- ledger
        .listKnownParties(ListKnownPartiesRequest(pageToken = UUID.randomUUID().toString))
        .mustFail("using invalid page token string")
      // Using negative pageSize
      onNegativePageSizeError <- ledger
        .listKnownParties(ListKnownPartiesRequest(pageSize = -100))
        .mustFail("using negative page size")
    } yield {
      assertGrpcError(
        t = onBadTokenError,
        errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
        exceptionMessageSubstring = None,
      )
      assertGrpcError(
        t = onNegativePageSizeError,
        errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
        exceptionMessageSubstring = None,
      )
    }

  })

  test(
    "PMPagedListKnownPartiesZeroPageSize",
    "Exercise ListKnownParties rpc: Requesting page of size zero means requesting server's default page size, which is larger than zero",
    allocate(NoParties),
    enabled = _.partyManagement.maxPartiesPageSize > 0,
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val partyId1 = ledger.nextPartyId()
    val partyId2 = ledger.nextPartyId()
    for {
      // Ensure we have at least two parties
      _ <- ledger.allocateParty(AllocatePartyRequest(partyId1, ""))
      _ <- ledger.allocateParty(AllocatePartyRequest(partyId2, ""))
      pageSizeZero <- ledger.listKnownParties(
        ListKnownPartiesRequest(pageSize = 0)
      )
      pageSizeOne <- ledger.listKnownParties(
        ListKnownPartiesRequest(pageSize = 1)
      )
    } yield {
      assert(
        pageSizeOne.partyDetails.nonEmpty,
        "First page with requested pageSize zero should return some parties",
      )
      assertEquals(pageSizeZero.partyDetails.head, pageSizeOne.partyDetails.head)
    }
  })

  test(
    "PMPagedListKnownPartiesMaxPageSize",
    "Exercise ListKnownParties rpc: Requesting more than maxPartiesPageSize results in an error",
    allocate(NoParties),
    enabled = _.partyManagement.maxPartiesPageSize > 0,
    disabledReason = "requires party management feature with parties page size limit",
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val maxPartiesPageSize = ledger.features.partyManagement.maxPartiesPageSize
    val parties = 1.to(maxPartiesPageSize + 1).map(_ => ledger.nextPartyId())
    for {
      // create lots of parties
      _ <- Future.sequence(parties.map(u => ledger.allocateParty(AllocatePartyRequest(u))))
      // request page size greater than the server's limit
      onTooLargePageSizeError <- ledger
        .listKnownParties(
          ListKnownPartiesRequest(pageSize = maxPartiesPageSize + 1, pageToken = "")
        )
        .mustFail("using too large a page size")
    } yield {
      assertGrpcError(
        t = onTooLargePageSizeError,
        errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
        exceptionMessageSubstring = Some("Page size must not exceed the server's maximum"),
      )
    }
  })
}
