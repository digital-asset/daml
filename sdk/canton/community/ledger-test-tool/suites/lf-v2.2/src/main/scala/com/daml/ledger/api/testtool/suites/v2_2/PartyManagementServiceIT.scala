// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.{NamePicker, Party}
import com.daml.ledger.api.v2.admin.identity_provider_config_service.DeleteIdentityProviderConfigRequest
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocateExternalPartyRequest,
  AllocatePartyRequest,
  AllocatePartyResponse,
  GenerateExternalPartyTopologyRequest,
  GetPartiesRequest,
  GetPartiesResponse,
  ListKnownPartiesRequest,
  ListKnownPartiesResponse,
  PartyDetails,
  UpdatePartyIdentityProviderIdRequest,
}
import com.daml.ledger.api.v2.crypto as lapicrypto
import com.daml.ledger.javaapi.data.Party as ApiParty
import com.daml.ledger.test.java.model.test.Dummy
import com.digitalasset.canton.ledger.error.groups.{AdminServiceErrors, RequestValidationErrors}
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString

import java.security.{KeyPairGenerator, Signature}
import java.util.UUID
import java.util.regex.Pattern
import scala.concurrent.Future
import scala.util.Random

final class PartyManagementServiceIT extends PartyManagementITBase {
  import CompanionImplicits.*

  val namePicker: NamePicker = NamePicker(
    "-_ 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  )

  test(
    "PMUpdatingPartyIdentityProviderNonDefaultIdps",
    "Test reassigning party to a different idp using non default idps",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
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
        UpdatePartyIdentityProviderIdRequest(
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
        UpdatePartyIdentityProviderIdRequest(
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
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val idpId1 = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId1)
      // allocate a party in the default idp
      party <- ledger.allocateParty(identityProviderId = None)
      get1 <- ledger.getParties(GetPartiesRequest(parties = Seq(party), identityProviderId = ""))
      // Update party's idp id
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderIdRequest(
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
        UpdatePartyIdentityProviderIdRequest(
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
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val idpIdNonExistent = ledger.nextIdentityProviderId()
    for {
      party <- ledger.allocateParty(identityProviderId = None)
      _ <- ledger
        .updatePartyIdentityProviderId(
          UpdatePartyIdentityProviderIdRequest(
            party = party,
            sourceIdentityProviderId = idpIdNonExistent,
            targetIdentityProviderId = "",
          )
        )
        .mustFailWith(
          "non existent source idp",
          RequestValidationErrors.InvalidArgument,
        )
      _ <- ledger
        .updatePartyIdentityProviderId(
          UpdatePartyIdentityProviderIdRequest(
            party = party,
            sourceIdentityProviderId = "",
            targetIdentityProviderId = idpIdNonExistent,
          )
        )
        .mustFailWith(
          "non existent target idp",
          RequestValidationErrors.InvalidArgument,
        )
    } yield ()
  })

  test(
    "PMUpdatingPartyIdentityProviderMismatchedSourceIdp",
    "Test reassigning party to a different idp using mismatched source idp id",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val idpIdNonDefault = ledger.nextIdentityProviderId()
    val idpIdTarget = ledger.nextIdentityProviderId()
    val idpIdMismatched = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpIdNonDefault)
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpIdMismatched)
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpIdTarget)
      partyDefault <- ledger.allocateParty(identityProviderId = None)
      partyNonDefault <- ledger.allocateParty(identityProviderId = Some(idpIdNonDefault))
      _ <- ledger
        .updatePartyIdentityProviderId(
          UpdatePartyIdentityProviderIdRequest(
            party = partyDefault,
            sourceIdentityProviderId = idpIdMismatched,
            targetIdentityProviderId = idpIdTarget,
          )
        )
        .mustFailWith(
          "mismatched source idp id",
          AdminServiceErrors.PartyManagement.PartyNotFound,
        )
      _ <- ledger
        .updatePartyIdentityProviderId(
          UpdatePartyIdentityProviderIdRequest(
            party = partyNonDefault,
            sourceIdentityProviderId = idpIdMismatched,
            targetIdentityProviderId = idpIdTarget,
          )
        )
        .mustFailWith(
          "mismatched source idp id",
          AdminServiceErrors.PartyManagement.PartyNotFound,
        )
      // cleanup
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderIdRequest(
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
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val idpId1 = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId1)
      partyDefault <- ledger.allocateParty(identityProviderId = None)
      partyNonDefault <- ledger.allocateParty(identityProviderId = Some(idpId1))
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderIdRequest(
          party = partyDefault,
          sourceIdentityProviderId = "",
          targetIdentityProviderId = "",
        )
      )
      get1 <- ledger.getParties(
        GetPartiesRequest(parties = Seq(partyDefault), identityProviderId = "")
      )
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderIdRequest(
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
        UpdatePartyIdentityProviderIdRequest(
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
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
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
        UpdatePartyIdentityProviderIdRequest(
          party = partyNonDefault,
          sourceIdentityProviderId = idpId1,
          targetIdentityProviderId = "",
        )
      )
      _ <- ledger.updatePartyIdentityProviderId(
        UpdatePartyIdentityProviderIdRequest(
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
          PartyDetails(
            party = partyDefault,
            isLocal = true,
            identityProviderId = "",
            localMetadata = None,
          ),
          PartyDetails(
            party = partyNonDefault,
            isLocal = true,
            identityProviderId = "",
            localMetadata = None,
          ),
        ),
      )
      assertEquals(
        "non default idp",
        getAsNonDefaultIdp.partyDetails.map(_.copy(localMetadata = None)).toSet,
        Set(
          PartyDetails(
            party = partyDefault,
            isLocal = false,
            identityProviderId = "",
            localMetadata = None,
          ),
          PartyDetails(
            party = partyNonDefault,
            isLocal = true,
            identityProviderId = idpId1,
            localMetadata = None,
          ),
          PartyDetails(
            party = partyOtherNonDefault,
            isLocal = false,
            identityProviderId = "",
            localMetadata = None,
          ),
        ),
      )
    }
  })

  test(
    "PMNonEmptyParticipantID",
    "Asking for the participant identifier should return a non-empty string",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      participantId <- ledger.getParticipantId()
    } yield {
      assert(participantId.nonEmpty, "The ledger returned an empty participant identifier")
    }
  })

  private val pMAllocateWithHint = "PMAllocateWithHint"
  test(
    pMAllocateWithHint,
    "It should be possible to provide a hint when allocating a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      party <- ledger.allocateParty(
        partyIdHint = Some(pMAllocateWithHint + "_" + Random.alphanumeric.take(10).mkString)
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
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      party <- ledger.allocateParty(partyIdHint = None)
    } yield assert(
      party.getValue.nonEmpty,
      "The allocated party identifier is an empty string",
    )
  })

  test(
    "PMAllocateWithLocalMetadataAnnotations",
    "Successfully allocating a party with non-empty annotations",
    partyAllocation = allocate(NoParties),
  )(implicit ec => { case p @ Participants(Participant(ledger, Seq())) =>
    for {
      (allocate1, _) <- ledger.allocateParty(
        AllocatePartyRequest(
          partyIdHint = "",
          localMetadata = Some(
            ObjectMeta(resourceVersion = "", annotations = Map("key1" -> "val1", "key2" -> "val2"))
          ),
          identityProviderId = "",
          synchronizerId = "",
          userId = "",
        ),
        p.minSynchronizers,
      )
      allocatedParty = allocate1.partyDetails.get.party
      expectedPartyDetails = PartyDetails(
        party = allocatedParty,
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
        identityProviderId = "",
      )
      _ = assertEquals(
        allocate1,
        expected = AllocatePartyResponse(
          partyDetails = Some(
            expectedPartyDetails
          )
        ),
      )
      get1 <- ledger.getParties(GetPartiesRequest(Seq(allocatedParty), ""))
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
    partyAllocation = allocate(NoParties),
  )(implicit ec => { case p @ Participants(Participant(ledger, Seq())) =>
    for {
      _ <- ledger
        .allocateParty(
          AllocatePartyRequest(
            partyIdHint = "",
            localMetadata = Some(
              ObjectMeta(
                resourceVersion = "",
                annotations = Map("key1" -> "val1", "key2" -> ""),
              )
            ),
            identityProviderId = "",
            synchronizerId = "",
            userId = "",
          ),
          p.minSynchronizers,
        )
        .mustFailWith(
          "allocating a party",
          RequestValidationErrors.InvalidArgument,
        )
    } yield ()
  })

  test(
    "PMRejectionDuplicateHint",
    "A party allocation request with a duplicate party hint should be rejected",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val hint = "party_hint" + "_" + Random.alphanumeric.take(10).mkString
    for {
      party <- ledger.allocateParty(partyIdHint = Some(hint))
      error <- ledger
        .allocateParty(partyIdHint = Some(hint))
        .mustFail("allocating a party with a duplicate hint")
    } yield {
      assert(
        party.getValue.nonEmpty,
        "The allocated party identifier is an empty string",
      )
      assertGrpcErrorRegex(
        error,
        RequestValidationErrors.InvalidArgument,
        Some(Pattern.compile("Party already exists|PartyToParticipant")),
      )
    }
  })

  test(
    "PMRejectLongPartyHints",
    "A party identifier which is too long should be rejected with the proper error",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      error <- ledger
        .allocateParty(
          partyIdHint = Some(Random.alphanumeric.take(256).mkString)
        )
        .mustFail("allocating a party with a very long identifier")
    } yield {
      assertGrpcError(
        error,
        RequestValidationErrors.InvalidArgument,
        Some("Party is too long"),
      )
    }
  })

  test(
    "PMRejectInvalidPartyHints",
    "A party identifier that contains invalid characters should be rejected with the proper error",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      error <- ledger
        .allocateParty(
          // Assumption: emojis will never be acceptable in a party identifier
          partyIdHint = Some("\uD83D\uDE00")
        )
        .mustFail("allocating a party with invalid characters")
    } yield {
      assertGrpcError(
        error,
        RequestValidationErrors.InvalidArgument,
        Some("non expected character"),
      )
    }
  })

  test(
    "PMAllocateOneHundred",
    "It should create unique party names when allocating many parties",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      parties <- ledger.allocateParties(n = 100, minSynchronizers = 0)
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
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      party1 <- ledger.allocateParty(
        partyIdHint = Some("PMGetPartiesDetails_" + Random.alphanumeric.take(10).mkString),
        localMetadata = Some(
          ObjectMeta(
            resourceVersion = "",
            annotations = Map("k1" -> "v1"),
          )
        ),
      )
      party2 <- ledger.allocateParty(
        partyIdHint = Some("PMGetPartiesDetails_" + Random.alphanumeric.take(10).mkString),
        localMetadata = Some(
          ObjectMeta(
            resourceVersion = "",
            annotations = Map("k2" -> "v2"),
          )
        ),
      )
      partyDetails <- ledger.getParties(
        Seq(party1, party2, Party("non-existent"))
      )
      noPartyDetails <- ledger.getParties(Seq(Party("non-existent")))
      zeroPartyDetails <- ledger.getParties(Seq.empty)
    } yield {
      val got = partyDetails.map(unsetResourceVersion).sortBy(_.party)
      val want = Seq(
        PartyDetails(
          party = Ref.Party.assertFromString(party1),
          isLocal = true,
          localMetadata = Some(
            ObjectMeta(
              resourceVersion = "",
              annotations = Map("k1" -> "v1"),
            )
          ),
          identityProviderId = "",
        ),
        PartyDetails(
          party = Ref.Party.assertFromString(party2),
          isLocal = true,
          localMetadata = Some(
            ObjectMeta(
              resourceVersion = "",
              annotations = Map("k2" -> "v2"),
            )
          ),
          identityProviderId = "",
        ),
      ).sortBy(_.party)
      assert(
        got == want,
        s"The allocated parties, ${Seq(party1, party2)}, were not retrieved successfully.\nGot\n$got\n\nwant\n$want",
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
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      party1 <- ledger
        .allocateParty(
          partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString),
          localMetadata = Some(
            ObjectMeta(
              resourceVersion = "",
              annotations = Map("k1" -> "v1"),
            )
          ),
        )
        .map(_.underlying)
      party2 <- ledger
        .allocateParty(
          partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString)
        )
        .map(_.underlying)
      party3 <- ledger
        .allocateParty(
          partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString)
        )
        .map(_.underlying)
      knownPartyResp <- ledger.listKnownParties()
      knownPartyIds = knownPartyResp.partyDetails.map(_.party).map(new ApiParty(_)).toSet
    } yield {
      val allocatedPartyIds = Set(party1, party2, party3)
      assert(
        allocatedPartyIds subsetOf knownPartyIds,
        s"The allocated party IDs $allocatedPartyIds are not a subset of $knownPartyIds.",
      )
      val fetchedAllocatedPartiesSet = knownPartyResp.partyDetails.collect {
        case details if allocatedPartyIds.contains(Party(details.party)) =>
          unsetResourceVersion(details)
      }.toSet
      assertEquals(
        fetchedAllocatedPartiesSet,
        expected = Set(
          PartyDetails(
            party = party1.getValue,
            isLocal = true,
            localMetadata = Some(
              ObjectMeta(
                resourceVersion = "",
                annotations = Map("k1" -> "v1"),
              )
            ),
            identityProviderId = "",
          ),
          PartyDetails(
            party = party2.getValue,
            isLocal = true,
            localMetadata = Some(
              ObjectMeta(
                resourceVersion = "",
                annotations = Map.empty,
              )
            ),
            identityProviderId = "",
          ),
          PartyDetails(
            party = party3.getValue,
            isLocal = true,
            localMetadata = Some(
              ObjectMeta(
                resourceVersion = "",
                annotations = Map.empty,
              )
            ),
            identityProviderId = "",
          ),
        ),
      )
    }
  })

  test(
    "PMGetPartiesBoundaryConditions",
    "GetParties should correctly report in boundary conditions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, Seq(alice))) =>
    val nonExisting = alpha.nextPartyId()
    for {
      // Running a dummy transaction seems to be required by the test framework to make parties visible via getParties
      _ <- alpha.create(alice, new Dummy(alice))

      alphaParties <- alpha.getParties(GetPartiesRequest(Seq(alice), ""))
      alphaParties2 <- alpha.getParties(GetPartiesRequest(Seq(alice, nonExisting), ""))
      _ <- alpha.getParties(GetPartiesRequest(Seq(nonExisting), ""))
      _ <- alpha.getParties(GetPartiesRequest(Seq.empty, ""))
    } yield {
      assert(
        alphaParties.partyDetails.exists(p => p.party == alice.getValue && p.isLocal),
        "Missing expected party from the participant",
      )
      assert(
        alphaParties2.partyDetails.sizeIs == 1,
        "Non existing party found in getParties response",
      )
      assert(
        alphaParties2.partyDetails.exists(p => p.party == alice.getValue && p.isLocal),
        "Missing expected party from the participant",
      )
    }
  })

  test(
    "PMGetPartiesIsLocal",
    "GetParties should correctly report whether parties are local or not",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(alice)), Participant(beta, Seq(bob))) =>
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
        if (alpha.endpointId != beta.endpointId) {
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
  )(implicit ec => { case p @ Participants(Participant(ledger, Seq())) =>
    def assertPartyPresentIn(party: String, list: ListKnownPartiesResponse, msg: String): Unit =
      assert(list.partyDetails.exists(_.party.startsWith(party)), msg)

    def assertPartyAbsentIn(party: String, list: ListKnownPartiesResponse, msg: String): Unit =
      assert(!list.partyDetails.exists(_.party.startsWith(party)), msg)

    for {
      pageBeforeCreate <- ledger.listKnownParties(
        ListKnownPartiesRequest(
          pageToken = "",
          pageSize = 10,
          identityProviderId = "",
        )
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
      _ <- ledger.allocateParty(
        AllocatePartyRequest(newPartyId, None, "", "", ""),
        p.minSynchronizers,
      )
      pageAfterCreate <- ledger.listKnownParties(
        ListKnownPartiesRequest(
          pageToken = "",
          pageSize = 10,
          identityProviderId = "",
        )
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
  )(implicit ec => { case p @ Participants(Participant(ledger, Seq())) =>
    val partyId1 = ledger.nextPartyId()
    val partyId2 = ledger.nextPartyId()
    val partyId3 = ledger.nextPartyId()
    val partyId4 = ledger.nextPartyId()

    for {
      // Create 4 parties to ensure we have at least two pages of two parties each
      _ <- ledger.allocateParty(
        AllocatePartyRequest(partyId1, None, "", "", ""),
        p.minSynchronizers,
      )
      _ <- ledger.allocateParty(
        AllocatePartyRequest(partyId2, None, "", "", ""),
        p.minSynchronizers,
      )
      _ <- ledger.allocateParty(
        AllocatePartyRequest(partyId3, None, "", "", ""),
        p.minSynchronizers,
      )
      _ <- ledger.allocateParty(
        AllocatePartyRequest(partyId4, None, "", "", ""),
        p.minSynchronizers,
      )
      // Fetch the first two full pages
      page1 <- ledger.listKnownParties(
        ListKnownPartiesRequest(pageToken = "", pageSize = 2, identityProviderId = "")
      )
      page2 <- ledger.listKnownParties(
        ListKnownPartiesRequest(
          pageToken = page1.nextPageToken,
          pageSize = 2,
          identityProviderId = "",
        )
      )
      // Verify that the second page stays the same even after we have created a new party that is lexicographically smaller than the last party on the first page
      newPartyId = (for {
        beforeLast <- page1.partyDetails.dropRight(1).lastOption
        beforeLastName <- beforeLast.party.split(':').headOption
        last <- page1.partyDetails.lastOption
        lastName <- last.party.split(':').headOption
        pick <- namePicker.lowerConstrained(lastName, beforeLastName)
      } yield pick).getOrElse("@BAD-PARTY@")
      _ <- ledger.allocateParty(
        AllocatePartyRequest(newPartyId, None, "", "", ""),
        p.minSynchronizers,
      )
      page2B <- ledger.listKnownParties(
        ListKnownPartiesRequest(
          pageToken = page1.nextPageToken,
          pageSize = 2,
          identityProviderId = "",
        )
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
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val pageSize = Math.min(10000, ledger.features.partyManagement.maxPartiesPageSize)

    def fetchNextPage(pageToken: String, pagesFetched: Int): Future[Unit] =
      for {
        page <- ledger.listKnownParties(
          ListKnownPartiesRequest(
            pageSize = pageSize,
            pageToken = pageToken,
            identityProviderId = "",
          )
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

    fetchNextPage(pageToken = "", pagesFetched = 0)
  })

  test(
    "PMPagedListKnownPartiesWithInvalidRequest",
    "Exercise ListKnownParties rpc: Requesting invalid pageSize or pageToken results in an error",
    allocate(NoParties),
    enabled = _.partyManagement.maxPartiesPageSize > 0,
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      // Using not Base64 encoded string as the page token
      onBadTokenError <- ledger
        .listKnownParties(
          ListKnownPartiesRequest(
            pageToken = UUID.randomUUID().toString,
            pageSize = 0,
            identityProviderId = "",
          )
        )
        .mustFail("using invalid page token string")
      // Using negative pageSize
      onNegativePageSizeError <- ledger
        .listKnownParties(
          ListKnownPartiesRequest(pageToken = "", pageSize = -100, identityProviderId = "")
        )
        .mustFail("using negative page size")
    } yield {
      assertGrpcError(
        t = onBadTokenError,
        errorCode = RequestValidationErrors.InvalidArgument,
        exceptionMessageSubstring = None,
      )
      assertGrpcError(
        t = onNegativePageSizeError,
        errorCode = RequestValidationErrors.InvalidArgument,
        exceptionMessageSubstring = None,
      )
    }

  })

  test(
    "PMPagedListKnownPartiesZeroPageSize",
    "Exercise ListKnownParties rpc: Requesting page of size zero means requesting server's default page size, which is larger than zero",
    allocate(NoParties),
    enabled = _.partyManagement.maxPartiesPageSize > 0,
  )(implicit ec => { case p @ Participants(Participant(ledger, Seq())) =>
    val partyId1 = ledger.nextPartyId()
    val partyId2 = ledger.nextPartyId()
    for {
      // Ensure we have at least two parties
      _ <- ledger.allocateParty(
        AllocatePartyRequest(partyId1, None, "", "", ""),
        p.minSynchronizers,
      )
      _ <- ledger.allocateParty(
        AllocatePartyRequest(partyId2, None, "", "", ""),
        p.minSynchronizers,
      )
      pageSizeZero <- ledger.listKnownParties(
        ListKnownPartiesRequest(pageToken = "", pageSize = 0, identityProviderId = "")
      )
      pageSizeOne <- ledger.listKnownParties(
        ListKnownPartiesRequest(pageToken = "", pageSize = 1, identityProviderId = "")
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
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val maxPartiesPageSize = ledger.features.partyManagement.maxPartiesPageSize
    for {
      // request page size greater than the server's limit
      onTooLargePageSizeError <- ledger
        .listKnownParties(
          ListKnownPartiesRequest(
            pageSize = maxPartiesPageSize + 1,
            pageToken = "",
            identityProviderId = "",
          )
        )
        .mustFail("using too large a page size")
    } yield {
      assertGrpcError(
        t = onTooLargePageSizeError,
        errorCode = RequestValidationErrors.InvalidArgument,
        exceptionMessageSubstring = Some("Page size must not exceed the server's maximum"),
      )
    }
  })

  test(
    "PMGenerateExternalPartyTopologyTransaction",
    "Generate topology transactions for external parties",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val partyHint = ledger.nextPartyId()
    val keyGen = KeyPairGenerator.getInstance("Ed25519")
    val keyPair = keyGen.generateKeyPair()
    val pb = keyPair.getPublic
    val signing = Signature.getInstance("Ed25519")
    signing.initSign(keyPair.getPrivate)

    for {
      syncIds <- ledger.getConnectedSynchronizers(None, None)
      syncId = syncIds.headOption.getOrElse(throw new Exception("No synchronizer connected"))
      response <- ledger.generateExternalPartyTopology(
        GenerateExternalPartyTopologyRequest(
          synchronizer = syncId,
          partyHint = partyHint,
          publicKey = Some(
            lapicrypto.SigningPublicKey(
              format =
                lapicrypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO,
              keyData = ByteString.copyFrom(pb.getEncoded),
              keySpec = lapicrypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519,
            )
          ),
          localParticipantObservationOnly = false,
          otherConfirmingParticipantUids = Seq(),
          confirmationThreshold = 1,
          observingParticipantUids = Seq(),
        )
      )
      _ = {
        signing.update(response.multiHash.toByteArray)
      }
      _ <- ledger.allocateExternalParty(
        AllocateExternalPartyRequest(
          synchronizer = syncId,
          onboardingTransactions = response.topologyTransactions.map(x =>
            AllocateExternalPartyRequest
              .SignedTransaction(transaction = x, signatures = Seq.empty)
          ),
          multiHashSignatures = Seq(
            lapicrypto.Signature(
              format = lapicrypto.SignatureFormat.SIGNATURE_FORMAT_RAW,
              signature = ByteString.copyFrom(signing.sign()),
              signedBy = response.publicKeyFingerprint,
              signingAlgorithmSpec = lapicrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519,
            )
          ),
          identityProviderId = "",
        ),
        minSynchronizers = Some(1),
      )
      parties <- ledger.getParties(
        GetPartiesRequest(
          parties = Seq(response.partyId),
          identityProviderId = "",
        )
      )
    } yield {
      assertEquals(parties.partyDetails.map(_.party), Seq(response.partyId))
    }
  })

}
