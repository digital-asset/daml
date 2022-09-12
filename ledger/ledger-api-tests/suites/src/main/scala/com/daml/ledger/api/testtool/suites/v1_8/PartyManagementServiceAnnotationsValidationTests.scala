// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.nio.charset.StandardCharsets

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  NoParties,
  Participant,
  Participants,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.{assertEquals, _}
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  PartyDetails,
  UpdatePartyDetailsRequest,
}
import com.google.protobuf.field_mask.FieldMask

trait PartyManagementServiceAnnotationsValidationTests { self: PartyManagementServiceIT =>

  test(
    "TestAnnotationsSizeLimits",
    "Test annotations' size limit",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val largeString = "a" * 256 * 1024
    val notSoLargeString = "a" * (256 * 1024 - 1)
    assertEquals(largeString.getBytes(StandardCharsets.UTF_8).length, 256 * 1024)
    for {
      _ <- ledger
        .allocateParty(
          AllocatePartyRequest(localMetadata =
            Some(ObjectMeta(annotations = Map("a" -> largeString)))
          )
        )
        .mustFailWith(
          "total size of annotations exceeds 256kb max limit",
          errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
          exceptionMessageSubstring = Some(
            "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: annotations from field 'party_details.local_metadata.annotations' are larger than the limit of 256kb, actual size"
          ),
        )
      create1 <- ledger.allocateParty(
        AllocatePartyRequest(localMetadata =
          Some(ObjectMeta(annotations = Map("a" -> notSoLargeString)))
        )
      )
      _ <- ledger
        .updatePartyDetails(
          UpdatePartyDetailsRequest(
            partyDetails = Some(
              PartyDetails(
                party = create1.partyDetails.get.party,
                localMetadata = Some(ObjectMeta(annotations = Map("a" -> largeString))),
              )
            )
          )
        )
        .mustFailWith(
          "total size of annotations, in a user update call, is over 256kb",
          errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
          exceptionMessageSubstring = Some(
            "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: annotations from field 'party_details.local_metadata.annotations' are larger than the limit of 256kb, actual size"
          ),
        )
    } yield ()
  })

  test(
    "TestAnnotationsKeySyntax",
    "Test annotations' key syntax",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val invalidKey = ".party.management.daml/foo_"
    for {
      create1 <- ledger.allocateParty(
        AllocatePartyRequest(localMetadata =
          Some(ObjectMeta(annotations = Map("0-party.management.daml/foo" -> "")))
        )
      )
      _ <- ledger
        .updatePartyDetails(
          UpdatePartyDetailsRequest(
            partyDetails = Some(
              PartyDetails(
                party = create1.partyDetails.get.party,
                localMetadata = Some(ObjectMeta(annotations = Map(invalidKey -> ""))),
              )
            ),
            updateMask = Some(FieldMask(Seq("party_details.local_metadata.annotations"))),
          )
        )
        .mustFailWith(
          "bad annotations key syntax on a user update",
          errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
          exceptionMessageSubstring = Some(
            "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Key prefix segment '.party.management.da...' has invalid syntax"
          ),
        )
      _ <- ledger
        .allocateParty(
          AllocatePartyRequest(localMetadata =
            Some(ObjectMeta(annotations = Map(invalidKey -> "")))
          )
        )
        .mustFailWith(
          "bad annotations key syntax on user creation",
          errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
          exceptionMessageSubstring = Some(
            "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Key prefix segment '.party.management.da...' has invalid syntax"
          ),
        )
    } yield ()
  })

}
