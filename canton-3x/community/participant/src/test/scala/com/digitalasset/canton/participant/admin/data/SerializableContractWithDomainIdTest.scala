// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.digitalasset.canton.topology.DomainId
import com.google.protobuf.ByteString
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.Base64

// TODO(i14441): Remove deprecated ACS download / upload functionality
@deprecated("Use ActiveContract", since = "2.8.0")
final class SerializableContractWithDomainIdTest
    extends AnyWordSpec
    with Matchers
    with EitherValues {

  "ACS.loadFromByteString" should {

    "fail on an identifier with a missing namespace with the expected error message" in {
      val improperDomainId = "acme"
      SerializableContractWithDomainId
        .loadFromByteString(
          ByteString.copyFromUtf8(s"$improperDomainId:::contract-id"),
          gzip = false,
        )
        .left
        .value shouldBe s"Invalid unique identifier `$improperDomainId` with missing namespace."
    }

    "fail on a invalid contract ID with the expected error message" in {
      val domainId = DomainId.tryFromString(s"acme::${"0" * 68}").filterString
      val encodedContractId =
        Base64.getEncoder.encodeToString(ByteString.copyFromUtf8("some-contract-id").toByteArray)
      SerializableContractWithDomainId
        .loadFromByteString(
          ByteString.copyFromUtf8(s"$domainId:::$encodedContractId"),
          gzip = false,
        )
        .left
        .value shouldBe
        "Failed parsing disclosed contract: BufferException(com.google.protobuf.InvalidProtocolBufferException$InvalidWireTypeException: Protocol message tag had invalid wire type.)"
    }

  }

}
