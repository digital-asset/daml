// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.UnknownProtoVersion
import com.digitalasset.canton.protobuf.{VersionedMessageV0, VersionedMessageV1, VersionedMessageV2}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.unused

class HasProtocolVersionedWrapperTest extends AnyWordSpec with BaseTest {

  import HasProtocolVersionedWrapperTest.*

  /*
     Supposing that basePV is 30, we get the scheme

      proto               0           1     2
      protocolVersion     30    31    32    33    34  ...
   */
  "HasVersionedWrapperV2" should {
    "use correct proto version depending on the protocol version for serialization" in {
      def message(pv: ProtocolVersion): Message =
        Message("Hey", 1, 2.0)(protocolVersionRepresentative(pv), None)
      message(basePV).toProtoVersioned.version shouldBe 0
      message(basePV + 1).toProtoVersioned.version shouldBe 0
      message(basePV + 2).toProtoVersioned.version shouldBe 1
      message(basePV + 3).toProtoVersioned.version shouldBe 2
      message(basePV + 4).toProtoVersioned.version shouldBe 2
    }

    def fromByteString(
        bytes: ByteString,
        protoVersion: Int,
        expectedProtocolVersion: ProtocolVersion,
    ): ParsingResult[Message] = Message
      .fromByteString(expectedProtocolVersion)(
        VersionedMessage[Message](bytes, protoVersion).toByteString
      )

    "set correct protocol version depending on the proto version" in {

      val messageV1 = VersionedMessageV1("Hey", 42).toByteString
      val expectedV1Deserialization =
        Message("Hey", 42, 1.0)(protocolVersionRepresentative(basePV + 2), None)
      fromByteString(messageV1, 1, basePV + 2).value shouldBe expectedV1Deserialization

      // Round trip serialization
      Message
        .fromByteString(basePV + 2)(
          expectedV1Deserialization.toByteString
        )
        .value shouldBe expectedV1Deserialization

      val messageV2 = VersionedMessageV2("Hey", 42, 43.0).toByteString
      val expectedV2Deserialization =
        Message("Hey", 42, 43.0)(protocolVersionRepresentative(basePV + 3), None)
      fromByteString(messageV2, 2, basePV + 3).value shouldBe expectedV2Deserialization

      // Round trip serialization
      Message
        .fromByteString(basePV + 3)(
          expectedV2Deserialization.toByteString
        )
        .value shouldBe expectedV2Deserialization
    }

    "return the protocol representative" in {
      protocolVersionRepresentative(basePV + 0).representative shouldBe basePV
      protocolVersionRepresentative(basePV + 1).representative shouldBe basePV
      protocolVersionRepresentative(basePV + 2).representative shouldBe basePV + 2
      protocolVersionRepresentative(basePV + 3).representative shouldBe basePV + 3
      protocolVersionRepresentative(basePV + 4).representative shouldBe basePV + 3
      protocolVersionRepresentative(basePV + 5).representative shouldBe basePV + 3
    }

    "return the highest inclusive protocol representative for an unknown protocol version" in {
      protocolVersionRepresentative(ProtocolVersion(-1)).representative shouldBe basePV + 3
    }

    "fail for an unknown proto version" in {
      val maxProtoVersion = Message.supportedProtoVersions.table.keys.max.v
      val unknownProtoVersion = ProtoVersion(maxProtoVersion + 1)

      Message
        .protocolVersionRepresentativeFor(unknownProtoVersion)
        .left
        .value shouldBe UnknownProtoVersion(unknownProtoVersion, Message.name)
    }

    "fail deserialization when the representative protocol version from the proto version does not match the expected (representative) protocol version" in {
      val message = VersionedMessageV1("Hey", 42).toByteString
      fromByteString(message, 1, basePV + 3).left.value should have message
        Message.unexpectedProtoVersionError(basePV + 3, basePV + 2).message
    }

    "validate proto version against expected (representative) protocol version" in {
      Message
        .validateDeserialization(ProtocolVersionValidation(basePV + 2), basePV + 2)
        .value shouldBe ()
      Message
        .validateDeserialization(ProtocolVersionValidation(basePV + 3), basePV + 2)
        .left
        .value should have message Message
        .unexpectedProtoVersionError(basePV + 3, basePV + 2)
        .message
      Message
        .validateDeserialization(
          ProtocolVersionValidation.NoValidation,
          basePV,
        )
        .value shouldBe ()
    }

    "status consistency between protobuf messages and protocol versions" in {
      new HasMemoizedProtocolVersionedWrapperCompanion[Message] {

        import com.digitalasset.canton.version.HasProtocolVersionedWrapperTest.Message.*

        // Used by the compiled string below
        @unused
        val stablePV = ProtocolVersion.createStable(10)
        @unused
        val alphaPV = ProtocolVersion.createAlpha(11)

        def name: String = "message"

        override def supportedProtoVersions: SupportedProtoVersions = ???

        clue("can use a stable proto message in a stable protocol version") {
          assertCompiles(
            """
             val _ = VersionedProtoConverter(stablePV)(VersionedMessageV1)(
               supportedProtoVersionMemoized(_)(fromProtoV1),
               _.toProtoV1.toByteString
             )"""
          ): Assertion
        }

        clue("can use a stable proto message in an alpha protocol version") {
          assertCompiles(
            """
             val _ = VersionedProtoConverter(alphaPV)(VersionedMessageV1)(
               supportedProtoVersionMemoized(_)(fromProtoV1),
               _.toProtoV1.toByteString
             )"""
          ): Assertion
        }

        clue("can use an alpha proto message in an alpha protocol version") {
          assertCompiles(
            """
             val _ = VersionedProtoConverter(alphaPV)(VersionedMessageV2)(
               supportedProtoVersionMemoized(_)(fromProtoV2),
               _.toProtoV2.toByteString
             )"""
          ): Assertion
        }

        clue("can not use an alpha proto message in a stable protocol version") {
          assertTypeError(
            """
             val _ = VersionedProtoConverter(stablePV)(VersionedMessageV2)(
               supportedProtoVersionMemoized(_)(fromProtoV2),
               _.toProtoV2.toByteString
             )"""
          ): Assertion
        }
      }
    }
  }
}

object HasProtocolVersionedWrapperTest {
  import org.scalatest.EitherValues.*

  private val basePV = ProtocolVersion.minimum

  implicit class RichProtocolVersion(val pv: ProtocolVersion) {
    def +(i: Int): ProtocolVersion = ProtocolVersion(pv.v + i)
  }

  private def protocolVersionRepresentative(
      pv: ProtocolVersion
  ): RepresentativeProtocolVersion[Message.type] =
    Message.protocolVersionRepresentativeFor(pv)

  final case class Message(
      msg: String,
      iValue: Int,
      dValue: Double,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[Message.type],
      val deserializedFrom: Option[ByteString] = None,
  ) extends HasProtocolVersionedWrapper[Message] {

    @transient override protected lazy val companionObj: Message.type = Message

    def toProtoV0 = VersionedMessageV0(msg)
    def toProtoV1 = VersionedMessageV1(msg, iValue)
    def toProtoV2 = VersionedMessageV2(msg, iValue, dValue)
  }

  object Message extends HasMemoizedProtocolVersionedWrapperCompanion[Message] {
    def name: String = "Message"

    /*
       Supposing that basePV is 30, we get the scheme

        proto               0           1     2
        protocolVersion     30    31    32    33    34  ...
     */
    override val supportedProtoVersions = SupportedProtoVersions(
      ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.createAlpha((basePV + 2).v))(
        VersionedMessageV1
      )(
        supportedProtoVersionMemoized(_)(fromProtoV1),
        _.toProtoV1.toByteString,
      ),
      // Can use a stable Protobuf message in a stable protocol version
      ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.createStable(basePV.v))(
        VersionedMessageV0
      )(
        supportedProtoVersionMemoized(_)(fromProtoV0),
        _.toProtoV0.toByteString,
      ),
      // Can use an alpha Protobuf message in an alpha protocol version
      ProtoVersion(2) -> VersionedProtoConverter(
        ProtocolVersion.createAlpha((basePV + 3).v)
      )(VersionedMessageV2)(
        supportedProtoVersionMemoized(_)(fromProtoV2),
        _.toProtoV2.toByteString,
      ),
    )

    def fromProtoV0(message: VersionedMessageV0)(bytes: ByteString): ParsingResult[Message] =
      Message(
        message.msg,
        0,
        0,
      )(
        protocolVersionRepresentativeFor(ProtoVersion(0)).value,
        Some(bytes),
      ).asRight

    def fromProtoV1(message: VersionedMessageV1)(bytes: ByteString): ParsingResult[Message] =
      Message(
        message.msg,
        message.value,
        1,
      )(
        protocolVersionRepresentativeFor(ProtoVersion(1)).value,
        Some(bytes),
      ).asRight

    def fromProtoV2(message: VersionedMessageV2)(bytes: ByteString): ParsingResult[Message] =
      Message(
        message.msg,
        message.iValue,
        message.dValue,
      )(
        protocolVersionRepresentativeFor(ProtoVersion(2)).value,
        Some(bytes),
      ).asRight
  }
}
