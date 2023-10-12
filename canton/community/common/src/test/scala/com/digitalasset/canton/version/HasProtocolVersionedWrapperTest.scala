// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protobuf.{VersionedMessageV0, VersionedMessageV1, VersionedMessageV2}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.HasProtocolVersionedWrapperTest.{
  Message,
  protocolVersionRepresentative,
}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

class HasProtocolVersionedWrapperTest extends AnyWordSpec with BaseTest {

  /*
      proto               0         1    2
      protocolVersion     3    4    5    6    7  ...
   */
  "HasVersionedWrapperV2" should {
    "use correct proto version depending on the protocol version for serialization" in {
      def message(i: Int): Message = Message("Hey", 1, 2.0)(protocolVersionRepresentative(i), None)
      message(3).toProtoVersioned.version shouldBe 0
      message(4).toProtoVersioned.version shouldBe 0
      message(5).toProtoVersioned.version shouldBe 1
      message(6).toProtoVersioned.version shouldBe 2
      message(7).toProtoVersioned.version shouldBe 2
    }

    "set correct protocol version depending on the proto version" in {
      def fromByteString(bytes: ByteString, protoVersion: Int): Message = Message
        .fromByteString(
          VersionedMessage[Message](bytes, protoVersion).toByteString
        )
        .value

      val messageV0 = VersionedMessageV0("Hey").toByteString
      val expectedV0Deserialization = Message("Hey", 0, 0)(protocolVersionRepresentative(3), None)
      Message
        .fromByteString(ProtoVersion(0))(
          messageV0
        )
        .value shouldBe expectedV0Deserialization

      // Round trip serialization
      Message
        .fromByteString(ProtoVersion(0))(
          expectedV0Deserialization.toByteString
        )
        .value shouldBe expectedV0Deserialization

      val messageV1 = VersionedMessageV1("Hey", 42).toByteString
      val expectedV1Deserialization =
        Message("Hey", 42, 1.0)(protocolVersionRepresentative(5), None)
      fromByteString(messageV1, 1) shouldBe expectedV1Deserialization

      // Round trip serialization
      Message
        .fromByteString(
          expectedV1Deserialization.toByteString
        )
        .value shouldBe expectedV1Deserialization

      val messageV2 = VersionedMessageV2("Hey", 42, 43.0).toByteString
      val expectedV2Deserialization =
        Message("Hey", 42, 43.0)(protocolVersionRepresentative(6), None)
      fromByteString(messageV2, 2) shouldBe expectedV2Deserialization

      // Round trip serialization
      Message
        .fromByteString(
          expectedV2Deserialization.toByteString
        )
        .value shouldBe expectedV2Deserialization
    }

    "return the protocol representative" in {
      protocolVersionRepresentative(3).representative shouldBe ProtocolVersion.v3
      protocolVersionRepresentative(4).representative shouldBe ProtocolVersion.v3
      protocolVersionRepresentative(5).representative shouldBe ProtocolVersion.v5
      protocolVersionRepresentative(6).representative shouldBe ProtocolVersion(6)
      protocolVersionRepresentative(7).representative shouldBe ProtocolVersion(6)
      protocolVersionRepresentative(8).representative shouldBe ProtocolVersion(6)
    }

    "status consistency between protobuf messages and protocol versions" in {
      new HasMemoizedProtocolVersionedWrapperCompanion[Message] {
        import com.digitalasset.canton.version.HasProtocolVersionedWrapperTest.Message.*

        val stablePV = ProtocolVersion.stable(10)
        val unstablePV = ProtocolVersion.unstable(11)

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

        clue("can use a stable proto message in an unstable protocol version") {
          assertCompiles(
            """
             val _ = VersionedProtoConverter(unstablePV)(VersionedMessageV1)(
               supportedProtoVersionMemoized(_)(fromProtoV1),
               _.toProtoV1.toByteString
             )"""
          ): Assertion
        }

        clue("can use an unstable proto message in an unstable protocol version") {
          assertCompiles(
            """
             val _ = VersionedProtoConverter(unstablePV)(VersionedMessageV2)(
               supportedProtoVersionMemoized(_)(fromProtoV2),
               _.toProtoV2.toByteString
             )"""
          ): Assertion
        }

        clue("can not use an unstable proto message in a stable protocol version") {
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
  private def protocolVersionRepresentative(i: Int): RepresentativeProtocolVersion[Message.type] =
    Message.protocolVersionRepresentativeFor(ProtocolVersion(i))

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
      proto               0         1    2
      protocolVersion     3    4    5    6    7  ...
     */
    override val supportedProtoVersions = SupportedProtoVersions(
      ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.unstable(5))(VersionedMessageV1)(
        supportedProtoVersionMemoized(_)(fromProtoV1),
        _.toProtoV1.toByteString,
      ),
      // Can use a stable Protobuf message in a stable protocol version
      ProtoVersion(0) -> LegacyProtoConverter(ProtocolVersion.stable(3))(VersionedMessageV0)(
        supportedProtoVersionMemoized(_)(fromProtoV0),
        _.toProtoV0.toByteString,
      ),
      // Can use an unstable Protobuf message in an unstable protocol version
      ProtoVersion(2) -> VersionedProtoConverter(
        ProtocolVersion.unstable(6)
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
        protocolVersionRepresentativeFor(ProtoVersion(0)),
        Some(bytes),
      ).asRight

    def fromProtoV1(message: VersionedMessageV1)(bytes: ByteString): ParsingResult[Message] =
      Message(
        message.msg,
        message.value,
        1,
      )(
        protocolVersionRepresentativeFor(ProtoVersion(1)),
        Some(bytes),
      ).asRight

    def fromProtoV2(message: VersionedMessageV2)(bytes: ByteString): ParsingResult[Message] =
      Message(
        message.msg,
        message.iValue,
        message.dValue,
      )(
        protocolVersionRepresentativeFor(ProtoVersion(2)),
        Some(bytes),
      ).asRight
  }
}
