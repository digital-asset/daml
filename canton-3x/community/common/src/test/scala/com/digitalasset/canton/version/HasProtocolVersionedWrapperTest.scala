// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protobuf.{VersionedMessageV0, VersionedMessageV1, VersionedMessageV2}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

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

    "set correct protocol version depending on the proto version" in {
      def fromByteString(bytes: ByteString, protoVersion: Int): Message = Message
        .fromByteString(
          VersionedMessage[Message](bytes, protoVersion).toByteString
        )
        .value

      val messageV1 = VersionedMessageV1("Hey", 42).toByteString
      val expectedV1Deserialization =
        Message("Hey", 42, 1.0)(protocolVersionRepresentative(basePV + 2), None)
      fromByteString(messageV1, 1) shouldBe expectedV1Deserialization

      // Round trip serialization
      Message
        .fromByteString(
          expectedV1Deserialization.toByteString
        )
        .value shouldBe expectedV1Deserialization

      val messageV2 = VersionedMessageV2("Hey", 42, 43.0).toByteString
      val expectedV2Deserialization =
        Message("Hey", 42, 43.0)(protocolVersionRepresentative(basePV + 3), None)
      fromByteString(messageV2, 2) shouldBe expectedV2Deserialization

      // Round trip serialization
      Message
        .fromByteString(
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

    "status consistency between protobuf messages and protocol versions" in {
      new HasMemoizedProtocolVersionedWrapperCompanion[Message] {
        import com.digitalasset.canton.version.HasProtocolVersionedWrapperTest.Message.*

        // Used by the compiled string below
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
      ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.unstable((basePV + 2).v))(
        VersionedMessageV1
      )(
        supportedProtoVersionMemoized(_)(fromProtoV1),
        _.toProtoV1.toByteString,
      ),
      // Can use a stable Protobuf message in a stable protocol version
      ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.stable(basePV.v))(
        VersionedMessageV0
      )(
        supportedProtoVersionMemoized(_)(fromProtoV0),
        _.toProtoV0.toByteString,
      ),
      // Can use an unstable Protobuf message in an unstable protocol version
      ProtoVersion(2) -> VersionedProtoConverter(
        ProtocolVersion.unstable((basePV + 3).v)
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
