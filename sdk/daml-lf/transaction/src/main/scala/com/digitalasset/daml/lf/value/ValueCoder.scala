// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml
package lf
package value

import com.daml.SafeProto
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.transaction.{
  SerializationVersion,
  Versioned,
  ensuresNoUnknownFields,
  ensuresNoUnknownFieldsThenDecode,
}
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.value.{ValueOuterClass => proto}
import com.daml.scalautil.Statement.discard
import com.google.protobuf
import com.google.protobuf.{ByteString, CodedInputStream}

import scala.Ordering.Implicits.infixOrderingOps
import scala.jdk.CollectionConverters._

/** Error type for signalling errors occurring during decoding serialized values
  * @param errorMessage description
  */
final case class DecodeError(errorMessage: String)

object DecodeError extends (String => DecodeError) {
  private[lf] def apply(version: SerializationVersion, isTooOldFor: String): DecodeError =
    DecodeError(s"serialization version $version is too old to support $isTooOldFor")
}

/** Error type for signalling errors occurring during encoding values
  * @param errorMessage description
  */
final case class EncodeError(errorMessage: String)

object EncodeError extends (String => EncodeError) {
  private[lf] def apply(version: SerializationVersion, isTooOldFor: String): EncodeError =
    EncodeError(s"serialization version $version is too old to support $isTooOldFor")
}

object ValueCoder extends ValueCoder(allowNullCharacters = false)

/** Utilities to serialize and de-serialize Values
  * as they form part of transactions, nodes and contract instances
  */
class ValueCoder(allowNullCharacters: Boolean) {
  import Value.MAXIMUM_NESTING

  type DecodeError = value.DecodeError
  val DecodeError: value.DecodeError.type = value.DecodeError

  type EncodeError = value.EncodeError
  val EncodeError: value.EncodeError.type = value.EncodeError

  /** Simple encoding to wire of identifiers
    *
    * @param id identifier value
    * @return wire format identifier
    */
  def decodeIdentifier(id: proto.Identifier): Either[DecodeError, Identifier] =
    ensuresNoUnknownFieldsThenDecode(id)(internal.decodeIdentifier)

  def encodeIdentifier(id: Identifier): proto.Identifier =
    internal.encodeIdentifier(id)

  /** Serializes [[VersionedValue]] to protobuf.
    *
    * @param versionedValue value to be written
    * @tparam Cid ContractId type
    * @return protocol buffer serialized values
    */
  def encodeVersionedValue(
      versionedValue: VersionedValue
  ): Either[EncodeError, proto.VersionedValue] =
    internal.encodeVersionedValue(versionedValue)

  /** Reads a serialized protobuf versioned value,
    * checks if the value version is currently supported and
    * converts the value to the type usable by engine/interpreter.
    *
    * @param protoValue0 the value to be read
    * @tparam Cid ContractId type
    * @return either error or [VersionedValue]
    */
  def decodeVersionedValue(protoValue0: proto.VersionedValue): Either[DecodeError, VersionedValue] =
    ensuresNoUnknownFieldsThenDecode(protoValue0)(internal.decodeVersionedValue)

  /** Serialize a Value to protobuf
    *
    * @param v0           value to be written
    * @param valueVersion version of value specification to encode to, or fail
    * @tparam Cid ContractId type
    * @return protocol buffer serialized values
    */
  def encodeValue(valueVersion: SerializationVersion, v0: Value): Either[EncodeError, ByteString] =
    internal.encodeValue(valueVersion, v0)

  /** Method to read a serialized protobuf value
    * to engine/interpreter usable Value type
    *
    * @param protoValue0 the value to be read
    * @tparam Cid ContractId type
    * @return either error or Value
    */
  def decodeValue(version: SerializationVersion, bytes: ByteString): Either[DecodeError, Value] =
    internal.decodeValue(version, bytes)

  object internal {

    def decodeCoid(bytes: ByteString): Either[DecodeError, ContractId] = {
      val cid = Bytes.fromByteString(bytes)
      Value.ContractId
        .fromBytes(cid)
        .left
        .map(_ => //
          DecodeError(s"""cannot parse contractId "${cid.toHexString}"""")
        )
    }

    def decodeOptionalCoid(
        cid: ByteString
    ): Either[DecodeError, Option[ContractId]] =
      if (cid.isEmpty)
        Right(None)
      else
        decodeCoid(cid).map(Some(_))

    def encodeIdentifier(id: Identifier): proto.Identifier = {
      proto.Identifier
        .newBuilder()
        .setPackageId(id.packageId)
        .addAllModuleName((id.qualifiedName.module.segments.toSeq: Seq[String]).asJava)
        .addAllName((id.qualifiedName.name.segments.toSeq: Seq[String]).asJava)
        .build()
    }

    def decodeIdentifier(id: proto.Identifier): Either[DecodeError, Identifier] =
      for {
        pkgId <- PackageId
          .fromString(id.getPackageId)
          .left
          .map(err => DecodeError(s"Invalid package id '${id.getPackageId}': $err"))

        moduleSegments = id.getModuleNameList.asScala
        module <- ModuleName
          .fromSegments(id.getModuleNameList.asScala)
          .left
          .map(err => DecodeError(s"Invalid module segments $moduleSegments: $err"))

        nameSegments = id.getNameList.asScala
        name <- DottedName
          .fromSegments(nameSegments)
          .left
          .map(err => DecodeError(s"Invalid name segments $nameSegments: $err"))

      } yield Identifier(pkgId, QualifiedName(module, name))

    def decodeValueVersion(vs: String): Either[DecodeError, SerializationVersion] =
      SerializationVersion.fromString(vs).left.map(DecodeError)

    def decodeVersionedValue(
        protoValue0: proto.VersionedValue
    ): Either[DecodeError, VersionedValue] =
      for {
        version <- decodeValueVersion(protoValue0.getVersion)
        value <- decodeValue(version, protoValue0.getValue)
      } yield Versioned(version, value)

    // We need (3 * MAXIMUM_NESTING + 1) as record and maps use:
    // - 3 nested messages for the cases of non empty records/maps)
    // - 2 nested messages for the cases of empty records/maps
    // Note the number of recursions is one less than the number of nested messages.
    private[this] val MAXIMUM_PROTO_RECURSION_LIMIT = 3 * MAXIMUM_NESTING + 1

    def decodeValue(protoValue0: proto.VersionedValue): Either[DecodeError, Value] =
      decodeVersionedValue(protoValue0) map (_.unversioned)

    private[this] def parseValue(bytes: ByteString): Either[DecodeError, proto.Value] =
      try {
        val cis = CodedInputStream.newInstance(bytes.asReadOnlyByteBuffer())
        discard(cis.setRecursionLimit(MAXIMUM_PROTO_RECURSION_LIMIT))
        val msg = proto.Value.parseFrom(cis)
        ensuresNoUnknownFields(msg).map(_ => msg)
      } catch {
        case scala.util.control.NonFatal(err) =>
          Left(DecodeError("cannot parse proto Value: " + err.getMessage))
      }

    def decodeValue(version: SerializationVersion, bytes: ByteString): Either[DecodeError, Value] =
      parseValue(bytes).flatMap(decodeValue(version, _))

    private[this] def decodeValue(
        version: SerializationVersion,
        protoValue0: proto.Value,
    ): Either[DecodeError, Value] = {
      case class Err(msg: String) extends Throwable(null, null, true, false)

      def identifier(s: String): Name =
        Name
          .fromString(s)
          .fold(
            err => throw Err(s"error decoding variant constructor: $err"),
            identity,
          )

      @scala.annotation.nowarn("cat=unused")
      def assertSince(minVersion: SerializationVersion, description: => String) =
        if (version < minVersion)
          throw Err(s"$description is not supported by serialization version $version")

      @scala.annotation.nowarn("cat=unused")
      def assertUntil(minVersion: SerializationVersion, description: => String) =
        if (version >= minVersion)
          throw Err(s"$description is not supported by serialization version $version")

      def ensuresNoNullCharacters(s: String): s.type =
        if (!allowNullCharacters && s.contains('\u0000'))
          throw Err(s"text contains null character")
        else
          s

      def go(nesting: Int, protoValue: proto.Value): Value = {
        if (nesting > MAXIMUM_NESTING) {
          throw Err(
            s"Provided proto value to decode exceeds maximum nesting level of $MAXIMUM_NESTING"
          )
        } else {
          val newNesting = nesting + 1

          protoValue.getSumCase match {
            case proto.Value.SumCase.UNIT =>
              ValueUnit
            case proto.Value.SumCase.BOOL =>
              ValueBool(protoValue.getBool)
            case proto.Value.SumCase.INT64 =>
              ValueInt64(protoValue.getInt64)
            case proto.Value.SumCase.DATE =>
              val d = Time.Date.fromDaysSinceEpoch(protoValue.getDate)
              d.fold(e => throw Err("error decoding date: " + e), ValueDate)
            case proto.Value.SumCase.TIMESTAMP =>
              val t = Time.Timestamp.fromLong(protoValue.getTimestamp)
              t.fold(e => throw Err("error decoding timestamp: " + e), ValueTimestamp)
            case proto.Value.SumCase.NUMERIC =>
              Numeric
                .fromString(protoValue.getNumeric)
                .fold(e => throw Err("error decoding numeric: " + e), ValueNumeric)
            case proto.Value.SumCase.PARTY =>
              val party = Party.fromString(protoValue.getParty)
              party.fold(e => throw Err("error decoding party: " + e), ValueParty)
            case proto.Value.SumCase.TEXT =>
              ValueText(ensuresNoNullCharacters(protoValue.getText))
            case proto.Value.SumCase.CONTRACT_ID =>
              val cid = decodeCoid(protoValue.getContractId)
              cid.fold(
                e => throw Err("error decoding contractId: " + e.errorMessage),
                ValueContractId(_),
              )
            case proto.Value.SumCase.OPTIONAL =>
              val option = protoValue.getOptional
              val mbV =
                if (option.getValue == ValueOuterClass.Value.getDefaultInstance) None
                else Some(go(newNesting, option.getValue))
              ValueOptional(mbV)
            case proto.Value.SumCase.LIST =>
              val list = protoValue.getList
              ValueList(
                list.getElementsList.asScala.map(go(newNesting, _)).to(FrontStack)
              )
            case proto.Value.SumCase.MAP =>
              val genMap = protoValue.getMap
              val entries = genMap.getEntriesList.asScala.view
                .map { entry =>
                  go(newNesting, entry.getKey) -> go(newNesting, entry.getValue)
                }
                .to(ImmArray)
              ValueGenMap(entries)
            case proto.Value.SumCase.TEXT_MAP =>
              val textMap = protoValue.getTextMap
              val entries =
                textMap.getEntriesList.asScala.view
                  .map { entry =>
                    ensuresNoNullCharacters(entry.getKey) -> go(newNesting, entry.getValue)
                  }
                  .to(ImmArray)
              val map = SortedLookupList
                .fromImmArray(entries)
                .fold(
                  err => throw Err(err),
                  identity,
                )
              ValueTextMap(map)
            case proto.Value.SumCase.RECORD =>
              val record = protoValue.getRecord
              ValueRecord(
                None,
                record.getFieldsList.asScala.view
                  .map { fld =>
                    (None, go(newNesting, fld.getValue))
                  }
                  .to(ImmArray),
              )
            case proto.Value.SumCase.VARIANT =>
              val variant = protoValue.getVariant
              ValueVariant(
                None,
                identifier(variant.getConstructor),
                go(newNesting, variant.getValue),
              )
            case proto.Value.SumCase.ENUM =>
              val enumeration = protoValue.getEnum
              ValueEnum(None, identifier(enumeration.getValue))
            case proto.Value.SumCase.SUM_NOT_SET =>
              throw Err(s"Value not set")
          }
        }
      }

      try {
        Right(go(0, protoValue0))
      } catch {
        case Err(msg) => Left(DecodeError(msg))
      }
    }

    def encodeVersionedValue(
        versionedValue: VersionedValue
    ): Either[EncodeError, proto.VersionedValue] =
      encodeVersionedValue(versionedValue.version, versionedValue.unversioned)

    def encodeVersionedValue(
        version: SerializationVersion,
        value: Value,
    ): Either[EncodeError, proto.VersionedValue] =
      for {
        bytes <- encodeValue(version, value)
      } yield {
        val builder = proto.VersionedValue.newBuilder()
        builder.setVersion(SerializationVersion.toProtoValue(version)).setValue(bytes).build()
      }

    def encodeValue(
        valueVersion: SerializationVersion,
        v0: Value,
    ): Either[EncodeError, ByteString] = {
      case class Err(msg: String) extends Throwable(null, null, true, false)

      @scala.annotation.nowarn("cat=unused")
      def assertSince(minVersion: SerializationVersion, description: => String) =
        if (valueVersion < minVersion)
          throw Err(s"$description is not supported by value version $valueVersion")

      def ensuresNoNullCharacters(s: String): String =
        if (!allowNullCharacters && s.contains('\u0000'))
          throw Err(s"Null Character in Text")
        else
          s

      def go(nesting: Int, v: Value): proto.Value = {
        if (nesting > MAXIMUM_NESTING) {
          throw Err(
            s"Provided Daml-LF value to encode exceeds maximum nesting level of $MAXIMUM_NESTING"
          )
        } else {
          val newNesting = nesting + 1

          val builder = proto.Value.newBuilder()
          v match {
            case ValueUnit =>
              builder.setUnit(protobuf.Empty.newBuilder()).build()
            case ValueBool(b) =>
              builder.setBool(b).build()
            case ValueInt64(i) =>
              builder.setInt64(i).build()
            case ValueNumeric(d) =>
              builder.setNumeric(Numeric.toString(d)).build()
            case ValueText(t) =>
              builder.setText(ensuresNoNullCharacters(t)).build()
            case ValueParty(p) =>
              builder.setParty(p).build()
            case ValueDate(d) =>
              builder.setDate(d.days).build()
            case ValueTimestamp(t) =>
              builder.setTimestamp(t.micros).build()
            case ValueContractId(coid) =>
              builder.setContractId(coid.toBytes.toByteString).build()
            case ValueList(elems) =>
              val listBuilder = proto.Value.List.newBuilder()
              elems.foreach(elem => discard(listBuilder.addElements(go(newNesting, elem))))
              builder.setList(listBuilder).build()
            case ValueRecord(_, fields) =>
              val recordBuilder = proto.Value.Record.newBuilder()
              fields.foreach { case (_, field) =>
                val b = proto.Value.Record.Field.newBuilder()
                discard(b.setValue(go(newNesting, field)))
                discard(recordBuilder.addFields(b))
              }
              builder.setRecord(recordBuilder).build()

            case ValueVariant(_, con, arg) =>
              val protoVar = proto.Value.Variant
                .newBuilder()
                .setConstructor(con)
                .setValue(go(newNesting, arg))
              builder.setVariant(protoVar).build()

            case ValueEnum(_, value) =>
              val protoEnum = proto.Value.Enum
                .newBuilder()
                .setValue(value)
              builder.setEnum(protoEnum).build()

            case ValueOptional(mbV) =>
              val protoOption = proto.Value.Optional.newBuilder()
              mbV.foreach(v => protoOption.setValue(go(newNesting, v)))
              builder.setOptional(protoOption).build()

            case ValueTextMap(map) =>
              val protoMap = proto.Value.TextMap.newBuilder()
              map.toImmArray.foreach { case (key, value) =>
                discard(
                  protoMap.addEntries(
                    proto.Value.TextMap.Entry
                      .newBuilder()
                      .setKey(ensuresNoNullCharacters(key))
                      .setValue(go(newNesting, value))
                  )
                )
              }
              builder.setTextMap(protoMap).build()

            case ValueGenMap(entries) =>
              val protoMap = proto.Value.Map.newBuilder()
              entries.foreach { case (key, value) =>
                discard(
                  protoMap.addEntries(
                    proto.Value.Map.Entry
                      .newBuilder()
                      .setKey(go(newNesting, key))
                      .setValue(go(newNesting, value))
                  )
                )
              }
              builder.setMap(protoMap).build()

          }
        }
      }

      try {
        SafeProto.toByteString(go(0, v0)).left.map(EncodeError)
      } catch {
        case Err(msg) => Left(EncodeError(msg))
      }
    }

  }
}
