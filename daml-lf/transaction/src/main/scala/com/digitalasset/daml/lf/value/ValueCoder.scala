// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package value

import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.transaction.{Versioned, TransactionVersion}
import com.daml.lf.value.Value._
import com.daml.lf.value.{ValueOuterClass => proto}
import com.daml.scalautil.Statement.discard
import com.google.protobuf
import com.google.protobuf.{ByteString, CodedInputStream}

import scala.Ordering.Implicits.infixOrderingOps
import scala.jdk.CollectionConverters._
import scala.util.{Try, Success, Failure}

/** Utilities to serialize and de-serialize Values
  * as they form part of transactions, nodes and contract instances
  */
object ValueCoder {
  import Value.MAXIMUM_NESTING

  /** Error type for signalling errors occurring during decoding serialized values
    * @param errorMessage description
    */
  final case class DecodeError(errorMessage: String)

  object DecodeError extends (String => DecodeError) {
    private[lf] def apply(version: TransactionVersion, isTooOldFor: String): DecodeError =
      DecodeError(s"transaction version ${version.protoValue} is too old to support $isTooOldFor")
  }

  /** Error type for signalling errors occurring during encoding values
    * @param errorMessage description
    */
  final case class EncodeError(errorMessage: String)

  object EncodeError extends (String => EncodeError) {
    private[lf] def apply(version: TransactionVersion, isTooOldFor: String): EncodeError =
      EncodeError(s"transaction version ${version.protoValue} is too old to support $isTooOldFor")
  }

  private[lf] def ensureNoUnknownFields(
      msg: com.google.protobuf.Message
  ): Either[DecodeError, Unit] = {
    val unknownFields = msg.getUnknownFields.asMap()
    Either.cond(
      unknownFields.isEmpty,
      (),
      DecodeError(
        s"unexpected field(s) ${unknownFields.keySet().asScala.mkString(", ")}  in ${msg.getClass.getSimpleName} message"
      ),
    )
  }

  object CidEncoder
  type EncodeCid = CidEncoder.type

  private[lf] def decodeCoid(bytes: ByteString): Either[DecodeError, ContractId] = {
    val cid = Bytes.fromByteString(bytes)
    Value.ContractId
      .fromBytes(cid)
      .left
      .map(_ => //
        DecodeError(s"""cannot parse contractId "${cid.toHexString}"""")
      )
  }

  private[lf] def decodeOptionalCoid(
      cid: ByteString
  ): Either[DecodeError, Option[ContractId]] =
    if (cid.isEmpty)
      Right(None)
    else
      decodeCoid(cid).map(Some(_))

  object CidDecoder
  type DecodeCid = CidDecoder.type

  // To be use only when certain the value does not contain Contract Ids
  @deprecated("use CidEncoder", since = "3.0.0")
  val UnsafeNoCidEncoder: EncodeCid = CidEncoder

  /** Simple encoding to wire of identifiers
    * @param id identifier value
    * @return wire format identifier
    */
  def encodeIdentifier(id: Identifier): proto.Identifier = {
    proto.Identifier
      .newBuilder()
      .setPackageId(id.packageId)
      .addAllModuleName((id.qualifiedName.module.segments.toSeq: Seq[String]).asJava)
      .addAllName((id.qualifiedName.name.segments.toSeq: Seq[String]).asJava)
      .build()
  }

  /** Decode identifier from wire format
    * @param id proto identifier
    * @return identifier
    */
  def decodeIdentifier(id: proto.Identifier): Either[DecodeError, Identifier] =
    for {
      _ <- ensureNoUnknownFields(id)
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

  private[lf] def encodeValueVersion(version: TransactionVersion): String =
    version.protoValue

  private[this] def decodeValueVersion(vs: String): Either[DecodeError, TransactionVersion] =
    TransactionVersion.fromString(vs).left.map(DecodeError)

  /** Reads a serialized protobuf versioned value,
    * checks if the value version is currently supported and
    * converts the value to the type usable by engine/interpreter.
    *
    * @param protoValue0 the value to be read
    * @tparam Cid ContractId type
    * @return either error or [VersionedValue]
    */
  def decodeVersionedValue(
      decodeCid: DecodeCid = CidDecoder,
      protoValue0: proto.VersionedValue,
  ): Either[DecodeError, VersionedValue] =
    for {
      _ <- ensureNoUnknownFields(protoValue0)
      version <- decodeValueVersion(protoValue0.getVersion)
      value <- decodeValue(decodeCid, version, protoValue0.getValue)
    } yield Versioned(version, value)

  // We need (3 * MAXIMUM_NESTING + 1) as record and maps use:
  // - 3 nested messages for the cases of non empty records/maps)
  // - 2 nested messages for the cases of empty records/maps
  // Note the number of recursions is one less than the number of nested messages.
  private[this] val MAXIMUM_PROTO_RECURSION_LIMIT = 3 * MAXIMUM_NESTING + 1

  def decodeValue(
      decodeCid: DecodeCid,
      protoValue0: proto.VersionedValue,
  ): Either[DecodeError, Value] =
    decodeVersionedValue(decodeCid, protoValue0) map (_.unversioned)

  private[this] def parseValue(bytes: ByteString): Either[DecodeError, proto.Value] =
    Try {
      val cis = CodedInputStream.newInstance(bytes.asReadOnlyByteBuffer())
      discard(cis.setRecursionLimit(MAXIMUM_PROTO_RECURSION_LIMIT))
      proto.Value.parseFrom(cis)
    } match {
      case Failure(exception: Error) =>
        Left(DecodeError("cannot parse proto Value: " + exception.getMessage))
      case Failure(throwable) =>
        throw throwable
      case Success(value) =>
        Right(value)
    }

  def decodeValue(protoValue0: proto.VersionedValue): Either[DecodeError, Value] =
    decodeValue(CidDecoder, protoValue0)

  def decodeValue(
      decodeCid: DecodeCid,
      version: TransactionVersion,
      bytes: ByteString,
  ): Either[DecodeError, Value] =
    parseValue(bytes).flatMap(decodeValue(decodeCid, version, _))

  /** Method to read a serialized protobuf value
    * to engine/interpreter usable Value type
    *
    * @param protoValue0 the value to be read
    * @tparam Cid ContractId type
    * @return either error or Value
    */

  def decodeValue(version: TransactionVersion, bytes: ByteString): Either[DecodeError, Value] =
    decodeValue(CidDecoder, version, bytes)

  @scala.annotation.nowarn("cat=unused")
  private[this] def decodeValue(
      decodeCid: DecodeCid,
      version: TransactionVersion,
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
    def assertSince(minVersion: TransactionVersion, description: => String) =
      if (version < minVersion)
        throw Err(s"$description is not supported by transaction version $version")

    @scala.annotation.nowarn("cat=unused")
    def assertUntil(minVersion: TransactionVersion, description: => String) =
      if (version >= minVersion)
        throw Err(s"$description is not supported by transaction version $version")

    def assertNoUnknownFields(msg: com.google.protobuf.Message) =
      ensureNoUnknownFields(msg).left.foreach(e => throw Err("error decoding decimal: " + e))

    def go(nesting: Int, protoValue: proto.Value): Value = {
      if (nesting > MAXIMUM_NESTING) {
        throw Err(
          s"Provided proto value to decode exceeds maximum nesting level of $MAXIMUM_NESTING"
        )
      } else {
        val newNesting = nesting + 1

        assertNoUnknownFields(protoValue)

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
              .fold(e => throw Err("error decoding decimal: " + e), ValueNumeric)
          case proto.Value.SumCase.PARTY =>
            val party = Party.fromString(protoValue.getParty)
            party.fold(e => throw Err("error decoding party: " + e), ValueParty)
          case proto.Value.SumCase.TEXT =>
            ValueText(protoValue.getText)
          case proto.Value.SumCase.CONTRACT_ID =>
            val cid = decodeCoid(protoValue.getContractId)
            cid.fold(
              e => throw Err("error decoding contractId: " + e.errorMessage),
              ValueContractId(_),
            )
          case proto.Value.SumCase.OPTIONAL =>
            val option = protoValue.getOptional
            assertNoUnknownFields(option)
            val mbV =
              if (option.getValue == ValueOuterClass.Value.getDefaultInstance) None
              else Some(go(newNesting, option.getValue))
            ValueOptional(mbV)
          case proto.Value.SumCase.LIST =>
            val list = protoValue.getList
            assertNoUnknownFields(list)
            ValueList(
              list.getElementsList.asScala.map(go(newNesting, _)).to(FrontStack)
            )
          case proto.Value.SumCase.MAP =>
            val genMap = protoValue.getMap
            assertNoUnknownFields(genMap)
            val entries = genMap.getEntriesList.asScala.view
              .map { entry =>
                assertNoUnknownFields(entry)
                go(newNesting, entry.getKey) -> go(newNesting, entry.getValue)
              }
              .to(ImmArray)
            ValueGenMap(entries)
          case proto.Value.SumCase.TEXT_MAP =>
            val textMap = protoValue.getTextMap
            assertNoUnknownFields(textMap)
            val entries =
              textMap.getEntriesList.asScala.view
                .map { entry =>
                  assertNoUnknownFields(entry)
                  entry.getKey -> go(newNesting, entry.getValue)
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
            assertNoUnknownFields(record)
            ValueRecord(
              None,
              record.getFieldsList.asScala.view
                .map { fld =>
                  assertNoUnknownFields(fld)
                  (None, go(newNesting, fld.getValue))
                }
                .to(ImmArray),
            )
          case proto.Value.SumCase.VARIANT =>
            val variant = protoValue.getVariant
            assertNoUnknownFields(variant)
            ValueVariant(None, identifier(variant.getConstructor), go(newNesting, variant.getValue))
          case proto.Value.SumCase.ENUM =>
            val enumeration = protoValue.getEnum
            assertNoUnknownFields(enumeration)
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

  /** Serializes [[VersionedValue]] to protobuf.
    *
    * @param versionedValue value to be written
    * @tparam Cid ContractId type
    * @return protocol buffer serialized values
    */
  def encodeVersionedValue(
      encodeCid: EncodeCid,
      versionedValue: VersionedValue,
  ): Either[EncodeError, proto.VersionedValue] =
    encodeVersionedValue(encodeCid, versionedValue.version, versionedValue.unversioned)

  def encodeVersionedValue(
      versionedValue: VersionedValue
  ): Either[EncodeError, proto.VersionedValue] =
    encodeVersionedValue(CidEncoder, versionedValue.version, versionedValue.unversioned)

  def encodeVersionedValue(
      encodeCid: EncodeCid,
      version: TransactionVersion,
      value: Value,
  ): Either[EncodeError, proto.VersionedValue] =
    for {
      bytes <- encodeValue(encodeCid, version, value)
    } yield {
      val builder = proto.VersionedValue.newBuilder()
      builder.setVersion(encodeValueVersion(version)).setValue(bytes).build()
    }

  def encodeVersionedValue(
      version: TransactionVersion,
      value: Value,
  ): Either[EncodeError, proto.VersionedValue] =
    encodeVersionedValue(CidEncoder, version, value)

  /** Serialize a Value to protobuf
    *
    * @param v0 value to be written
    * @param encodeCid a function to stringify contractIds (it's better to be invertible)
    * @param valueVersion version of value specification to encode to, or fail
    * @tparam Cid ContractId type
    * @return protocol buffer serialized values
    */
  @scala.annotation.nowarn("cat=unused")
  def encodeValue(
      encodeCid: EncodeCid = CidEncoder,
      valueVersion: TransactionVersion,
      v0: Value,
  ): Either[EncodeError, ByteString] = {
    case class Err(msg: String) extends Throwable(null, null, true, false)

    @scala.annotation.nowarn("cat=unused")
    def assertSince(minVersion: TransactionVersion, description: => String) =
      if (valueVersion < minVersion)
        throw Err(s"$description is not supported by value version $valueVersion")

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
            builder.setText(t).build()
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
            elems.foreach(elem => {
              discard(listBuilder.addElements(go(newNesting, elem)))
            })
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
                    .setKey(key)
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

  private[value] def valueFromBytes(
      decodeCid: DecodeCid,
      bytes: Array[Byte],
  ): Either[DecodeError, Value] = {
    decodeValue(decodeCid, proto.VersionedValue.parseFrom(bytes))
  }

}
