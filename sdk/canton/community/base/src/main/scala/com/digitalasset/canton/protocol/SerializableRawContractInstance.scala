// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass}
import com.daml.lf.value.ValueCoder
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  MemoizedEvidenceWithFailure,
  ProtoConverter,
  SerializationCheckFailed,
}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.Lens
import monocle.macros.GenLens
import slick.jdbc.{GetResult, SetParameter}

import scala.annotation.unused

/** Represents a serializable contract instance and memoizes the serialization.
  *
  * @param contractInstance The contract instance whose serialization is to be memoized.
  * @param deserializedFrom If set, the given [[ByteString]] will be deemed to be the valid serialization for
  *                         the given contract instance. If [[None]],
  *                         the serialization is produced by [[TransactionCoder.encodeContractInstance]].
  */
final case class SerializableRawContractInstance private (
    contractInstance: LfContractInst
)(
    override val deserializedFrom: Option[ByteString]
) extends MemoizedEvidenceWithFailure[ValueCoder.EncodeError] {

  /** @throws com.digitalasset.canton.serialization.SerializationCheckFailed If the serialization of the contract instance failed
    */
  @throws[SerializationCheckFailed[ValueCoder.EncodeError]]
  protected[this] override def toByteStringChecked: Either[ValueCoder.EncodeError, ByteString] =
    TransactionCoder
      .encodeContractInstance(coinst = contractInstance)
      .map(_.toByteString)

  lazy val contractHash: LfHash =
    LfHash.assertHashContractInstance(
      contractInstance.unversioned.template,
      contractInstance.unversioned.arg,
      contractInstance.unversioned.packageName,
    )

  @unused // needed for lenses
  private def copy(
      contractInstance: LfContractInst = this.contractInstance
  ): SerializableRawContractInstance =
    SerializableRawContractInstance(contractInstance)(None)
}

object SerializableRawContractInstance {

  @VisibleForTesting
  lazy val contractInstanceUnsafe: Lens[SerializableRawContractInstance, LfContractInst] =
    GenLens[SerializableRawContractInstance](_.contractInstance)

  implicit def contractGetResult(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[SerializableRawContractInstance] = GetResult { r =>
    SerializableRawContractInstance
      .fromByteString(ByteString.copyFrom(r.<<[Array[Byte]]))
      .getOrElse(throw new DbDeserializationException("Invalid contract instance"))
  }

  implicit def contractSetParameter(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[SerializableRawContractInstance] = (c, pp) =>
    pp >> c.getCryptographicEvidence.toByteArray

  def create(
      contractInstance: LfContractInst
  ): Either[ValueCoder.EncodeError, SerializableRawContractInstance] =
    try {
      Right(new SerializableRawContractInstance(contractInstance)(None))
    } catch {
      case SerializationCheckFailed(err: ValueCoder.EncodeError) => Left(err)
    }

  /** Build a [[SerializableRawContractInstance]] from lf-protobuf and ContractId encoded ContractInst
    * @param bytes byte string representing contract instance
    * @return  contract id
    */
  def fromByteString(
      bytes: ByteString
  ): ParsingResult[SerializableRawContractInstance] =
    for {
      contractInstanceP <- ProtoConverter.protoParser(
        TransactionOuterClass.ContractInstance.parseFrom
      )(bytes)
      contractInstance <- TransactionCoder
        .decodeContractInstance(protoCoinst = contractInstanceP)
        .leftMap(error => ValueConversionError("", error.toString))
    } yield createWithSerialization(contractInstance)(bytes)

  @VisibleForTesting
  def createWithSerialization(contractInst: LfContractInst)(
      deserializedFrom: ByteString
  ): SerializableRawContractInstance =
    new SerializableRawContractInstance(contractInst)(Some(deserializedFrom))
}
