// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.checked
import com.google.protobuf.ByteString

/** This trait wraps a ByteString that is limited to a certain maximum length.
  * Classes implementing this trait expose `create` and `tryCreate` methods to safely (and non-safely) construct
  * such a ByteString.
  *
  * The canonical use case is ensuring that we don't encrypt more data than the underlying crypto algorithm can:
  * for example, Rsa2048OaepSha256 can only encrypt 190 bytes at a time.
  */
sealed trait LengthLimitedByteString {
  protected def str: ByteString

  /** Maximum number of byte characters allowed. */
  def maxLength: Int

  // optionally give a name for the type of ByteString you are attempting to validate for nicer error messages
  def name: Option[String] = None

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  def canEqual(a: Any): Boolean =
    a.isInstanceOf[LengthLimitedByteString] || a.isInstanceOf[ByteString]

  override def equals(that: Any): Boolean =
    that match {
      case that: LengthLimitedByteString =>
        canEqual(this) && this.str == that.str && this.maxLength == that.maxLength
      case that: ByteString => canEqual(this) && this.str == that
      case _ => false
    }

  override def hashCode(): Int = str.hashCode()

  require(
    str.size() <= maxLength,
    s"The given ${name.getOrElse("byteString")} has a maximum length of $maxLength but a ${name
        .getOrElse("byteString")} of length ${str.size()} ('$str') was given",
  )

  def unwrap: ByteString = str

  override def toString: String = str.toString

}

final case class ByteString190 private (str: ByteString)(override val name: Option[String])
    extends LengthLimitedByteString {
  override def maxLength: Int = ByteString190.maxLength
}

object ByteString190 extends LengthLimitedByteStringCompanion[ByteString190] {
  override def maxLength: Int = 190

  override protected def factoryMethod(str: ByteString)(name: Option[String]): ByteString190 =
    ByteString190(str)(name)
}

final case class ByteString256 private (str: ByteString)(override val name: Option[String])
    extends LengthLimitedByteString {
  override def maxLength: Int = ByteString256.maxLength
}

object ByteString256 extends LengthLimitedByteStringCompanion[ByteString256] {
  override def maxLength: Int = 256

  override protected def factoryMethod(str: ByteString)(name: Option[String]): ByteString256 =
    ByteString256(str)(name)
}

final case class ByteString4096 private (str: ByteString)(override val name: Option[String])
    extends LengthLimitedByteString {
  override def maxLength: Int = ByteString6144.maxLength
}

object ByteString4096 extends LengthLimitedByteStringCompanion[ByteString4096] {
  override def maxLength: Int = 4096

  override protected def factoryMethod(str: ByteString)(name: Option[String]): ByteString4096 =
    ByteString4096(str)(name)
}

final case class ByteString6144 private (str: ByteString)(override val name: Option[String])
    extends LengthLimitedByteString {
  override def maxLength: Int = ByteString6144.maxLength
}

object ByteString6144 extends LengthLimitedByteStringCompanion[ByteString6144] {
  override def maxLength: Int = 6144

  override protected def factoryMethod(str: ByteString)(name: Option[String]): ByteString6144 =
    ByteString6144(str)(name)
}

/** Trait that implements method commonly needed in the companion object of an [[LengthLimitedByteString]] */
trait LengthLimitedByteStringCompanion[A <: LengthLimitedByteString] {

  val empty: A = checked(factoryMethod(ByteString.EMPTY)(None))

  /** The maximum byteString length. Should not be overwritten with `val` to avoid initialization issues. */
  def maxLength: Int

  /** Factory method for creating a ByteString.
    *
    * @throws java.lang.IllegalArgumentException if `str` is longer than [[maxLength]]
    */
  protected def factoryMethod(str: ByteString)(name: Option[String]): A

  private def errorMsg(
      tooLongStr: ByteString,
      maxLength: Int,
      name: Option[String],
  ): String =
    s"The given ${name.getOrElse("byteString")} has a maximum length of $maxLength but a ${name
        .getOrElse("byteString")} of length ${tooLongStr.size()} was given"

  def create(str: ByteString, name: Option[String] = None): Either[String, A] =
    Either.cond(
      str.size() <= maxLength,
      factoryMethod(str)(name),
      errorMsg(str, maxLength, name),
    )

  def tryCreate(str: ByteString, name: Option[String] = None): A =
    factoryMethod(str)(name)
}
