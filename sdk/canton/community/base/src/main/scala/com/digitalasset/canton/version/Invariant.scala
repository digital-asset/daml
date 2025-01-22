// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import scala.math.Ordered.orderingToOrdered
import scala.reflect.ClassTag

trait Invariant[ValueClass, Comp] {
  def validateInstance(
      v: ValueClass,
      rpv: RepresentativeProtocolVersion[Comp],
  ): Either[String, Unit]
}

private[version] sealed trait InvariantImpl[ValueClass, Comp, T]
    extends Invariant[ValueClass, Comp]
    with Product
    with Serializable {
  def attribute: ValueClass => T
  def validate(v: T, pv: ProtocolVersion): Either[String, Unit]
  def validate(v: T, rpv: RepresentativeProtocolVersion[Comp]): Either[String, Unit]
  def validateInstance(
      v: ValueClass,
      rpv: RepresentativeProtocolVersion[Comp],
  ): Either[String, Unit] =
    validate(attribute(v), rpv)
}

/*
  This trait encodes a default value starting (or ending) at a specific protocol version.
 */
private[version] sealed trait DefaultValue[ValueClass, Comp, T]
    extends InvariantImpl[ValueClass, Comp, T] {

  def defaultValue: T

  /** Returns `v` or the default value, depending on the `protocolVersion`.
    */
  def orValue(v: T, protocolVersion: ProtocolVersion): T

  /** Returns `v` or the default value, depending on the `protocolVersion`.
    */
  def orValue(v: T, protocolVersion: RepresentativeProtocolVersion[Comp]): T

  override def validate(v: T, rpv: RepresentativeProtocolVersion[Comp]): Either[String, Unit] =
    validate(v, rpv.representative)
}

final case class DefaultValueFromInclusive[ValueClass, Comp: ClassTag, T](
    attribute: ValueClass => T,
    attributeName: String,
    startInclusive: RepresentativeProtocolVersion[Comp],
    defaultValue: T,
) extends DefaultValue[ValueClass, Comp, T] {

  def orValue(v: T, protocolVersion: ProtocolVersion): T =
    if (protocolVersion >= startInclusive.representative) defaultValue else v

  def orValue(v: T, protocolVersion: RepresentativeProtocolVersion[Comp]): T =
    if (protocolVersion >= startInclusive) defaultValue else v

  override def validate(
      v: T,
      pv: ProtocolVersion,
  ): Either[String, Unit] = {
    val shouldHaveDefaultValue = pv >= startInclusive.representative

    Either.cond(
      !shouldHaveDefaultValue || v == defaultValue,
      (),
      s"expected default value for $attributeName in ${implicitly[ClassTag[Comp]].getClass.getSimpleName} but found $v",
    )
  }
}

final case class DefaultValueUntilExclusive[ValueClass, Comp: ClassTag, T](
    attribute: ValueClass => T,
    attributeName: String,
    untilExclusive: RepresentativeProtocolVersion[Comp],
    defaultValue: T,
) extends DefaultValue[ValueClass, Comp, T] {
  def orValue(v: T, protocolVersion: ProtocolVersion): T =
    if (protocolVersion < untilExclusive.representative) defaultValue else v

  def orValue(v: T, protocolVersion: RepresentativeProtocolVersion[Comp]): T =
    if (protocolVersion < untilExclusive) defaultValue else v

  override def validate(
      v: T,
      pv: ProtocolVersion,
  ): Either[String, Unit] = {
    val shouldHaveDefaultValue = pv < untilExclusive.representative

    Either.cond(
      !shouldHaveDefaultValue || v == defaultValue,
      (),
      s"expected default value for $attributeName in ${implicitly[ClassTag[Comp]].getClass.getSimpleName} but found $v",
    )
  }
}

final case class EmptyOptionExactlyUntilExclusive[ValueClass, Comp, T](
    attribute: ValueClass => Option[T],
    attributeName: String,
    untilExclusive: RepresentativeProtocolVersion[Comp],
) extends DefaultValue[ValueClass, Comp, Option[T]] {
  val defaultValue: Option[T] = None

  def orValue(v: Option[T], protocolVersion: ProtocolVersion): Option[T] =
    if (protocolVersion < untilExclusive.representative) defaultValue else v

  def orValue(v: Option[T], protocolVersion: RepresentativeProtocolVersion[Comp]): Option[T] =
    if (protocolVersion < untilExclusive) defaultValue else v

  override def validate(
      v: Option[T],
      pv: ProtocolVersion,
  ): Either[String, Unit] =
    Either.cond(
      v.isEmpty == pv < untilExclusive.representative,
      (),
      s"expecting None for $attributeName if and only if pv < ${untilExclusive.representative}; for $pv, found: $v",
    )
}
