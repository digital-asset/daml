// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package data

import com.digitalasset.daml.lf.Sized.{LONG_S, OBJECT_HEADER_S, REFERENCE_S, SizedBytesString}
import com.digitalasset.daml.lf.SizedFixSizeAtom
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf

import scala.annotation.nowarn

@nowarn("msg=implicit numeric widening")
object Sized {

  implicit def SizedImmArray[A](implicit a: Sized[A]): SizedContainer1[ImmArray, A] =
    new SizedContainer1[ImmArray, A] {

      override def elements(x: ImmArray[A]): Iterator[A] = x.iterator

      override def approximateShallowFootprint(n: Int): Long = 80 + 8 * n

      override def fromList(a: List[A]): ImmArray[A] = a.to(ImmArray)
    }

  implicit def SizedFrontStack[A](implicit a: Sized[A]): SizedContainer1[FrontStack, A] =
    new SizedContainer1[FrontStack, A] {

      override def elements(x: FrontStack[A]): Iterator[A] = x.iterator

      override def approximateShallowFootprint(n: Int): Long = 48 + 32 * n

      override def fromList(as: List[A]): FrontStack[A] =
        as.foldLeft(FrontStack.empty[A])(_.+:(_))
    }

  implicit def SizedBackStack[A](implicit a: Sized[A]): SizedContainer1[BackStack, A] =
    new SizedContainer1[BackStack, A] {

      override def elements(x: BackStack[A]): Iterator[A] = x.reverseIterator

      override def fromList(as: List[A]): BackStack[A] =
        as.foldLeft(BackStack.empty[A])(_.:+(_))

      override def approximateShallowFootprint(n: Int): Long = 48 + 32 * n

    }

  implicit val SizedBytes: SizedWrapper1[data.Bytes, protobuf.ByteString] =
    new SizedWrapper1[data.Bytes, protobuf.ByteString](
      data.Bytes.fromByteString,
      _.toByteString,
    )

  implicit val SizedFixSizeAtom: SizedFixSizeAtom[Hash] = new SizedFixSizeAtom[crypto.Hash](
    OBJECT_HEADER_S +
      REFERENCE_S +
      SizedBytes.approximateShallowFootprint_ + // scala drops this wrapping
      SizedBytesString.approximateFootprint(Hash.underlyingHashLength)
  )

  implicit lazy val SizedNumeric: Sized[Numeric] = new SizedFixSizeAtom[Numeric](232) {
    final override def bloat(x: Numeric) = {
      val _ = x.toString
    }
  }

  implicit lazy val SizedDate: Sized[Time.Date] = new SizedFixSizeAtom[Time.Date](24)

  implicit lazy val SizedTimestamp: Sized[Time.Timestamp] = new SizedFixSizeAtom[Time.Timestamp](24)

  def SizedIdString[T <: String] = new SizedVariableLengthAtom[T] {

    override def length(x: T): Int = x.length

    override def approximateFootprint(n: Int): Long = 63 + n
  }

  implicit lazy val SizedPackageId: SizedVariableLengthAtom[Ref.PackageId] = SizedIdString

  implicit lazy val SizedParty: SizedVariableLengthAtom[Ref.Party] = SizedIdString

  implicit lazy val SizedName: SizedVariableLengthAtom[Ref.Name] = SizedIdString

  implicit lazy val SizedDottedName: SizedWrapper1[Ref.DottedName, ImmArray[Ref.Name]] =
    new SizedWrapper1[Ref.DottedName, ImmArray[Ref.Name]](
      Ref.DottedName.assertFromNames,
      _.segments,
      24,
    )

  implicit val SizeQualifiedName: SizedWrapper2[Ref.QualifiedName, Ref.DottedName, Ref.DottedName] =
    new SizedWrapper2[Ref.QualifiedName, Ref.DottedName, Ref.DottedName](
      Ref.QualifiedName(_, _),
      (x: Ref.QualifiedName) => (x.module, x.name),
      32,
    )

  implicit lazy val SizedIdentifier
      : SizedWrapper2[Ref.Identifier, Ref.PackageId, Ref.QualifiedName] =
    new SizedWrapper2[Ref.Identifier, Ref.PackageId, Ref.QualifiedName](
      Ref.Identifier(_, _),
      (x: Ref.Identifier) => (x.packageId, x.qualifiedName),
      32,
    )

  implicit object SizedContractIdV1
      extends SizedWrapper2[Value.ContractId.V1, crypto.Hash, data.Bytes](
        Value.ContractId.V1(_, _),
        (x: Value.ContractId.V1) => (x.discriminator, x.suffix),
        (OBJECT_HEADER_S
          + LONG_S // lazyness bitmap
          + REFERENCE_S // toBytes
          + REFERENCE_S // coid
          + REFERENCE_S // discrimitaot
          + REFERENCE_S // suffix
        ) max 528,
      ) {
    override def bloat(x: ContractId.V1): Unit = {
      val _ = x.toBytes
      val _ = x.coid
    }
  }

  implicit lazy val SizedContractId: Sized[ContractId] =
    SizedContractIdV1.asInstanceOf[Sized[ContractId]]

}
