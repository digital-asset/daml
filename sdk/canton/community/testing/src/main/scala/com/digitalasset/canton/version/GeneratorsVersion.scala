// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import org.scalacheck.{Arbitrary, Gen}

object GeneratorsVersion {

  // Prefer to use `representativeProtocolVersionGen` which offers a better distribution over the protobuf versions
  implicit val protocolVersionArb: Arbitrary[ProtocolVersion] =
    Arbitrary(Gen.oneOf(ProtocolVersion.supported))

  def valueForEmptyOptionExactlyUntilExclusive[
      ValueClass <: HasRepresentativeProtocolVersion,
      Comp <: HasProtocolVersionedWrapperCompanion[ValueClass, _],
      T,
  ](
      pv: ProtocolVersion,
      invariant: EmptyOptionExactlyUntilExclusive[ValueClass, Comp, T],
      gen: Gen[T],
  ): Gen[Option[T]] =
    if (pv < invariant.untilExclusive.representative) Gen.const(None) else Gen.some(gen)

  def valueForEmptyOptionExactlyUntilExclusive[
      ValueClass <: HasRepresentativeProtocolVersion,
      Comp <: HasProtocolVersionedWrapperCompanion[ValueClass, _],
      T,
  ](
      pv: ProtocolVersion,
      invariant: EmptyOptionExactlyUntilExclusive[ValueClass, Comp, T],
  )(implicit arb: Arbitrary[T]): Gen[Option[T]] =
    valueForEmptyOptionExactlyUntilExclusive(pv, invariant, arb.arbitrary)

  def defaultValueGen[
      ValueClass <: HasRepresentativeProtocolVersion,
      Comp <: HasSupportedProtoVersions[ValueClass, ?, ?, ?],
      T,
  ](
      protocolVersion: ProtocolVersion,
      defaultValue: DefaultValue[ValueClass, Comp, T],
      gen: Gen[T],
  ): Gen[T] =
    gen.map(defaultValue.orValue(_, protocolVersion))

  def defaultValueGen[
      ValueClass <: HasRepresentativeProtocolVersion,
      Comp <: HasProtocolVersionedWrapperCompanion[ValueClass, _],
      T,
  ](
      protocolVersion: ProtocolVersion,
      defaultValue: DefaultValue[ValueClass, Comp, T],
  )(implicit arb: Arbitrary[T]): Gen[T] =
    arb.arbitrary.map(defaultValue.orValue(_, protocolVersion))

  def defaultValueGen[
      ValueClass <: HasRepresentativeProtocolVersion,
      Comp <: HasProtocolVersionedWrapperCompanion[ValueClass, _],
      T,
  ](
      protocolVersion: RepresentativeProtocolVersion[Comp],
      defaultValue: DefaultValue[ValueClass, Comp, T],
  )(implicit arb: Arbitrary[T]): Gen[T] =
    defaultValueGen(protocolVersion.representative, defaultValue)

  def defaultValueArb[
      ValueClass <: HasRepresentativeProtocolVersion,
      Comp <: HasProtocolVersionedWrapperCompanion[ValueClass, _],
      T,
  ](
      protocolVersion: RepresentativeProtocolVersion[Comp],
      defaultValue: DefaultValue[ValueClass, Comp, T],
  )(implicit arb: Arbitrary[T]): Gen[T] =
    defaultValueGen(protocolVersion.representative, defaultValue)

  def representativeProtocolVersionGen[ValueClass <: HasRepresentativeProtocolVersion](
      companion: HasProtocolVersionedWrapperCompanion[ValueClass, _]
  ): Gen[RepresentativeProtocolVersion[companion.type]] =
    representativeProtocolVersionFilteredGen(companion)(Nil)

  def representativeProtocolVersionFilteredGen[ValueClass <: HasRepresentativeProtocolVersion](
      companion: HasProtocolVersionedWrapperCompanion[ValueClass, _]
  )(
      exclude: List[RepresentativeProtocolVersion[companion.type]] = Nil
  ): Gen[RepresentativeProtocolVersion[companion.type]] =
    Gen.oneOf(companion.versioningTable.converters.forgetNE.values.collect {
      case codec if codec.isSupported && !exclude.contains(codec.fromInclusive) =>
        codec.fromInclusive
    })
}
