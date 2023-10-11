// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import org.scalacheck.{Arbitrary, Gen}

object GeneratorsVersion {

  // Prefer to use `representativeProtocolVersionGen` which offers a better distribution over the protobuf versions
  implicit val protocolVersionArb: Arbitrary[ProtocolVersion] =
    Arbitrary(Gen.oneOf(ProtocolVersion.supported))

  def defaultValueGen[Comp <: HasProtocolVersionedWrapperCompanion[_, _], T](
      protocolVersion: ProtocolVersion,
      defaultValue: Comp#DefaultValue[T],
      gen: Gen[T],
  ): Gen[T] =
    gen.map(defaultValue.orValue(_, protocolVersion))

  def defaultValueGen[Comp <: HasProtocolVersionedWrapperCompanion[_, _], T](
      protocolVersion: ProtocolVersion,
      defaultValue: Comp#DefaultValue[T],
  )(implicit arb: Arbitrary[T]): Gen[T] =
    arb.arbitrary.map(defaultValue.orValue(_, protocolVersion))

  def defaultValueGen[Comp <: HasProtocolVersionedWrapperCompanion[_, _], T](
      protocolVersion: RepresentativeProtocolVersion[Comp],
      defaultValue: Comp#DefaultValue[T],
  )(implicit arb: Arbitrary[T]): Gen[T] =
    defaultValueGen(protocolVersion.representative, defaultValue)

  def defaultValueArb[Comp <: HasProtocolVersionedWrapperCompanion[_, _], T](
      protocolVersion: RepresentativeProtocolVersion[Comp],
      defaultValue: Comp#DefaultValue[T],
  )(implicit arb: Arbitrary[T]): Gen[T] =
    defaultValueGen(protocolVersion.representative, defaultValue)

  def representativeProtocolVersionGen[ValueClass <: HasRepresentativeProtocolVersion](
      companion: HasProtocolVersionedWrapperCompanion[ValueClass, _]
  ): Gen[RepresentativeProtocolVersion[companion.type]] = {
    Gen.oneOf(companion.supportedProtoVersions.converters.forgetNE.values.collect {
      case codec if codec.isSupported => codec.fromInclusive
    })
  }
}
