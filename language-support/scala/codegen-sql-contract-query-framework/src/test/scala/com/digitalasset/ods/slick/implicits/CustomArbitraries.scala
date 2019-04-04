// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick.implicits

import java.time._
import java.util.concurrent.TimeUnit

import com.digitalasset.ledger.api.refinements.ApiTypes.{ContractId, Party, TemplateId, WorkflowId}
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.client.binding.Primitive
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.duration.Duration

trait CustomArbitraries {

  // limit the maximum date
  private val maxInstantMillis =
    LocalDate.of(9999, Month.DECEMBER, 31).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli
  private val maxDurationNanos = TimeUnit.HOURS.toNanos(24)

  implicit val instantArbitrary = Arbitrary[Instant](
    Gen.choose(0, maxInstantMillis).map(Instant.ofEpochMilli)
  )

  implicit val localDateArbitrary = Arbitrary[LocalDate](
    instantArbitrary.arbitrary
      .map(_.atZone(ZoneOffset.UTC))
      .map(_.toLocalDate)
  )

  private val identifierArbitrary: Gen[Identifier] =
    for {
      packageId <- Gen.alphaNumStr
      name <- Gen.alphaNumStr
    } yield Identifier(packageId, name)

  implicit val durationArbitrary = Arbitrary[Duration](
    Gen.choose(0L, maxDurationNanos).map(Duration.fromNanos)
  )

  implicit val javaDurationArbitrary = Arbitrary[java.time.Duration](
    durationArbitrary.arbitrary.map(d => java.time.Duration.ofNanos(d.toNanos))
  )

  implicit val partyArbitrary = Arbitrary[Party](
    Party.subst(Gen.alphaNumStr)
  )

  implicit val templateIdArbitrary = Arbitrary[TemplateId](
    TemplateId.subst(identifierArbitrary)
  )

  implicit val contractIdArbitrary = Arbitrary[ContractId](
    Gen.alphaNumStr.map(ContractId(_))
  )

  implicit def primiticeContractIdArbitrary[T]: Arbitrary[Primitive.ContractId[T]] =
    Arbitrary[Primitive.ContractId[T]](
      Gen.alphaNumStr.map(Primitive.ContractId(_))
    )

  implicit val ledgerProcessIdArbitrary = Arbitrary[WorkflowId](
    WorkflowId.subst(Gen.alphaNumStr)
  )

}
