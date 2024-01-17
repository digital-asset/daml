// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.{
  AbstractLengthLimitedString,
  LengthLimitedStringCompanion,
}
import com.google.protobuf.ByteString
import org.scalacheck.{Arbitrary, Gen}

object Generators {
  private val nonEmptyMaxSize: Int = 4

  implicit val byteStringArb: Arbitrary[ByteString] = Arbitrary(
    Gen.stringOfN(256, Gen.alphaNumChar).map(ByteString.copyFromUtf8)
  )

  implicit val applicationIdArb: Arbitrary[ApplicationId] = Arbitrary(
    Gen.stringOfN(32, Gen.alphaNumChar).map(ApplicationId.assertFromString)
  )
  implicit val commandIdArb: Arbitrary[CommandId] = Arbitrary(
    Gen.stringOfN(32, Gen.alphaNumChar).map(CommandId.assertFromString)
  )
  implicit val ledgerSubmissionIdArb: Arbitrary[LedgerSubmissionId] = Arbitrary(
    Gen.stringOfN(32, Gen.alphaNumChar).map(LedgerSubmissionId.assertFromString)
  )
  implicit val workflowIdArb: Arbitrary[WorkflowId] = Arbitrary(
    Gen.stringOfN(32, Gen.alphaNumChar).map(WorkflowId.assertFromString)
  )

  def transferCounterOGen: Gen[TransferCounterO] =
    Gen.choose(0, Long.MaxValue).map(i => Some(TransferCounter(i)))

  def lengthLimitedStringGen[A <: AbstractLengthLimitedString](
      companion: LengthLimitedStringCompanion[A]
  ): Gen[A] = for {
    length <- Gen.choose(1, companion.maxLength)
    str <- Gen.stringOfN(length, Gen.alphaNumChar)
  } yield companion.tryCreate(str)

  def nonEmptyListGen[T](implicit arb: Arbitrary[T]): Gen[NonEmpty[List[T]]] = for {
    size <- Gen.choose(1, nonEmptyMaxSize - 1)
    element <- arb.arbitrary
    elements <- Gen.containerOfN[List, T](size, arb.arbitrary)
  } yield NonEmpty(List, element, elements: _*)

  def nonEmptySetGen[T](implicit arb: Arbitrary[T]): Gen[NonEmpty[Set[T]]] =
    nonEmptyListGen[T].map(_.toSet)
  def nonEmptySet[T](implicit arb: Arbitrary[T]): Arbitrary[NonEmpty[Set[T]]] =
    Arbitrary(nonEmptyListGen[T].map(_.toSet))
}
