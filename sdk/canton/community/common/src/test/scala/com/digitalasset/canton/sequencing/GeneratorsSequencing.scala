// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.{Generators, SequencerAlias}
import magnolify.scalacheck.auto.genArbitrary
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsSequencing {
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.Generators.*

  implicit val sequencerAliasArb: Arbitrary[SequencerAlias] = Arbitrary(
    string255Arb.arbitrary.map(str =>
      SequencerAlias.create(str.str).valueOr(err => throw new IllegalArgumentException(err))
    )
  )

  implicit val endPointGen: Arbitrary[Endpoint] =
    Arbitrary(for {
      host <- Gen.alphaNumStr.filter(_.nonEmpty)
      port <- Arbitrary.arbitrary[Port]
    } yield Endpoint(host, port))

  implicit val endPointsArb: Arbitrary[NonEmpty[Seq[Endpoint]]] =
    Arbitrary(Generators.nonEmptySetGen[Endpoint].map(_.toSeq))

  implicit val sequencerConnectionArb: Arbitrary[SequencerConnection] = genArbitrary
  implicit val sequencerConnectionsArb: Arbitrary[SequencerConnections] = Arbitrary(
    for {
      connection <- Arbitrary.arbitrary[SequencerConnection]
    } yield SequencerConnections.single(connection)
  )
}
