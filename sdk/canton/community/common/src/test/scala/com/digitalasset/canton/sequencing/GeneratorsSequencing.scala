// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.topology.GeneratorsTopology
import com.digitalasset.canton.{Generators, SequencerAlias}
import magnolify.scalacheck.auto.genArbitrary
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsSequencing(generatorsTopology: GeneratorsTopology) {
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.Generators.*
  import generatorsTopology.*

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
  implicit val submissionRequestAmplificationArb: Arbitrary[SubmissionRequestAmplification] =
    genArbitrary
  implicit val sequencerConnectionPoolDelaysArb: Arbitrary[SequencerConnectionPoolDelays] =
    genArbitrary

  implicit val sequencerConnectionsArb: Arbitrary[SequencerConnections] = Arbitrary(
    for {
      connections <- Generators
        .nonEmptySetGen[SequencerConnection]
        .map(_.toSeq)
        .map(_.distinctBy(_.sequencerAlias))
      sequencerTrustThreshold <- Gen.choose(1, connections.size).map(PositiveInt.tryCreate)
      sequencerLivenessMargin <- Gen
        .choose(0, connections.size - sequencerTrustThreshold.unwrap)
        .map(NonNegativeInt.tryCreate)
      submissionRequestAmplification <- submissionRequestAmplificationArb.arbitrary
      sequencerConnectionPoolDelays <- sequencerConnectionPoolDelaysArb.arbitrary
    } yield SequencerConnections.tryMany(
      connections,
      sequencerTrustThreshold,
      sequencerLivenessMargin,
      submissionRequestAmplification,
      sequencerConnectionPoolDelays,
    )
  )
}
