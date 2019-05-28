// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.config

import com.digitalasset.daml.lf.data.Ref.Party

import scalaz.OneAnd
import scopt.Read
import scopt.Read.reads

import scala.language.higherKinds
import scala.collection.breakOut
import scala.collection.generic.CanBuildFrom

private[extractor] object CustomScoptReaders {
  implicit def partyRead: Read[Party] = reads { s =>
    Party fromString s fold (e => throw new IllegalArgumentException(e), identity)
  }

  implicit def nonEmptySeqRead[F[_], A](
      implicit ev: Read[A],
      target: CanBuildFrom[Nothing, A, F[A]]): Read[OneAnd[F, A]] = reads { s =>
    val Array(hd, tl @ _*) = s split Read.sep
    OneAnd(ev reads hd, tl.map(ev.reads)(breakOut))
  }
}
