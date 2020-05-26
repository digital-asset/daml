// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.config

import com.daml.lf.data.Ref.Party

import scalaz.OneAnd
import scopt.Read
import scopt.Read.reads

import scala.language.higherKinds
import scala.collection.breakOut
import scala.collection.generic.CanBuildFrom

private[extractor] object CustomScoptReaders {
  implicit val partyRead: Read[Party] = reads { s =>
    Party fromString s fold (e => throw new IllegalArgumentException(e), identity)
  }

  implicit val templateConfigRead: Read[TemplateConfig] = reads { s =>
    s.split(':') match {
      case Array(moduleName, entityName) =>
        TemplateConfig(moduleName, entityName)
      case _ =>
        throw new IllegalArgumentException(
          s"Expected TemplateConfig string: '<moduleName>:<entityName>', got: '$s'")
    }
  }

  implicit def nonEmptySeqRead[F[_], A](
      implicit ev: Read[A],
      target: CanBuildFrom[Nothing, A, F[A]]): Read[OneAnd[F, A]] = reads { s =>
    val Array(hd, tl @ _*) = s split Read.sep
    OneAnd(ev reads hd, tl.map(ev.reads)(breakOut))
  }
}
