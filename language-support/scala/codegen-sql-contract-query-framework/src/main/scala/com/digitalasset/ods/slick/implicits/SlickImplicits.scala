// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick.implicits

import java.sql.Timestamp
import java.time.Instant

import com.digitalasset.ledger.api.refinements.ApiTypes.{ContractId, Party, WorkflowId}
import com.digitalasset.ledger.client.binding.{Primitive => P}
import com.digitalasset.ods.slick.SlickProfile
import scalaz.syntax.tag._

import scala.concurrent.duration.Duration

//noinspection TypeAnnotation
trait SlickImplicits {
  self: SlickProfile =>

  import self.profile.api._

  implicit val bigDecimalColumnType =
    MappedColumnType.base[BigDecimal, String](_.toString, BigDecimal.apply)

  implicit val bigIntColumnType =
    MappedColumnType.base[BigInt, String](_.toString, BigInt.apply)

  implicit val instantColumnType =
    MappedColumnType.base[Instant, Timestamp](Timestamp.from, _.toInstant)

  implicit val durationColumnType =
    MappedColumnType.base[Duration, Long](_.toNanos, Duration.fromNanos)

  implicit val javaDurationColumnType =
    MappedColumnType.base[java.time.Duration, Long](_.toNanos, java.time.Duration.ofNanos)
}

//noinspection TypeAnnotation
trait PlatformSlickImplicits {
  self: SlickProfile =>

  import self.profile.api._

  private val stringColumnType_ = implicitly[ColumnType[String]]

  implicit val partyColumnType =
    MappedColumnType.base[Party, String](_.unwrap, Party(_))

  implicit val apiContractIdColumnType =
    ContractId.subst[ColumnType, String](stringColumnType_)

  implicit def primitiveContractIdColumnType[T]: ColumnType[P.ContractId[T]] =
    P.ContractId.subst[ColumnType, T](apiContractIdColumnType)

  implicit val WorkflowIdColumnType =
    WorkflowId.subst[ColumnType, String](stringColumnType_)

}
