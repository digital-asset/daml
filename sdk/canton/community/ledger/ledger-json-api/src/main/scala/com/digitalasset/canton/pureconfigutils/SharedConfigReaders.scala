// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pureconfigutils

import pureconfig.error.{CannotConvert, FailureReason}

import java.nio.file.Path

final case class HttpServerConfig(
    address: String = com.digitalasset.canton.cliopts.Http.defaultAddress,
    port: Option[Int] = None,
    portFile: Option[Path] = None,
)

object SharedConfigReaders {

  def catchConvertError[A, B](f: String => Either[String, B])(implicit
      B: reflect.ClassTag[B]
  ): String => Either[FailureReason, B] =
    s => f(s).left.map(CannotConvert(s, B.toString, _))

}
