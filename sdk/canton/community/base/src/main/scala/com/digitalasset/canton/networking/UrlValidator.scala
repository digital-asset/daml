// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking

import cats.syntax.either.*

import java.net.URI
import scala.util.Try

object UrlValidator {

  sealed trait InvalidUrl extends Product with Serializable {
    def message: String
  }

  final case class InvalidFormat(url: String, cause: Throwable) extends InvalidUrl {
    override def message: String = s"Url `$url` is invalid: $cause"
  }
  final case class InvalidScheme(scheme: String) extends InvalidUrl {
    override def message: String = s"Scheme `$scheme` is invalid"
  }
  case object InvalidHost extends InvalidUrl {
    override def message: String = "Invalid host"
  }

  def validate(url: String): Either[InvalidUrl, URI] = {
    Try(new URI(url)).toEither
      .leftMap(InvalidFormat(url, _))
      .flatMap { url =>
        if (url.getScheme != "http" && url.getScheme != "https") {
          Left(InvalidScheme(url.getScheme))
        } else if (Option(url.getHost).forall(_.isEmpty)) Left(InvalidHost)
        else
          Right(url)
      }
  }

}
