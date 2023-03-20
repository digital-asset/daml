// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

// Reference: https://gist.github.com/darkfrog26/60a42e8e803ab84dce915245dafb1c03

object Regression {

  final case class Data(x: Double, y: Double) {
    def this(x: Long, y: FiniteDuration) = {
      this(x.toDouble, y.toNanos.toDouble)
    }

    def this(x: Double, y: FiniteDuration) = {
      this(x, y.toNanos.toDouble)
    }
  }

  final case class Model(gradient: Double, intercept: Double, fitProbability: Double) {
    def apply(value: Double): Double = gradient * value + intercept

    override def toString: String = s"gradient: $gradient, intercept: $intercept, R^2: $fitProbability"
  }

  def linear(pairs: Seq[Data])(implicit ec: ExecutionContext): Future[Model] = for {
    x1 <- Future(pairs.foldLeft(0.0)((sum, d) => sum + d.x))
    y1 <- Future(pairs.foldLeft(0.0)((sum, d) => sum + d.y))
    x2 <- Future(pairs.foldLeft(0.0)((sum, d) => sum + math.pow(d.x, 2.0)))
    y2 <- Future(pairs.foldLeft(0.0)((sum, d) => sum + math.pow(d.y, 2.0)))
    xy <- Future(pairs.foldLeft(0.0)((sum, d) => sum + d.x * d.y))
    size = pairs.size
    dn = size * x2 - math.pow(x1, 2.0)
    gradient <- Future {
      ((size * xy) - (x1 * y1)) / dn
    }
    intercept <- Future {
      ((y1 * x2) - (x1 * xy)) / dn
    }
    t1 <- Future {
      ((size * xy) - (x1 * y1)) * ((size * xy) - (x1 * y1))
    }
    t2 <- Future {
      (size * x2) - math.pow(x1, 2)
    }
    t3 <- Future {
      (size * y2) - math.pow(y1, 2)
    }
  } yield {
    assert(dn != 0.0, "Can't solve the system!")

    if (t2 * t3 != 0.0) {
      Model(gradient, intercept, t1 / (t2 * t3))
    } else {
      Model(gradient, intercept, 0.0)
    }
  }
}
