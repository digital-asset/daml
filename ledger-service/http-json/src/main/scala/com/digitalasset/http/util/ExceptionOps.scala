package com.digitalasset.http.util

import scalaz.Show

object ExceptionOps {

  implicit class ExceptionOps(val underlying: Throwable) extends AnyVal {
    def description: String = getDescription(underlying)
  }

  implicit val showInstance: Show[Throwable] = Show.shows(e => getDescription(e))

  def getDescription(e: Throwable): String = {
    val name: String = getClass.getName
    Option(e.getMessage).filter(_.nonEmpty) match {
      case Some(m) => s"$name: $m"
      case None => name
    }
  }
}
