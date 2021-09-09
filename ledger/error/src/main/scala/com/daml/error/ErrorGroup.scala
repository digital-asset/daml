package com.daml.error

abstract class ErrorGroup()(implicit parent: ErrorClass) {
  val docsHierarchy = ""
  implicit val errorClass: ErrorClass = parent.extend(getClass.getSimpleName.replace("$", ""))
}
