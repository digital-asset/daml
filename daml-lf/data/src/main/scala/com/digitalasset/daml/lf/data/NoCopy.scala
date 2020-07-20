package com.daml.lf.data

  trait NoCopy {
    // prevents autogeneration of copy method in case class
    protected def copy(nothing: Nothing): Nothing = nothing
  }
