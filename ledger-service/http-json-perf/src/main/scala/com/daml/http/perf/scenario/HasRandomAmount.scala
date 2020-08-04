package com.daml.http.perf.scenario

private[scenario] trait HasRandomAmount {
  private val rng = new scala.util.Random(123456789)

  // can be called from two different scenarios, need to synchronize access
  def randomAmount(): Int = {
    val x = this.synchronized { rng.nextInt(10) }
    x + 5 // [5, 15)
  }
}
