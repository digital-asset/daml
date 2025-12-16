def bootstrapSynchronizer(): Unit = {
  bootstrap.synchronizer(
    synchronizerName = "da",
    sequencers = Seq(sequencer1),
    mediators = Seq(mediator1),
    synchronizerOwners = Seq(sequencer1, mediator1),
    synchronizerThreshold = PositiveInt.two,
    staticSynchronizerParameters =
      StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
  )

  participant1.health.wait_for_initialized()

  participant1.synchronizers.connect_local(sequencer1, alias = "da")
  participant2.synchronizers.connect_local(sequencer1, alias = "da")
}

def runPing(): Unit = participant2.health.ping(participant1)
