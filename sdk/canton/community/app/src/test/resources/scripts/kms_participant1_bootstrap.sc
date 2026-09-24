def runBootstrap(
    namespaceKmsKey: String,
    sequencerAuthKmsKey: String,
    signingKmsKey: String,
    encryptionKmsKey: String,
): Unit = {
  val namespaceKey = participant1.keys.secret.register_kms_signing_key(
    namespaceKmsKey,
    SigningKeyUsage.NamespaceOnly,
  )

  val sequencerAuthKey = participant1.keys.secret.register_kms_signing_key(
    sequencerAuthKmsKey,
    SigningKeyUsage.SequencerAuthenticationOnly,
  )

  val signingKey = participant1.keys.secret.register_kms_signing_key(
    signingKmsKey,
    SigningKeyUsage.ProtocolOnly,
  )

  val encryptionKey = participant1.keys.secret.register_kms_encryption_key(encryptionKmsKey)

  val namespace = Namespace(namespaceKey.id)
  participant1.topology.init_id_from_uid(
    UniqueIdentifier.tryCreate("manual-" + participant1.name, namespace)
  )

  participant1.topology.namespace_delegations.propose_delegation(
    namespace,
    namespaceKey,
    CanSignAllMappings,
  )

  participant1.topology.owner_to_key_mappings.add_keys(
    keys =
      Seq(sequencerAuthKey, signingKey, encryptionKey).map(key => (key.fingerprint, key.purpose))
  )
}
