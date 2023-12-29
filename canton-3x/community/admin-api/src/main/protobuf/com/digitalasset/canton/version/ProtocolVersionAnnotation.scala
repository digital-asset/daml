package com.digitalasset.canton.version

object ProtocolVersionAnnotation {

  /** Type-level marker for whether a protocol version is stable */
  sealed trait Status

  /** Marker for unstable protocol versions */
  sealed trait Unstable extends Status

  /** Marker for stable protocol versions */
  sealed trait Stable extends Status
}

/** Marker trait for Protobuf messages generated by scalapb
  * that are used in some stable protocol versions
  *
  * Implements both [[com.digitalasset.canton.version.ProtocolVersionAnnotation.Stable]] and
  * [[com.digitalasset.canton.version.ProtocolVersionAnnotation.Unstable]] means that [[StableProtoVersion]]
  * messages can be used in stable and unstable protocol versions.
  */
trait StableProtoVersion
    extends ProtocolVersionAnnotation.Stable
    with ProtocolVersionAnnotation.Unstable

/** Marker trait for Protobuf messages generated by scalapb
  * that are used only unstable protocol versions
  */
trait UnstableProtoVersion extends ProtocolVersionAnnotation.Unstable

/** Marker trait for Protobuf messages generated by scalapb
  * that are used only to persist data in node storage.
  * These messages are never exchanged as part of a protocol.
  */
trait StorageProtoVersion
