-- |
-- = Protocol Buffers v3 for Haskell
--
-- This package defines tools for working with protocol buffers version 3 in
-- Haskell.
--
-- Specifically, it provides:
--
-- * Low-level functions for encoding and decoding messages
-- * Type classes for encoding and decoding messages, and instances for all
--   wire formats identified in the specification
-- * A higher-level approach to encoding and decoding, based on "GHC.Generics"
-- * A way of creating .proto files from Haskell types.
--
-- See the "Proto3.Suite.Tutorial" module for more details.
{-# LANGUAGE ExplicitNamespaces #-}

module Proto3.Suite
  (
  -- * Message Encoding/Decoding
    toLazyByteString
  , fromByteString
  , fromB64
  , Message(..)
  , MessageField(..)
  , Primitive(..)
  , HasDefault(..)
  , FieldNumber(..)
  , fieldNumber

  -- * Documentation
  , message
  , enum
  , RenderingOptions(..)
  , Named(..)
  , Finite(..)

  -- * Wire Formats
  , Fixed(..)
  , Signed(..)
  , Enumerated(..)
  , Nested(..)
  , UnpackedVec(..)
  , PackedVec(..)
  , NestedVec(..)
  , Commented(..)
  , type (//)()

  -- * AST
  , module DotProto
  ) where

import Proto3.Suite.Class
import Proto3.Suite.DotProto as DotProto
import Proto3.Suite.Types

import Proto3.Wire.Types
