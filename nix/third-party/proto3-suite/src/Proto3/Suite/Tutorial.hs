-- |
-- = Tutorial
--
-- >>> :set -XOverloadedStrings
--
-- This module contains a worked example of encoding and decoding messages,
-- and exporting a corresponding .proto file from Haskell types.
--
-- == Setup
--
-- If you are using "GHC.Generics", you should enable the generic deriving
-- extension, and import the main module:
--
-- > {-# LANGUAGE DeriveGeneric #-}
--
-- > import Proto3.Suite
-- > import GHC.Generics
--

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE OverloadedStrings #-}

module Proto3.Suite.Tutorial where

import Data.Int (Int32)
import Proto3.Suite (Enumerated, Nested, NestedVec, PackedVec,
                     Message, Named, Finite,
                     DotProtoDefinition, enum, message, packageFromDefs, toProtoFileDef)
import Data.Proxy
import Data.Word (Word32)
import GHC.Generics

-- |
-- == Defining Message Types
--
-- Define messages using Haskell record types. You can use any 'MessageField' types
-- in your records, and the correct serializer and deserializer will be generated
-- for you.
--
-- Make sure to derive a 'Generic' instance for your type, and then derive instances
-- for 'Message' and 'Named' using the default (empty) instances:
--
-- > instance Message Foo
-- > instance Named Foo
--
-- == Encoding Messages
--
-- Now we can encode a value of type 'Foo' using 'Proto3.Suite.toLazyByteString'.
--
-- For example:
--
-- >>> Proto3.Suite.toLazyByteString (Foo 42 (Proto3.Suite.PackedVec (pure 123)))
-- "\b*\DC2\SOH{"
--
-- We can also decode messages using `fromByteString`:
--
-- >>> Proto3.Suite.fromByteString "\b*\DC2\SOH{" :: Either [Proto3.Suite..Decode.Parser.ParseError] Foo
-- Right (Foo {fooX = 42, fooY = PackedVec {packedvec = [123]}})
data Foo = Foo
  { fooX :: Word32
  , fooY :: PackedVec Int32
  } deriving (Show, Eq, Generic)

instance Message Foo
instance Named Foo

-- |
-- == Nested Messages
--
-- Messages can contain other messages, by using the 'Nested' constructor, and
-- lists of nested messages using the 'NestedVec constructor'.
data Bar = Bar
  { barShape :: Enumerated Shape
  , barFoo   :: Nested Foo
  , foos     :: NestedVec Foo
  }
  deriving (Eq, Generic)

instance Message Bar
instance Named Bar

-- |
-- == Enumerations
--
-- Enumerated types can be used by deriving the 'Enum', 'Finite' and 'Named'
-- classes. Each of these instances are implied by a 'Generic' instance, so can
-- be derived as follows:
--
-- > data Shape
-- >   = Circle
-- >   | Square
-- >   | Triangle
-- >   deriving (Generic, Enum, Finite, Named)
data Shape
  = Circle
  | Square
  | Triangle
  deriving (Bounded, Eq, Enum, Finite, Generic, Named, Ord)

-- |
-- == Generating a .proto file
--
-- We can generate a .proto file for the 'Foo' and 'Bar' data types by
-- using the 'toProtoFileDef' function. We have to provide a package name, and
-- explicitly list the message and enumeration types as a 'DotProto' value.
--
-- >>> putStrLn protoFile
-- syntax = "proto3";
-- package examplePackageName;
-- enum Shape {
--   Circle = 0;
--   Square = 1;
--   Triangle = 2;
-- }
-- message Foo {
--   uint32 fooX = 1;
--   repeated int32 fooY = 2 [packed=true];
-- }
-- message Bar {
--   Shape barShape = 1;
--   Foo barFoo = 2;
--   repeated Foo foos = 3 [packed=false];
-- }

protoFile :: String
protoFile = toProtoFileDef $ packageFromDefs "examplePackageName"
  ([ enum    (Proxy :: Proxy Shape)
   , message (Proxy :: Proxy Foo)
   , message (Proxy :: Proxy Bar)
   ] :: [DotProtoDefinition])
