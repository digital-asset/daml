-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
module DA.Daml.LF.Proto3.Decode
  ( Error(..)
  , decodePayload
  ) where

import DA.Prelude
import Da.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast (Package)
import DA.Daml.LF.Proto3.Error (Error(ParseError), Decode)
import qualified DA.Daml.LF.Proto3.DecodeV1 as DecodeV1

decodePayload :: ArchivePayload -> Decode Package
decodePayload payload = case archivePayloadSum payload of
    Just ArchivePayloadSumDamlLf0{} -> Left $ ParseError "Payload is DamlLf0"
    Just (ArchivePayloadSumDamlLf1 package) -> DecodeV1.decodePackage minor package
    Just ArchivePayloadSumDamlLfDev{} -> Left $ ParseError "LF Dev major no longer supported"
    Nothing -> Left $ ParseError "Empty payload"
    where
        minor = archivePayloadMinor payload
