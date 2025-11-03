-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{- This file parses stable-packages.yaml, passed in as YAML_FILE_PATH by Bazel via
 CPP. This list is used to solve the circular dependency between the encoder,
 which must know which Ids are stable, and the stable packages generator, which
 uses the encoder. See the documentation in stable-packages.yaml for further
 information. -}

{- HLINT ignore "Avoid restricted extensions" -}
{-# LANGUAGE CPP                #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveLift         #-}
{-# OPTIONS_GHC -Wno-orphans    #-}

module DA.Daml.EmbedFileWithDecoder(
    embedFile
  ) where

import           Data.ByteString      (ByteString)
import qualified Data.ByteString      as BS
import qualified Data.Text            as T
import qualified Data.Yaml            as Yaml

import "template-haskell" Language.Haskell.TH.Syntax as TH

-- import DA.Daml.LF.Ast
import DA.Daml.LF.Ast.Version.VersionType


decode :: ByteString -> [(Version, T.Text)]
decode bs = either (\ex -> error $ "Failed to decode: " ++ show ex) id decoded
  where
    decoded :: Either Yaml.ParseException [(Version, T.Text)]
    decoded = Yaml.decodeEither' bs

embedFileWithDecoder :: TH.Lift a => (ByteString -> a) -> TH.Q TH.Exp
embedFileWithDecoder decoder =
  (runIO $ BS.readFile YAML_FILE_PATH) >>= lift . decoder

deriving instance Lift MajorVersion
deriving instance Lift MinorVersion
deriving instance Lift Version

embedFile :: TH.Q TH.Exp
embedFile = embedFileWithDecoder decode
