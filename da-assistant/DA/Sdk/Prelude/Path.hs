-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}

module DA.Sdk.Prelude.Path
    ( module Filesystem.Path.CurrentOS
    , pathToText
    , textToPath
    , pathToString
    , stringToPath
    ) where

import qualified Data.Text                 as T
import           Filesystem.Path.CurrentOS hiding (null, concat, empty, (<.>), stripPrefix, currentOS)
import           Prelude                   hiding (FilePath)

-- | Convert a `FilePath` to a `T.Text`.
pathToText :: FilePath -> T.Text
pathToText = T.pack . encodeString

-- | Convert a `T.Text` to a `FilePath`.
textToPath :: T.Text -> FilePath
textToPath = decodeString . T.unpack

-- | Convert a `FilePath` to a `String`.
pathToString :: FilePath -> String
pathToString = encodeString

-- | Convert a `String` to a `FilePath`.
stringToPath :: String -> FilePath
stringToPath = decodeString
