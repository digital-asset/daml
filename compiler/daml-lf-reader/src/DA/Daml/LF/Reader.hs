-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Reader
    ( Manifest(..)
    , ManifestData(..)
    , manifestFromDar
    ) where

import Codec.Archive.Zip
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.UTF8 as UTF8
import qualified Data.HashMap.Strict as Map
import Data.List.Extra
import System.FilePath

data Manifest = Manifest
    { mainDalf :: FilePath
    , dalfs :: [FilePath]
    } deriving (Show)

data ManifestData = ManifestData
    { mainDalfContent :: BSL.ByteString
    , dalfsContent :: [BSL.ByteString]
    } deriving (Show)

lineToKeyValue :: String -> (String, String)
lineToKeyValue line = case splitOn ":" line of
    [l, r] -> (trim l , trim r)
    _ -> error $ "Expected two fields in line " <> line

manifestMapToManifest :: Map.HashMap String String -> Manifest
manifestMapToManifest hash = Manifest mainDalf dependDalfs
    where
        mainDalf = Map.lookupDefault "unknown" "Main-Dalf" hash
        dependDalfs = map trim $ delete mainDalf (splitOn "," (Map.lookupDefault "unknown" "Dalfs" hash))

manifestDataFromDar :: Archive -> Manifest -> ManifestData
manifestDataFromDar archive manifest = ManifestData manifestDalfByte dependencyDalfBytes
    where
        manifestDalfByte = head [fromEntry e | e <- zEntries archive, ".dalf" `isExtensionOf` eRelativePath e  && eRelativePath e  == mainDalf manifest]
        dependencyDalfBytes = [fromEntry e | e <- zEntries archive, ".dalf" `isExtensionOf` eRelativePath e  && elem (trim (eRelativePath e))  (dalfs manifest)]

manifestFromDar :: Archive -> ManifestData
manifestFromDar dar =  manifestDataFromDar dar manifest
    where
        manifestEntry = head [fromEntry e | e <- zEntries dar, ".MF" `isExtensionOf` eRelativePath e]
        linesStr = lines $ UTF8.toString manifestEntry
        manifest = manifestMapToManifest $ Map.fromList $ map lineToKeyValue (filter (\a -> a /= "" ) linesStr)

