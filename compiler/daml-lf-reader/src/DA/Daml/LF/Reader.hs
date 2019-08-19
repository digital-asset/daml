-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Reader
    ( Manifest(..)
    , ManifestData(..)
    , manifestFromDar
    , multiLineContent
    , getManifestField
    ) where

import "zip-archive" Codec.Archive.Zip
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.UTF8 as UTF8
import qualified Data.ByteString.Char8 as BSC
import qualified Data.HashMap.Strict as Map
import Data.List.Extra
import System.FilePath
import Safe

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

multiLineContent :: String -> [String]
multiLineContent = filter (not . null) . lines . replace "\n " ""

manifestMapToManifest :: Map.HashMap String String -> Manifest
manifestMapToManifest hash = Manifest mainDalf dependDalfs
    where
        mainDalf = Map.lookupDefault (error "no Main-Dalf entry in manifest") "Main-Dalf" hash
        dependDalfs = map trim $ delete mainDalf (splitOn "," (Map.lookupDefault (error "no Dalfs entry in manifest") "Dalfs" hash))

manifestDataFromDar :: Archive -> Manifest -> ManifestData
manifestDataFromDar archive manifest = ManifestData manifestDalfByte dependencyDalfBytes
    where
        manifestDalfByte = headNote "manifestDalfByte" [fromEntry e | e <- zEntries archive, ".dalf" `isExtensionOf` eRelativePath e  && eRelativePath e  == mainDalf manifest]
        dependencyDalfBytes = [fromEntry e | e <- zEntries archive, ".dalf" `isExtensionOf` eRelativePath e  && elem (trim (eRelativePath e))  (dalfs manifest)]

manifestFromDar :: Archive -> ManifestData
manifestFromDar dar = manifestDataFromDar dar manifest
    where
        manifestEntry = headNote "manifestEntry" [fromEntry e | e <- zEntries dar, ".MF" `isExtensionOf` eRelativePath e]
        manifestLines = multiLineContent $ UTF8.toString manifestEntry
        manifest = manifestMapToManifest $ Map.fromList $ map lineToKeyValue manifestLines

-- | Get a single field from the manifest zip entry.
getManifestField :: Entry -> String -> Maybe String
getManifestField manifest field =
    headMay
        [ value
        | l <-
              lines $
              replace "\n " "" $ BSC.unpack $ BSL.toStrict $ fromEntry manifest
              -- the newline replacement is for reassembling multilines
        , Just value <- [stripPrefix (field ++ ": ") l]
        ]
