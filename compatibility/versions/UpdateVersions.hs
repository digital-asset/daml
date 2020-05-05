-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Control.Concurrent.Async
import Control.Lens (view)
import Crypto.Hash (hashlazy, Digest, SHA256)
import Data.Aeson
import Data.ByteArray.Encoding (Base(Base16), convertToBase)
import Data.ByteString (ByteString)
import qualified Data.HashMap.Strict as HashMap
import Data.Map (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.SemVer (Version)
import qualified Data.SemVer as SemVer
import qualified Data.Text as T
import Network.HTTP.Simple
import Options.Applicative
import System.IO.Extra

newtype Versions = Versions { getVersions :: Set Version }
    deriving Show

instance Semigroup Versions where
    Versions a <> Versions b = Versions (a <> b)

instance FromJSON Versions where
    parseJSON = withObject "version object" $ \obj ->
        either fail (pure . Versions . Set.fromList) $
        traverse SemVer.fromText $ HashMap.keys obj

minimumVersion :: Version
minimumVersion = SemVer.incrementMajor SemVer.initial

headVersion :: Version
headVersion = SemVer.initial

-- We include this here so buildifier does not modify this file.
copyrightHeader :: [T.Text]
copyrightHeader =
    [ "# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved."
    , "# SPDX-License-Identifier: Apache-2.0"
    ]

renderVersionsFile :: Versions -> Map Version Checksums -> T.Text
renderVersionsFile (Versions (Set.toAscList -> versions)) checksums =
    T.unlines $ concat
        [ copyrightHeader
        , []
        , [ "sdk_versions = [" ]
        , map renderVersion (versions <> [headVersion])
        , [ "]"]
        , [ "platform_versions = [" ]
        , map renderVersion (versions <> [headVersion])
        , [ "]" ]
        , [ "stable_versions = [" ]
        , map renderVersion (stableVersions <> [headVersion])
        , [ "]" ]
        , [ "latest_stable_version = \"" <> SemVer.toText (last stableVersions) <> "\"" ]
        , [ "version_sha256s = {"]
        , concatMap renderChecksums (Map.toList checksums)
        , [ "}" ]
        ]
  where
    renderChecksums (ver, Checksums{..}) =
        [ "    \"" <> SemVer.toText ver <> "\": {"
        , "        \"linux\": " <> renderDigest linuxHash <> ","
        , "        \"macos\": " <> renderDigest macosHash <> ","
        , "        \"windows\": " <> renderDigest windowsHash <> ","
        , "        \"test_tool\": " <> renderDigest testToolHash <> ","
        , "    },"
        ]
    renderDigest digest = T.pack $ show (convertToBase Base16 digest :: ByteString)
    renderVersion ver = "    \"" <> SemVer.toText ver <> "\","
    stableVersions = filter (null . view SemVer.release) versions

data Opts = Opts
  { outputFile :: FilePath
  } deriving Show

data Checksums = Checksums
  { linuxHash :: Digest SHA256
  , macosHash :: Digest SHA256
  , windowsHash :: Digest SHA256
  , testToolHash :: Digest SHA256
  }

getChecksums :: Version -> IO Checksums
getChecksums ver = do
    putStrLn ("Requesting hashes for " <> SemVer.toString ver)
    [linuxHash, macosHash, windowsHash, testToolHash] <-
        forConcurrently [sdkUrl "linux", sdkUrl "macos", sdkUrl "windows", testToolUrl] $ \url -> do
        req <- parseRequestThrow url
        bs <- httpLbs req
        let !hash = hashlazy (getResponseBody bs)
        pure hash
    pure Checksums {..}
  where sdkUrl platform =
            "https://github.com/digital-asset/daml/releases/download/v" <>
            SemVer.toString ver <> "/" <>
            "daml-sdk-" <> SemVer.toString ver <> "-" <> platform <> ".tar.gz"
        testToolUrl =
            "https://repo1.maven.org/maven2/com/daml/ledger-api-test-tool/" <>
            SemVer.toString ver <> "/ledger-api-test-tool-" <> SemVer.toString ver <> ".jar"

optsParser :: Parser Opts
optsParser = Opts
  <$> strOption (short 'o' <> help "Path to output file")

main :: IO ()
main = do
    Opts{..} <- execParser (info optsParser fullDesc)
    versionsReq <- parseRequestThrow "https://docs.daml.com/versions.json"
    snapshotsReq <- parseRequestThrow "https://docs.daml.com/snapshots.json"
    versionsResp <- httpJSON versionsReq
    snapshotsResp <- httpJSON snapshotsReq
    let allVersions = Versions (Set.filter (>= minimumVersion) (getVersions (getResponseBody versionsResp <> getResponseBody snapshotsResp)))
    checksums <- mapM (\ver -> (ver,) <$> getChecksums ver) (Set.toList $ getVersions allVersions)
    writeFileUTF8 outputFile (T.unpack $ renderVersionsFile allVersions $ Map.fromList checksums)

