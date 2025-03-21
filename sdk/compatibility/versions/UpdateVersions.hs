-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Control.Concurrent.Async
import Control.Lens (view)
import Control.Monad
import Crypto.Hash (digestFromByteString, hashlazy, Digest, SHA256)
import Data.ByteArray.Encoding (Base(Base16), convertFromBase, convertToBase)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BSL
import Data.Either (fromRight, rights)
import Data.Either.Extra (eitherToMaybe)
import Data.List
import Data.Map (Map)
import Data.Maybe (mapMaybe)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.SemVer (Version)
import qualified Data.SemVer as SemVer
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Network.HTTP.Client (responseTimeout, responseTimeoutMicro)
import Network.HTTP.Simple
import Options.Applicative
import System.IO.Extra
import qualified System.Process

newtype Versions = Versions { getVersions :: Set Version }
    deriving Show

instance Semigroup Versions where
    Versions a <> Versions b = Versions (a <> b)

minimumVersion :: Version
minimumVersion =  fromRight (error "Invalid version") $ SemVer.fromText "3.0.0"

headVersion :: Version
headVersion = SemVer.initial

-- We include this here so buildifier does not modify this file.
copyrightHeader :: [T.Text]
copyrightHeader =
    [ "# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved."
    , "# SPDX-License-Identifier: Apache-2.0"
    ]

renderVersionsFile :: Versions -> Map Version Checksums -> T.Text
renderVersionsFile (Versions (Set.toAscList -> versions)) checksums =
    T.unlines $ concat
        [ copyrightHeader
        , [ "# This file is autogenerated and should not be modified manually."
          , "# Update versions/UpdateVersions.hs instead."
          ]
        , [ "sdk_versions = [" ]
        , map renderVersion (versions <> [headVersion])
        , [ "]"]
        , [ "platform_versions = [" ]
        , map renderVersion (versions <> [headVersion])
        , [ "]" ]
        , [ "stable_versions = [" ]
        , map renderVersion (stableVersions <> [headVersion])
        , [ "]" ]
        , [ "latest_stable_version = \"" <> SemVer.toText latestVersion <> "\"" ]
        , [ "version_sha256s = {"]
        , concatMap renderChecksums (Map.toList checksums)
        , [ "}" ]
        ]
  where
    renderChecksums (ver, Checksums{..}) = concat
      [ [ "    \"" <> SemVer.toText ver <> "\": {"
        , "        \"linux\": " <> renderDigest linuxHash <> ","
        , "        \"macos\": " <> renderDigest macosHash <> ","
        , "        \"windows\": " <> renderDigest windowsHash <> ","
        , "        \"daml_types\": " <> renderDigest damlTypesHash <> ","
        , "        \"daml_ledger\": " <> renderDigest damlLedgerHash <> ","
        , "        \"daml_react\": " <> renderDigest damlReactHash <> ","
        ]
      , [ "    }," ]
      ]
    renderDigest digest = T.pack $ show (convertToBase Base16 digest :: ByteString)
    renderVersion ver = "    \"" <> SemVer.toText ver <> "\","
    stableVersions = filter (null . view SemVer.release) versions
    latestVersion = if null stableVersions then SemVer.version 2 8 0 [] [] else last stableVersions

data Opts = Opts
  { outputFile :: FilePath
  } deriving Show

data Checksums = Checksums
  { linuxHash :: Digest SHA256
  , macosHash :: Digest SHA256
  , windowsHash :: Digest SHA256
  , damlTypesHash :: Digest SHA256
  , damlLedgerHash :: Digest SHA256
  , damlReactHash :: Digest SHA256
  -- ^ Nothing for older versions
  }

getChecksums :: Version -> IO Checksums
getChecksums ver = do
    putStrLn ("Requesting hashes for " <> SemVer.toString ver)
    req <- parseRequestThrow sha256Url
    lines <- map T.words . T.lines . T.decodeUtf8 . BSL.toStrict . getResponseBody <$> httpLbs req
    Just [linuxHash, macosHash, windowsHash] <- pure $
        forM [sdkFilePath "linux", sdkFilePath "macos", sdkFilePath "windows"] $ \path -> do
            -- This is fairly hacky but given that we only run this script
            -- offline that seems fine for now.
            (base16Hash : _) <- find (\line -> path == line !! 1) lines
            byteHash <- (eitherToMaybe . convertFromBase Base16 . T.encodeUtf8) base16Hash
            digestFromByteString @SHA256 @ByteString byteHash
    [ damlTypesHash, damlLedgerHash, damlReactHash] <-
        forConcurrently
            [ tsLib "types"
            , tsLib "ledger"
            , tsLib "react"
            ] getHash
    pure Checksums {..}
  where sdkFilePath platform = T.pack $
            "./daml-sdk-" <> SemVer.toString ver <> "-" <> platform <> ".tar.gz"
        sha256Url =
            "https://github.com/digital-asset/daml/releases/download/v" <>
            SemVer.toString ver <> "/sha256sums"
        tsLib name =
            "https://registry.npmjs.org/@daml/" <> name <>
            "/-/" <> name <> "-" <> SemVer.toString ver <> ".tgz"
        getHash url = do
          req <- parseRequestThrow url
          bs <- httpLbs req { responseTimeout = responseTimeoutMicro (60 * 10 ^ (6 :: Int) ) }
          let !hash = hashlazy (getResponseBody bs)
          pure hash

optsParser :: Parser Opts
optsParser = Opts
  <$> strOption (short 'o' <> help "Path to output file")

getVersionsFromTags :: IO (Set Version)
getVersionsFromTags = do
    tags <- lines <$> System.Process.readProcess "git" ["tag"] ""
    let versions = Set.fromList $ rights $ mapMaybe (fmap (SemVer.fromText . T.pack) . stripPrefix "v") tags
    return $ latestPatchVersions $ Set.filter (null . view SemVer.release) versions

-- | Given a set of versions filter it to those that are the latest patch release in a given
-- major.minor series.
latestPatchVersions :: Set Version -> Set Version
latestPatchVersions allVersions =
    Set.filter (\version -> all (f version) allVersions) allVersions
  where
    f this that = toMajorMinor this /= toMajorMinor that || view SemVer.patch this >= view SemVer.patch that
    toMajorMinor v = (view SemVer.major v, view SemVer.minor v)

additionalVersions :: Set Version
additionalVersions = Set.fromList [
  ]

main :: IO ()
main = do
    Opts{..} <- execParser (info optsParser fullDesc)
    stableVers <- getVersionsFromTags
    let filterVersions = Set.filter (>= minimumVersion) stableVers
    let allVersions = Versions (Set.union filterVersions additionalVersions)
    checksums <- mapM (\ver -> (ver,) <$> getChecksums ver) (Set.toList $ getVersions allVersions)
    writeFileUTF8 outputFile (T.unpack $ renderVersionsFile allVersions $ Map.fromList checksums)
