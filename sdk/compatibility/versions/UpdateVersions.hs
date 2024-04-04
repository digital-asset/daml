-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import Data.Function ((&))
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
minimumVersion =  fromRight (error "Invalid version") $ SemVer.fromText "2.0.0"

headVersion :: Version
headVersion = SemVer.initial

-- We include this here so buildifier does not modify this file.
copyrightHeader :: [T.Text]
copyrightHeader =
    [ "# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved."
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
        , [ "latest_stable_version = \"" <> SemVer.toText (last stableVersions) <> "\"" ]
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
        , "        \"test_tool\": " <> renderDigest testToolHash <> ","
        , "        \"daml_types\": " <> renderDigest damlTypesHash <> ","
        , "        \"daml_ledger\": " <> renderDigest damlLedgerHash <> ","
        , "        \"daml_react\": " <> renderDigest damlReactHash <> ","
        , "        \"create_daml_app_patch\": " <> renderDigest createDamlAppPatchHash <> ","
        ]
      , [ "    }," ]
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
  , damlTypesHash :: Digest SHA256
  , damlLedgerHash :: Digest SHA256
  , damlReactHash :: Digest SHA256
  , createDamlAppPatchHash :: Digest SHA256
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
    [ testToolHash, damlTypesHash, damlLedgerHash, damlReactHash] <-
        forConcurrently
            [ testToolUrl
            , tsLib "types"
            , tsLib "ledger"
            , tsLib "react"
            ] getHash
    createDamlAppPatchHash <- getHash createDamlAppUrl
    pure Checksums {..}
  where sdkFilePath platform = T.pack $
            "./daml-sdk-" <> SemVer.toString ver <> "-" <> platform <> ".tar.gz"
        sha256Url =
            "https://github.com/digital-asset/daml/releases/download/v" <>
            SemVer.toString ver <> "/sha256sums"
        testToolUrl =
            "https://repo1.maven.org/maven2/com/daml/ledger-api-test-tool/" <>
            SemVer.toString ver <> "/ledger-api-test-tool-" <> SemVer.toString ver <> ".jar"
        tsLib name =
            "https://registry.npmjs.org/@daml/" <> name <>
            "/-/" <> name <> "-" <> SemVer.toString ver <> ".tgz"
        createDamlAppUrl =
          -- TODO: remove condition when 2.7 and 2.8 have post-subdir releases
            if getMinor ver `elem` ["2.7", "2.8"]  then
               "https://raw.githubusercontent.com/digital-asset/daml/v" <> SemVer.toString ver
               <> "/templates/create-daml-app-test-resources/messaging.patch"
            else
               "https://raw.githubusercontent.com/digital-asset/daml/v" <> SemVer.toString ver
               <> "/sdk/templates/create-daml-app-test-resources/messaging.patch"
        getHash url = do
          req <- parseRequestThrow url
          bs <- httpLbs req { responseTimeout = responseTimeoutMicro (60 * 10 ^ (6 :: Int) ) }
          let !hash = hashlazy (getResponseBody bs)
          pure hash

optsParser :: Parser Opts
optsParser = Opts
  <$> strOption (short 'o' <> help "Path to output file")

getMinor :: Version -> String
getMinor v = show (view SemVer.major v) <> "." <> show (view SemVer.minor v)

-- removelist approach because this list has to be maintained manually and
-- forgetting to remove is better than forgetting to add
unsupportedMinors :: Set String
unsupportedMinors = Set.fromList ["2.0", "2.1", "2.2", "2.4", "2.5", "2.6"]

getVersionsFromTags :: IO (Set Version)
getVersionsFromTags = do
    tags <- lines <$> System.Process.readProcess "git" ["tag"] ""
    return $ tags
           & mapMaybe (fmap (SemVer.fromText . T.pack) . stripPrefix "v")
           & rights
           & filter (null . view SemVer.release)
           & latestPatchVersions
           & filter (\v -> Set.notMember (getMinor v) unsupportedMinors)
           & Set.fromList

-- | Given a set of versions filter it to those that are the latest patch release in a given
-- major.minor series.
latestPatchVersions :: [Version] -> [Version]
latestPatchVersions allVersions =
    filter (\version -> all (f version) allVersions) allVersions
  where
    f this that = toMajorMinor this /= toMajorMinor that || view SemVer.patch this >= view SemVer.patch that
    toMajorMinor v = (view SemVer.major v, view SemVer.minor v)

main :: IO ()
main = do
    Opts{..} <- execParser (info optsParser fullDesc)
    stableVers <- getVersionsFromTags
    let allVersions = Versions $ Set.filter (>= minimumVersion) stableVers
    checksums <- mapM (\ver -> (ver,) <$> getChecksums ver) (Set.toList $ getVersions allVersions)
    writeFileUTF8 outputFile (T.unpack $ renderVersionsFile allVersions $ Map.fromList checksums)
