-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Control.Concurrent.Async
import Control.Lens ((.~), (&), (^?!), view, _Right)
import Control.Monad
import Crypto.Hash (digestFromByteString, hashlazy, Digest, SHA256)
import Data.Aeson
import Data.ByteArray.Encoding (Base(Base16), convertFromBase, convertToBase)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BSL
import Data.Either (fromRight)
import Data.Either.Extra (eitherToMaybe)
import qualified Data.HashMap.Strict as HashMap
import Data.List
import Data.Map (Map)
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
        ]
      , [ "        \"create_daml_app_patch\": " <> renderDigest hash <> ","
        | Just hash <- [mbCreateDamlAppPatchHash]
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
  , mbCreateDamlAppPatchHash :: Maybe (Digest SHA256)
  -- ^ Nothing for older versions
  }

-- | The messaging patch wasn’t included in 1.0.0 directly
-- but only added later.
-- However, the code did not change and we can apply
-- the later patch on the older versions.
-- Therefore we fallback to using the patch from this version
-- for releases before this one.
firstMessagingPatch :: Version
firstMessagingPatch =
    fromRight (error "Invalid version") $
    SemVer.fromText "1.1.0-snapshot.20200422.3991.0.6391ee9f"

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
    mbCreateDamlAppPatchHash <- traverse getHash mbCreateDamlAppUrl
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
        mbCreateDamlAppUrl
          | ver >= firstMessagingPatch =
             Just $
               "https://raw.githubusercontent.com/digital-asset/daml/v" <> SemVer.toString ver
               <> "/templates/create-daml-app-test-resources/messaging.patch"
          | otherwise = Nothing
        getHash url = do
          req <- parseRequestThrow url
          bs <- httpLbs req { responseTimeout = responseTimeoutMicro (60 * 10 ^ (6 :: Int) ) }
          let !hash = hashlazy (getResponseBody bs)
          pure hash

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
    let stableVers = getVersions (getResponseBody versionsResp)
    let allSnapshots = getVersions (getResponseBody snapshotsResp)
    -- List of releases that we want to filter out snapshots for even
    -- though they do not exist.
    let skipped = Set.fromList [SemVer.fromText "1.1.0" ^?! _Right]
    -- Only include snapshots for which there is no following stable version.
    -- We do not simply filter for anything > than the latest stable version
    -- since we might have a snapshot for a bugfix release.
    let prunedSnapshots = Set.filter (\v -> toStable v `Set.notMember` (stableVers <> skipped)) allSnapshots
    let allVersions = Versions (Set.filter (>= minimumVersion) (stableVers <> prunedSnapshots))
    checksums <- mapM (\ver -> (ver,) <$> getChecksums ver) (Set.toList $ getVersions allVersions)
    writeFileUTF8 outputFile (T.unpack $ renderVersionsFile allVersions $ Map.fromList checksums)
  where toStable v = v & SemVer.release .~ [] & SemVer.metadata .~ []
