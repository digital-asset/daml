-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Reader
    ( parseManifestFile
    , readManifest
    , manifestPath
    , DalfManifest(..)
    , Dalfs(..)
    , readDalfManifest
    , readDalfs
    , readDalfsWithMeta
    , dalfsToList
    ) where

import "zip-archive" Codec.Archive.Zip
import qualified DA.Daml.LF.Ast as LF
import Data.Bifunctor (second)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.UTF8 as BSUTF8
import Data.Char
import Data.Either.Extra
import Data.List.Extra
import qualified Data.Text as T
import Data.Void
import System.FilePath (takeBaseName)
import Text.Megaparsec
import Text.Megaparsec.Byte

type Parser = Parsec Void ByteString

parseManifestFile :: ByteString -> Either String [(ByteString, ByteString)]
parseManifestFile bs = case parse manifestParser "MANIFEST.MF" bs of
    Left errBundle -> Left $ errorBundlePretty errBundle
    Right r -> Right r

readManifest :: Archive -> Either String [(ByteString, ByteString)]
readManifest dar = do
    entry <- getEntry dar manifestPath
    parseManifestFile $ BSL.toStrict entry

manifestPath :: FilePath
manifestPath = "META-INF/MANIFEST.MF"

-- | We try to be fairly lenient in our parser, e.g., we do not enforce that
-- lines abide to the 72 byte limit.
--
-- See
-- https://docs.oracle.com/javase/7/docs/technotes/guides/jar/jar.html#JAR_Manifest
-- for a description of the format.
manifestParser :: Parser [(ByteString, ByteString)]
manifestParser = do
    xs <- many manifestHeaderParser
    _ <- many eol
    pure xs

manifestHeaderParser :: Parser (ByteString, ByteString)
manifestHeaderParser = do
    name <- nameParser
    _ <- chunk ": "
    value <- valueParser
    pure (name, value)

nameParser :: Parser ByteString
nameParser = do
    x <- alphaNumChar
    xs <- takeWhileP Nothing isHeaderChar
    pure $! BS.cons x xs
    where isHeaderChar x =
              -- isAlphaNum will also match non-ASCII chars but since we get it by applying chr
              -- to a single byte that is not an issue.
              let xChr = chr $ fromIntegral x in isAlphaNum xChr || xChr == '-' || xChr == '_'

valueParser :: Parser ByteString
valueParser = do
     xs <- takeWhileP Nothing isOtherChar
     _ <- eol
     xss <- many continuation
     pure $! BS.concat (xs : xss)
  where isOtherChar x = x `notElem` [fromIntegral $ ord x | x <- ['\n', '\r', '\0']]
        continuation = do
            _ <- char $ fromIntegral $ ord ' '
            xs <- takeWhileP Nothing isOtherChar
            _ <- eol
            pure xs

-- | The entries from the MANIFEST.MF file relating to .dalf files.
data DalfManifest = DalfManifest
    { mainDalfPath :: FilePath
    , dalfPaths :: [FilePath]
    -- ^ Includes the mainDalf.
    , sdkVersion :: String
    , packageName :: Maybe String
    } deriving (Show)

-- | The dalfs stored in the DAR.
data Dalfs a = Dalfs
    { mainDalf :: a
    , dalfs :: [a]
    -- ^ Excludes the mainDalf.
    } deriving (Eq, Ord, Show, Functor)

dalfsToList :: Dalfs a -> [a]
dalfsToList d = mainDalf d : dalfs d

readDalfManifest :: Archive -> Either String DalfManifest
readDalfManifest dar = do
    attrs <- readManifest dar
    mainDalf <- getAttr "Main-Dalf" attrs
    dalfPaths <- splitOn ", " <$> getAttr "Dalfs" attrs
    sdkVersion <- getAttr "Sdk-Version" attrs
    let mbName = eitherToMaybe (getAttr "Name" attrs)
    pure $ DalfManifest mainDalf dalfPaths sdkVersion mbName
  where
    getAttr :: ByteString -> [(ByteString, ByteString)] -> Either String String
    getAttr attrName attrs =
        maybe (missingAttr attrName) (Right . BSUTF8.toString) $
        lookup attrName attrs
    missingAttr attrName = Left $ "No " <> BSUTF8.toString attrName <> " attribute in manifest."

readDalfs :: Archive -> Either String (Dalfs BSL.ByteString)
readDalfs dar = do
    DalfManifest{..} <- readDalfManifest dar
    mainDalf <- getEntry dar mainDalfPath
    dalfs <- mapM (getEntry dar) (delete mainDalfPath dalfPaths)
    pure $ Dalfs mainDalf dalfs

extractNameAndPackageIdFromPath :: FilePath -> (T.Text, LF.PackageId)
extractNameAndPackageIdFromPath = second LF.PackageId . T.breakOnEnd "-" . T.pack . takeBaseName

readDalfsWithMeta :: Archive -> Either String (Dalfs (T.Text, BSL.ByteString, LF.PackageId))
readDalfsWithMeta dar = do
    DalfManifest{..} <- readDalfManifest dar
    let getEntryWithMeta dar path = do
          dalf <- getEntry dar path
          let (name, pkgId) = extractNameAndPackageIdFromPath path
          pure (name, dalf, pkgId)
    mainDalf <- getEntryWithMeta dar mainDalfPath
    dalfs <- mapM (getEntryWithMeta dar) (delete mainDalfPath dalfPaths)
    pure $ Dalfs mainDalf dalfs

getEntry :: Archive -> FilePath -> Either String BSL.ByteString
getEntry dar path = case findEntryByPath path dar of
    Nothing -> Left $ "Could not find " <> path <> " in DAR"
    Just entry -> Right $ fromEntry entry
