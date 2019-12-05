-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Reader
    ( parseManifestFile
    , readManifest
    , manifestPath
    , DalfManifest(..)
    , Dalfs(..)
    , readDalfManifest
    , readDalfs
    , stripPkgId
    , parseUnitId
    ) where

import "zip-archive" Codec.Archive.Zip
import Control.Monad (guard)
import qualified DA.Daml.LF.Ast as LF
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.UTF8 as BSUTF8
import Data.Char
import Data.List.Extra
import Data.Maybe
import qualified Data.Text as T
import Data.Void
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
    } deriving (Show)

-- | The dalfs stored in the DAR.
data Dalfs = Dalfs
    { mainDalf :: BSL.ByteString
    , dalfs :: [BSL.ByteString]
    -- ^ Excludes the mainDalf.
    } deriving (Show)

readDalfManifest :: Archive -> Either String DalfManifest
readDalfManifest dar = do
    attrs <- readManifest dar
    mainDalf <- getAttr "Main-Dalf" attrs
    dalfPaths <- splitOn ", " <$> getAttr "Dalfs" attrs
    sdkVersion <- getAttr "Sdk-Version" attrs
    pure $ DalfManifest mainDalf dalfPaths sdkVersion
  where
    getAttr :: ByteString -> [(ByteString, ByteString)] -> Either String String
    getAttr attrName attrs =
        maybe (missingAttr attrName) (Right . BSUTF8.toString) $
        lookup attrName attrs
    missingAttr attrName = Left $ "No " <> BSUTF8.toString attrName <> " attribute in manifest."

readDalfs :: Archive -> Either String Dalfs
readDalfs dar = do
    DalfManifest{..} <- readDalfManifest dar
    mainDalf <- getEntry dar mainDalfPath
    dalfs <- mapM (getEntry dar) (delete mainDalfPath dalfPaths)
    pure $ Dalfs mainDalf dalfs

getEntry :: Archive -> FilePath -> Either String BSL.ByteString
getEntry dar path = case findEntryByPath path dar of
    Nothing -> Left $ "Could not find " <> path <> " in DAR"
    Just entry -> Right $ fromEntry entry

-- Strip the package id from the end of a dalf file name
-- TODO (drsk) This needs to become a hard error
stripPkgId :: String -> String -> Maybe String
stripPkgId baseName expectedPkgId = do
    (unitId, pkgId) <- stripInfixEnd "-" baseName
    guard $ pkgId == expectedPkgId
    pure unitId

-- Get the unit id of a string, given an expected package id of the package, by stripping the
-- package id from the back. I.e. if 'package-name-123abc' is given and the known package id is
-- '123abc', then 'package-name' is returned as unit id.
parseUnitId :: String -> LF.PackageId -> String
parseUnitId name pkgId =
    fromMaybe name $ stripPkgId name $ T.unpack $ LF.unPackageId pkgId
