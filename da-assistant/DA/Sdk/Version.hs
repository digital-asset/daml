-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Semantic and build versions.
module DA.Sdk.Version
  ( SemVersion(..)
  , showSemVersion
  , parseSemVersion
  , showSemVersionCompatable
  , semVersionParser
  , BuildVersion(..)
  , showBuildVersion
  , parseBuildVersion
  , Versioned(..)
  ) where

import           Control.Monad        (void)
import           Control.Applicative
import qualified DA.Sdk.Pretty        as P
import qualified Data.Aeson.Types     as Aeson
import           Data.Attoparsec.Text
import           Data.Char            (isAlpha, isDigit)
import           Data.Hashable        (Hashable (..))
import           Data.Monoid          ((<>))
import qualified Data.Text.Extended   as T
import qualified Data.Yaml            as Yaml
import           Database.SQLite.Simple           hiding (Error)
import           Database.SQLite.Simple.Internal
import           Database.SQLite.Simple.FromField
import           Database.SQLite.Simple.Ok
import           Database.SQLite.Simple.ToField
import           Turtle.Format        (d, format, s, (%))
import           Web.HttpApiData      (FromHttpApiData (parseUrlPiece), ToHttpApiData (toUrlPiece))

--------------------------------------------------------------------------------
-- Data Types
--------------------------------------------------------------------------------

data SemVersion = SemVersion
    { _svMajor      :: Int
    , _svMinor      :: Int
    , _svPatch      :: Int
    , _svPreRelease :: Maybe T.Text
    }
    deriving (Show, Eq)

instance Hashable SemVersion where
    hashWithSalt i SemVersion {..} = hashWithSalt i (_svMajor, _svMinor, _svPatch, _svPreRelease)

instance Ord SemVersion where
    compare (SemVersion ma1 mi1 pa1 pr1) (SemVersion ma2 mi2 pa2 pr2) =
        compare (ma1, mi1, pa1, pr1) (ma2, mi2, pa2, pr2)

data BuildVersion = BuildVersion Int T.Text
                    | HeadVersion
    deriving (Show, Eq)

instance Ord BuildVersion where
    compare (BuildVersion a _) (BuildVersion b _) = compare a b
    compare HeadVersion HeadVersion = EQ
    compare HeadVersion _ = GT
    compare _ HeadVersion = LT


--------------------------------------------------------------------------------
-- Semantic Version
--------------------------------------------------------------------------------

-- | Format a semantic version according to spec.
showSemVersion :: SemVersion -> T.Text
showSemVersion (SemVersion major minor patch mbPreRelease) =
    case mbPreRelease of
        Just preRelease ->
            format (d%"."%d%"."%d%"-"%s) major minor patch preRelease
        Nothing ->
            format (d%"."%d%"."%d) major minor patch

-- | Parse a semantic version.
-- | Format a semantic version according to spec compatable for new daml-assitant.
showSemVersionCompatable :: SemVersion -> T.Text
showSemVersionCompatable (SemVersion _ minor patch mbPreRelease) =
    case mbPreRelease of
        Just preRelease ->
            format (s%"-"%d%"."%d%"-"%s) "nightly-100" minor patch preRelease
        Nothing ->
            format (s%"."%d%"."%d) "nightly-100" minor patch

-- Note: Versions of the form "0.8" will get parsed as version "0.8.0".
parseSemVersion :: T.Text -> Maybe SemVersion
parseSemVersion versionText = case parseOnly semVersionParser versionText of
  Left _        -> Nothing
  Right version -> Just version

semVersionParser :: Parser SemVersion
semVersionParser = do
    major <- decimal
    minor <- option 0 $ do
        void $ char '.'
        decimal
    patch <- option 0 $ do
        void $ char '.'
        decimal
    mbPreRelease <-
        option Nothing
      $ fmap (Just . T.pack) $ do
            void $ char '-'
            many' $ satisfy $ \c ->
                isDigit c || isAlpha c || c == '-'
    pure $ SemVersion major minor patch mbPreRelease

--------------------------------------------------------------------------------
-- Build Version
--------------------------------------------------------------------------------

showBuildVersion :: BuildVersion -> T.Text
showBuildVersion (BuildVersion ver tag) =
    format (d%"-"%s) ver tag
showBuildVersion HeadVersion = "HEAD"
parseBuildVersion :: T.Text -> Maybe BuildVersion
parseBuildVersion versionText =
    case parseOnly buildVersionParser versionText of
      Left _        -> Nothing
      Right version -> Just version

buildVersionParser :: Parser BuildVersion
buildVersionParser =  (HeadVersion <$ string "HEAD")  <|>  BuildVersion <$> decimal <* char '-' <*> takeText <* endOfInput

--------------------------------------------------------------------------------
-- Classes
--------------------------------------------------------------------------------

class (Show a, Eq a, Yaml.FromJSON a) => Versioned a where
    parseVersion :: T.Text -> Maybe a
    formatVersion :: a -> T.Text

--------------------------------------------------------------------------------
-- Instances
--------------------------------------------------------------------------------

instance Yaml.ToJSON SemVersion where
    toJSON semVersion = Yaml.String $ showSemVersion semVersion

instance Yaml.FromJSON SemVersion where
    parseJSON y@(Yaml.String v) = case parseSemVersion v of
        Just semVer -> pure semVer
        Nothing     -> Aeson.typeMismatch "SemVersion" y

    parseJSON invalid = Aeson.typeMismatch "SemVersion" invalid

instance Yaml.FromJSON BuildVersion where
    parseJSON y@(Yaml.String v) = case parseBuildVersion v of
        Just semVer -> pure semVer
        Nothing     -> Aeson.typeMismatch "BuildVersion" y

    parseJSON invalid = Aeson.typeMismatch "BuildVersion" invalid

instance ToHttpApiData SemVersion where
    toUrlPiece = showSemVersion

instance FromHttpApiData SemVersion where
    parseUrlPiece t = case parseSemVersion t of
        Nothing     -> Left $ "Cannot parse '" <> t <> "' as SemVersion"
        Just semVer -> Right semVer

instance P.Pretty SemVersion where
    pretty = P.pretty . showSemVersion

instance Versioned SemVersion where
    parseVersion = parseSemVersion
    formatVersion = showSemVersion

instance Versioned BuildVersion where
    parseVersion = parseBuildVersion
    formatVersion = showBuildVersion

instance ToField SemVersion where
    toField = toField . showSemVersion

instance FromField SemVersion where
    fromField f@(Field (SQLText t) _) =
        case parseSemVersion t of
            Nothing -> returnError ConversionFailed f "need a Semantic Version"
            Just sv -> Ok sv
    fromField x                       = returnError ConversionFailed x "need a text"
