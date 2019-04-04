-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module DA.Sdk.Cli.Repository.Types
    ( -- * Types
      Package(..)
    , Subject(..)
    , Repository(..)
    , PackageName(..)
    , GenericVersion(..)
    , PackageDescriptor(..)
    , PackageInfo(..)
    , PkgFileInfo(..)
    , DownloadablePackage(..)
    , Attribute(..)
    , SearchAttribute(..)
    , AttributeValue(..)
    , Credentials(..)
    , BintrayCredentials(..)
    , RequestError(..)
    , Error(..)
    , Handle(..)
    , Release(..)
    , ManagementHandle(..)
    , RequestHandle(..)
    , TagsFilter(..)
    , BitFlag(..)
    , BintrayPackage(..)
    , LatestVersion(..)

    , NameSpace(..)
    , TemplateName(..)
    , ReleaseLine(..)
    , Revision(..)
    , Tag(..)
    , Template(..)
    , TemplateType(..)
    , TemplateRepoHandle(..)
    , toExactVsnAndRelLine
    , toExactVsnAndRelLineTagTxt

    , FetchName (..)
    , FetchPrefix (..)
    , FetchArg (..)

      -- * Defaults
    , defaultTagsFilter

      -- * Pretty printing
    , prettyReleaseWithTags
    , prettyReleasesWithTags

    , versionSpecParser

    , AcceptEverything

    -- * Utilities
    , attributesToMap
    , semToGenericVersion
    ) where

import           DA.Sdk.Cli.Credentials
import qualified DA.Sdk.Cli.Metadata    as Metadata
import           DA.Sdk.Cli.OS          (OS)
import           DA.Sdk.Cli.Conf.Types  (NameSpace (..))
import qualified DA.Sdk.Cli.Conf.Consts as Conf
import           DA.Sdk.Prelude
import qualified DA.Sdk.Pretty          as P
import qualified DA.Sdk.Version         as V
import qualified Data.Aeson.Types       as Aeson
import           Data.Hashable          (Hashable (..))
import qualified Data.HashMap.Strict    as HMS
import           Data.List.NonEmpty     (NonEmpty((:|)))
import qualified Data.Map               as Map
import qualified Data.Text.Extended     as T
import qualified Data.Text.Encoding     as T
import qualified Data.ByteString        as BS
import qualified Data.ByteString.Lazy   as BSL
import qualified Data.Set               as Set
import qualified Data.Yaml              as Yaml
import           Data.Yaml              ((.:), (.=))
import           Data.Map               (Map)
import           Data.Typeable
import           Database.SQLite.Simple           hiding (Error)
import           Database.SQLite.Simple.Internal
import           Database.SQLite.Simple.FromField
import           Database.SQLite.Simple.Ok
import           Database.SQLite.Simple.ToField
import qualified Network.HTTP.Types.Status as Status
import           Servant.API
import qualified Servant.Client         as S
import           Control.Monad.Trans.Except (ExceptT)
import qualified Data.Attoparsec.Text   as A
import           Text.Read              (lexP, parens, pfail, readPrec)
import           Text.Read.Lex          (Lexeme(Ident))
import           Network.HTTP.Media     ((//))
import qualified Data.List.NonEmpty     as NE
import qualified Network.HTTP.Types.Status as S

-- FetchPrefix, FetchName

newtype FetchPrefix = FetchPrefix
    { unwrapFetchPrefix :: Text
    } deriving (Eq, Show)

newtype FetchName = FetchName
    { unwrapFetchName :: Text
    } deriving (Eq, Show)

data FetchArg = FetchArg
    { fetchPrefix :: Maybe FetchPrefix
    , fetchName :: FetchName
    } deriving (Eq, Show)

newtype TemplateName = TemplateName
    { unwrapTemplateName :: Text
    } deriving (Eq, Show)
data ReleaseLine     = ReleaseLine Int Int
                     | ExactRelease Int Int Int deriving Eq
newtype Revision     = Revision Int deriving (Eq, Show)
data Tag             = Tag
    { tagName  :: Text
    , tagValue :: Text
    } deriving (Eq, Show)
data Template        = Template
    { templateName      :: Text -- TODO (GH) this could be of type TemplateName
    , templateDesc      :: Text
    , templateRevs      :: [Revision]
    , templateTags      :: [Tag]
    , templateNameSpace :: NameSpace
    , templateRelLine   :: Text
    } deriving (Eq, Show)
data TemplateType = Addon
                  | Project deriving (Eq)

instance (Show TemplateType) where
    show Addon   = "add-on"
    show Project = "project"

instance (Read TemplateType) where
    readPrec =
        parens
        ( do Ident s <- lexP
             case s of
               txt | (T.toLower $ T.pack txt) == "add-on"   -> return Addon
               txt | (T.toLower $ T.pack txt) == "project"  -> return Project
               _       -> pfail
        )

instance (Show ReleaseLine) where
    show (ReleaseLine x y)    = intercalate "." [show x, show y, "x"]
    show (ExactRelease x y z) = intercalate "." [show x, show y, show z]

toExactVsnAndRelLineTagTxt :: V.SemVersion -> (Text, Text)
toExactVsnAndRelLineTagTxt sdkVsn = (exactVsn, T.pack relLine)
  where
    relLine  = intercalate "." [show $ V._svMajor sdkVsn, show $ V._svMinor sdkVsn, "x"]
    exactVsn = T.takeWhile (\c -> c /= '-') $ V.showSemVersion sdkVsn

toExactVsnAndRelLine :: V.SemVersion -> (ReleaseLine, ReleaseLine)
toExactVsnAndRelLine sdkVsn = (exactVsn, relLine)
  where
    relLine  = ReleaseLine (V._svMajor sdkVsn) (V._svMinor sdkVsn)
    exactVsn = ExactRelease (V._svMajor sdkVsn) (V._svMinor sdkVsn) (V._svPatch sdkVsn)

-- Bintray neither accepts simple query flags (which are used by Servant,
-- like ?someflag), nor true/false flags (Servant turns Booleans to those,
-- like ?someflag=true)
data BitFlag = Yes | No deriving (Eq, Show)

-- | A bintray subject.
newtype Subject = Subject Text deriving (Eq, Show)

-- | A bintray repository.
newtype Repository = Repository Text deriving (Eq, Show)

-- | A bintray package name.
newtype PackageName = PackageName Text deriving (Eq, Ord, Show)

-- | A generic version (text).
newtype GenericVersion = GenericVersion Text deriving (Eq, Ord, Show)

semToGenericVersion :: V.SemVersion -> GenericVersion
semToGenericVersion = GenericVersion . V.showSemVersion

instance P.Pretty GenericVersion where
    pretty (GenericVersion vTxt) = P.pretty vTxt

instance Aeson.ToJSON GenericVersion where
    toJSON (GenericVersion vTxt) = Aeson.String vTxt

instance P.Pretty v => P.Pretty (DownloadablePackage v) where -- TODO: nice pretty instance
    pretty dlPkg =
        P.sep
            [ P.mb (_dpClassifier dlPkg) P.t
            , P.t $ _pId $ _dpPackage dlPkg
            , P.pretty $ _dpVersion dlPkg
            ]

-- | A type which allows (with its special FromJSON method) querying latest
-- available version
newtype LatestVersion = LatestVersion GenericVersion deriving (Eq, Show)

-- | A descriptor used for package creation on Bintray.
data PackageDescriptor = PackageDescriptor
    { descPkgName     :: Text
    , descDescription :: Text
    } deriving (Eq, Show)

data PackageInfo = PackageInfo
    { infoPkgName     :: PackageName
    , infoVersions    :: [GenericVersion]
    , infoDescription :: Text
    , infoAttributes  :: Map Text [Text]
    } deriving (Eq, Ord, Show)

data PkgFileInfo = PkgFileInfo
    { pkgFilePath :: Text }
    deriving (Eq, Ord, Show)

-- | Bintray attributes can have different types. We only need the string type
-- at the moment.
-- Possible types: string, date, number, boolean, version
newtype AttributeValue
    = AVText [Text] deriving (Eq, Show)

-- | A bintray attribute.
data Attribute = Attribute
    { _aName :: Text
    , _aValue :: AttributeValue
    } deriving (Eq, Show)

attributesToMap :: [Attribute] -> Map Text [Text]
attributesToMap =
    Map.fromList .
    fmap (\l@( (n, _) :| _) -> (n, concatMap snd l)) .
    NE.groupBy ((==) `on` fst) . sortBy (compare `on` fst) .
    fmap (\(Attribute attrName (AVText attrValues)) -> (attrName, attrValues))

newtype SearchAttribute = SearchAttribute Attribute deriving (Eq, Show)

-- | A package on one of our repositories.
data Package = Package
    { _pId    :: Text
    -- ^ Name of the package. E.g: @"damlc"@.
    , _pGroup :: [Text]
    -- ^ Group of the package. E.g: @["com", "digitalasset"]@.
    } deriving (Show)

-- | A released artifact on the artifact server in a certain version.
data DownloadablePackage version = DownloadablePackage
    { _dpClassifier :: Maybe Text
      -- ^ A maven package classifier.
    , _dpPackaging  :: Metadata.Packaging
      -- ^ What kind of package ('tar.gz'/'jar'/...) are we talking about?
    , _dpPackage    :: Package
      -- ^ The package to download.
    , _dpVersion    :: version
      -- ^ The version of the package.
    } deriving (Show)

-- | A package as returned from the bintray API.
data BintrayPackage = BintrayPackage
    { _bpPackageName :: PackageName
    , _bpRepository :: Repository
    , _bpOwner :: Subject
    , _bpVersions :: [GenericVersion]
    , _bpDescription :: Text
    , _bpAttributes :: Map Text [Text]
    } deriving (Eq, Show)

data RequestError
    = RequestErrorUnauthorized
    | RequestErrorStatus Status.Status
    | ServantError S.ServantError
    | ServantErrorWithContext S.ServantError String
    deriving (Show, Eq)

data Error
    = ErrorVersionParse Text
      -- ^ A version couldn't be parsed.
    | ErrorRequest RequestError
      -- ^ There was an error in a http request.
    | ErrorJSONParse Text
      -- ^ There was an error while parsing some JSON.
    | ErrorDownload RequestError
      -- ^ There was an error while downloading a package.
    | ErrorUpload RequestError
      -- ^ There was an error while uploading a package.
    | ErrorUnknownOS Text
      -- ^ The os is not a supported one.
    | ErrorMissingCredentials
      -- ^ No credentials found.
    | ErrorNoVersionFound
      -- ^ No version could be found on the server.
    | ErrorTemplateAlreadyExists Text
      -- ^ The template to be uploaded already exists.
    | ErrorNoSuchTemplate TemplateName ReleaseLine TemplateType
      -- ^ The template to be downloaded cannot be found.
    | ErrorTar Text
      -- ^ There was an error with the archive (compression/decompression).
    deriving (Show, Eq)

-- | Tags used to filter package versions from the remote server.
data TagsFilter = TagsFilter
    { _includedTags :: Set.Set Text
      -- ^ At least one of the included tags needs to match a package tag.
    , _excludedTags :: Set.Set Text
      -- ^ No excluded tag is allowed to match a package tag.
    } deriving (Show)

-- | This handle can be used to query our package repository and download packages.
data Handle m = Handle
    { hLatestSdkVersion ::
        TagsFilter -> m (Either Error V.SemVersion)
      -- ^ Check for the latest SDK version.
    , hDownloadPackage ::
           forall version. (V.Versioned version)
        => Repository
        -> DownloadablePackage version
        -> FilePath
        -> m (Either Error FilePath)
      -- ^ Download a package from the repository (experimental or normal).
      -- TODO (GH) Remove and replace with the same one from Bintray Request Handle
    , hLatestPackageVersion ::
           forall version. (V.Versioned version)
        => Package
        -> m (Either Error version)
      -- ^ Check for the latest version of the sdk assistant.
    , hDownloadSdkAssistant ::
           Package
        -> OS
        -> V.BuildVersion
        -> FilePath
        -> m (Either Error FilePath)
      -- ^ Download the latest version of the sdk assistant into a directory.
    , hCheckCredentials ::
           m (Either Error ())
      -- ^ Check for valid credentials.
    , hGetPackageTags ::
           PackageName
        -> m (Either Error (Set.Set Text))
    }

data RequestHandle = RequestHandle
    { hReqCreatePackage ::
           Subject
        -> Repository
        -> PackageDescriptor
        -> Maybe BasicAuthData
        -> ExceptT Error IO ()
      -- ^ Create package in the distant repository
    , hReqSetAttributes ::
           Subject
        -> Repository
        -> PackageName
        -> [Attribute]
        -> Maybe BasicAuthData
        -> ExceptT Error IO ()
      -- ^ Set attributes of a package
    , hReqUploadPackage ::
           Subject
        -> Repository
        -> PackageName
        -> GenericVersion
        -> [Text]
        -> BS.ByteString
        -> Maybe BitFlag
        -> Maybe BasicAuthData
        -> ExceptT Error IO ()
      -- ^ Upload a package to the distant repository
    , hReqSearchPkgsWithAtts ::
           Subject
        -> Repository
        -> [SearchAttribute]
        -> Maybe BasicAuthData
        -> ExceptT Error IO [PackageInfo]
      -- ^ Search for packages with the given attributes
    , hListRepos ::
           Subject
        -> Maybe BasicAuthData
        -> ExceptT Error IO [Repository]
      -- ^ List repositories for a given subject
    , hSearchPkgVersionFiles ::
           Subject
        -> Repository
        -> PackageName
        -> GenericVersion
        -> Maybe BitFlag
        -> Maybe BasicAuthData
        -> ExceptT Error IO [PkgFileInfo]
    , hGetPackageInfoReq ::
           Subject
        -> Repository
        -> PackageName
        -> Maybe BasicAuthData
        -> ExceptT Error IO BintrayPackage
    , hGetLatestVersionReq ::
           Subject
        -> Repository
        -> PackageName
        -> Maybe BasicAuthData
        -> ExceptT Error IO LatestVersion
    , hDownloadPackageReq ::
           Subject
        -> Repository
        -> PackageName
        -> GenericVersion
        -> FilePath
        -> Maybe BasicAuthData
        -> ExceptT Error IO FilePath
    }

data Release a = Release
    { _releaseVersion :: V.SemVersion
    , _releaseTags :: a
    }
    deriving (Show, Eq)

data ManagementHandle = ManagementHandle
    { _mhListReleases ::
           forall m. MonadIO m
        => m (Either Error [Release (Set.Set Text)])
      -- List available releases.
    , _mhGetReleaseTags ::
           forall m. MonadIO m
        => GenericVersion
        -> m (Either Error (Set.Set Text))
      -- Get all tags of a release.
    , _mhSetReleaseTags ::
           forall m. MonadIO m
        => GenericVersion
        -> Set.Set Text
        -> m (Either Error (Set.Set Text))
    , _mhAddReleaseTags ::
           forall m. MonadIO m
        => GenericVersion
        -> Set.Set Text
        -> m (Either Error (Set.Set Text))
      -- ^ Add the tags passed and to a release and return all tags on that
      -- release.
    , _mhRemoveReleaseTags ::
           forall m. MonadIO m
        => GenericVersion
        -> Set.Set Text
        -> m (Either Error (Set.Set Text))
      -- ^ Remove the tags passed and return the tags still set for a release.
    , _mhListPublishedTemplates ::
          forall m. MonadIO m
        => Subject
        -> Repository
        -> m (Either Error [(PackageName, Text, GenericVersion)])
      -- ^ List published templates in a specific subject and repository.
    , _mhListRepos ::
          forall m. MonadIO m
        => Subject
        -> m (Either Error [Repository])
      -- ^ List all repositories for a specific subject.
    }

-- | This handle enables template uploading / downloading and other operations
data TemplateRepoHandle = TemplateRepoHandle
    { uploadTemplate ::
           NameSpace
        -> TemplateName
        -> ReleaseLine
        -> Revision
        -> BS.ByteString
        -> ExceptT Error IO ()
    , downloadTemplate ::
           NameSpace
        -> TemplateName
        -> ReleaseLine
        -> Revision
        -> FilePath
        -> ExceptT Error IO FilePath
    , getLatestRevision ::
           NameSpace
        -> TemplateName
        -> ReleaseLine
        -> ExceptT Error IO (Maybe Revision)
    , tagTemplate ::
           NameSpace
        -> TemplateName
        -> ReleaseLine
        -> [Tag]
        -> ExceptT Error IO ()
    , searchTemplates ::
           NameSpace
        -> [Tag]
        -> ExceptT Error IO [Template]
    , getInfo ::
           NameSpace
        -> TemplateName
        -> ReleaseLine
        -> ExceptT Error IO (Maybe Template)
    , listNameSpaces ::
           ExceptT Error IO [NameSpace]
    }

--------------------------------------------------------------------------------
-- Defaults
--------------------------------------------------------------------------------

-- | Default tags, used when first installing the sdk.
defaultTagsFilter :: TagsFilter
defaultTagsFilter = TagsFilter
    { _includedTags = Set.fromList Conf.defaultIncludedTags
    , _excludedTags = Set.fromList Conf.defaultExcludedTags
    }

versionSpecParser :: A.Parser GenericVersion
versionSpecParser = do
    p1 <- some A.digit
    p2 <- many $
      do
        pChr1 <- A.char '.'
        pStr2 <- some A.digit
        return (pChr1 : pStr2)
    return $ GenericVersion $ T.pack (p1 <> concat p2)

data AcceptEverything

instance Accept AcceptEverything where
   contentType _ = "*" // "*"

instance MimeRender AcceptEverything BS.ByteString where
   mimeRender _ = BSL.fromStrict

instance MimeUnrender AcceptEverything BS.ByteString where
   mimeUnrender _ = Right . BSL.toStrict

--------------------------------------------------------------------------------
-- Instances
--------------------------------------------------------------------------------

instance V.Versioned GenericVersion where
    parseVersion = A.maybeResult . A.parse versionSpecParser
    formatVersion (GenericVersion vsn) = vsn

instance P.Pretty RequestError where
    pretty = \case
        RequestErrorUnauthorized -> P.vsep
            [ P.reflow "The request was unauthorized."
            , P.reflow "Your credentials in ~/.da/da.yaml are probably incorrect."
            ]
        RequestErrorStatus (Status.Status code message) -> P.vsep
            [ P.label "Status Code" $ P.pretty code
            , P.label "Status Message" $ P.t truncatedMessage
            ]
            where
              decodedMessage = T.decodeUtf8 message
              truncatedMessage = if T.length decodedMessage > 100 then
                    T.take 97 $ decodedMessage  <> " ..."
                    else decodedMessage

        ServantError e -> P.t $ detailServantError e ""
        -- The goal is to add context for all ServantErrors. To do it incrementally, we keep ServantError for now
        ServantErrorWithContext e ctx -> P.t $ detailServantError e ctx

detailServantError :: S.ServantError -> String -> Text
detailServantError (S.FailureResponse r) ctx                = let st = S.responseStatusCode r
                                                            in ("Received failure response: " <>
                                                               (T.show $ S.statusMessage st) <>
                                                               " (" <>
                                                               (T.show $ S.statusCode st) <>
                                                               ") with context: ") <>
                                                               T.show ctx
detailServantError (S.DecodeFailure t _resp) _              = "Decode failure: " <> t
detailServantError (S.UnsupportedContentType mType _resp) _ = "Response has unsupported \
                                                            \content type: " <> T.show mType
detailServantError (S.InvalidContentTypeHeader _resp) _     = "The content type header is invalid."
-- In this case, `t` is a long text showing internal types so it is not included in the message.
detailServantError (S.ConnectionError t) _ =
    if "no such protocol name" `T.isInfixOf` t || "certificate rejected" `T.isInfixOf` t
        then T.unlines
            [ "Network connection error: Failed to form HTTPS connection with Bintray."
            , ""
            , "On Linux, this may be caused by missing system packages: netbase and ca-certificates."
            , "Please run:"
            , ""
            , "    sudo apt-get install netbase ca-certificates"
            , ""
            , "And try again."
            , ""
            , "Internal error message: " <> t ]
        else T.unlines
            [ "Network connection error."
            , ""
            , "Internal error message: " <> t ]

instance P.Pretty Error where
    pretty = \case
        ErrorVersionParse version ->
            P.label "Version could not be parsed"
          $ P.pretty version
        ErrorRequest requestError ->
            P.label "There was an error while making a request"
          $ P.pretty requestError
        ErrorJSONParse jsonParseError ->
            P.label "There was an error while parsing the JSON response"
          $ P.pretty jsonParseError
        ErrorDownload downloadError ->
            P.label "There was an error while downloading a package"
          $ P.pretty downloadError
        ErrorUpload uploadError ->
            P.label "There was an error while uploading a package"
          $ P.pretty uploadError
        ErrorUnknownOS os ->
            P.label "The os could not be recognized"
          $ P.pretty os
        ErrorMissingCredentials ->
            P.t "Missing credentials."
        ErrorNoVersionFound ->
            P.t "No matching version could be found on the server."
        ErrorTemplateAlreadyExists name ->
            P.t ("Template already exists: " <> name)
        ErrorNoSuchTemplate (TemplateName tn) rl tType ->
            P.t ("No such " <> T.show tType <>
                    " template found: " <> tn <> "-" <> T.show rl)
        ErrorTar msg ->
            P.label "There was an error while processing the tarball"
          $ P.pretty msg

prettyReleaseWithTags :: Release (Set.Set Text) -> P.Doc ann
prettyReleaseWithTags (Release version tags) =
    P.pretty version
        <> ":"
        P.<+> P.hsep (map P.t $ Set.toList tags)

prettyReleasesWithTags :: [Release (Set.Set Text)] -> P.Doc ann
prettyReleasesWithTags = P.vsep . fmap prettyReleaseWithTags

instance Yaml.FromJSON a => Yaml.FromJSON (Release a) where
    parseJSON = Yaml.withObject "Release" $ \o ->
        Release <$> o .: "version" <*> o .: "tags"

instance Yaml.ToJSON a => Yaml.ToJSON (Release a) where
    toJSON (Release version tags) = Yaml.object
        [ "version" .= version
        , "tags" .= tags
        ]

instance Yaml.ToJSON PackageDescriptor where
    toJSON (PackageDescriptor pkgName description) = Yaml.object
        [ "name" .= pkgName
        , "desc" .= description
        ]

instance Yaml.FromJSON PackageDescriptor where
    parseJSON (Yaml.Object v) = do
        PackageDescriptor <$> v .: "name" <*> v .: "desc"
    parseJSON invalid = Aeson.typeMismatch "PackageDescription" invalid

instance Yaml.ToJSON Attribute where
    toJSON (Attribute name value) = case value of
        AVText ts ->
            Yaml.object
                [ "name" .= name
                , "type" .= ("string" :: Text)
                , "values" .= ts
                ]

instance Yaml.ToJSON SearchAttribute where
    toJSON (SearchAttribute (Attribute name (AVText values))) =
            Yaml.object [ name .= values ]

-- Example:
-- [
--  {"name": "att1", "values" : ["val1"], "type": "string"},
--  {"name": "att2", "values" : [1, 2.2, 4], "type": "number"},
--  {"name": "att3", "values" : ["2011-07-14T19:43:37+0100", "2011-07-14T19:43:37+0100", "1994-11-05T13:15:30Z"], "type": "date"}
-- ]
instance Yaml.FromJSON Attribute where
  parseJSON (Yaml.Object v) = do
      aName <- v .: "name"
      aType <- v .: "type" :: Aeson.Parser String
      aValue <- case aType of
          "string" -> AVText <$> v .: "values"
          _ -> fail $ "Attribute type \"" ++ aType ++ "\" not supported."

      pure $ Attribute
          { _aName = aName
          , _aValue = aValue
          }
  parseJSON invalid = Aeson.typeMismatch "Attribute" invalid

instance Yaml.FromJSON SearchAttribute where
    parseJSON (Yaml.Object v) = do
        case HMS.toList v of
            [(name, rawValues)] -> do
                values <- Yaml.parseJSON rawValues
                pure $ SearchAttribute (Attribute name (AVText values))
            xs -> fail $ "Expected one search attribute, found '" <> show xs <> "'"
    parseJSON invalid = Aeson.typeMismatch "SearchAttribute" invalid

instance Yaml.FromJSON PackageInfo where
    parseJSON (Yaml.Object v) = do
        pName <- v .: "name"
        pDesc <- v .: "desc" :: Aeson.Parser Text
        pVersions <- v .: "versions" :: Aeson.Parser [Text]
        pAttribs <- v .: "attributes" :: Aeson.Parser (Map Text [Text])

        pure $ PackageInfo
            { infoPkgName     = PackageName pName
            , infoDescription = pDesc
            , infoVersions    = map GenericVersion pVersions
            , infoAttributes  = pAttribs
            }
    parseJSON invalid = Aeson.typeMismatch "PackageInfo" invalid

instance Yaml.ToJSON PackageInfo where
    toJSON PackageInfo {..} = Yaml.object
        [ "name"     .= infoPkgName
        , "desc"     .= infoDescription
        , "versions" .= infoVersions
        ]

instance Hashable Subject where
    hashWithSalt i (Subject t) = hashWithSalt i t

instance Yaml.FromJSON Subject where
    parseJSON = Yaml.withText "Subject" (pure . Subject)

instance Yaml.ToJSON Subject where
    toJSON (Subject t) = Yaml.String t

instance Yaml.FromJSON PkgFileInfo where
    parseJSON (Yaml.Object o) = do
        rName <- o .: "path"
        pure $ PkgFileInfo rName
    parseJSON invalid = Aeson.typeMismatch "PkgFileInfo" invalid

instance Yaml.ToJSON PkgFileInfo where
    toJSON PkgFileInfo {..} = Yaml.object
        [ "path" .= pkgFilePath ]

instance Hashable Repository where
    hashWithSalt i (Repository t) = hashWithSalt i t

instance Yaml.FromJSON Repository where
    parseJSON (Yaml.String t) = pure $ Repository t
    parseJSON (Yaml.Object o) = do
        rName <- o .: "name"
        pure $ Repository rName
    parseJSON invalid = Aeson.typeMismatch "Repository" invalid

instance Yaml.ToJSON Repository where
    toJSON (Repository t) = Yaml.String t

instance Hashable PackageName where
    hashWithSalt i (PackageName t) = hashWithSalt i t

instance Yaml.FromJSON PackageName where
    parseJSON = Yaml.withText "PackageName" (pure . PackageName)

instance Yaml.ToJSON PackageName where
    toJSON (PackageName t) = Yaml.String t

instance ToField GenericVersion where
    toField (GenericVersion vTxt) = toField vTxt

instance FromField GenericVersion where
    fromField f = GenericVersion <$> fromField f

instance Hashable GenericVersion where
    hashWithSalt i (GenericVersion vTxt) = hashWithSalt i vTxt

instance Yaml.FromJSON GenericVersion where
    parseJSON = Yaml.withText "GenericVersion" (pure . GenericVersion)

instance Yaml.FromJSON LatestVersion where
    parseJSON (Yaml.Object o) = do
        version <- o .: "name"
        pure $ LatestVersion $ GenericVersion version
    parseJSON invalid = Aeson.typeMismatch "LatestVersion" invalid

instance Yaml.ToJSON LatestVersion where
    toJSON (LatestVersion t) = Yaml.object
        [ "name" .= Aeson.toJSON t ]

instance ToHttpApiData Subject where
    toUrlPiece (Subject t) = t

instance ToHttpApiData Repository where
    toUrlPiece (Repository t) = t

instance ToHttpApiData PackageName where
    toUrlPiece (PackageName t) = t

instance ToHttpApiData GenericVersion where
    toUrlPiece (GenericVersion t) = t

instance ToHttpApiData BitFlag where
    toUrlPiece Yes = "1"
    toUrlPiece No  = "0"

instance FromHttpApiData Subject where
    parseUrlPiece = Right . Subject

instance FromHttpApiData Repository where
    parseUrlPiece = Right . Repository

instance FromHttpApiData PackageName where
    parseUrlPiece = Right . PackageName

instance FromHttpApiData GenericVersion where
    parseUrlPiece = Right . GenericVersion

instance FromHttpApiData BitFlag where
    parseUrlPiece "1" = Right Yes
    parseUrlPiece "0" = Right No
    parseUrlPiece t   = Left $ "Cannot convert '" <> t <> "' into BitFlag, expected either '1' or '0'"

instance Yaml.ToJSON BintrayPackage where
    toJSON BintrayPackage {..} = Yaml.object
        [ "name"       .= _bpPackageName
        , "repo"       .= _bpRepository
        , "owner"      .= _bpOwner
        , "versions"   .= _bpVersions
        , "desc"       .= _bpDescription
        , "attributes" .= _bpAttributes
        ]

instance Yaml.FromJSON BintrayPackage where
    parseJSON (Yaml.Object v) = do
        vName <- v .: "name"
        vRepository <- v .: "repo"
        vOwner <- v .: "owner"
        vVersions <- v .: "versions"
        vDescription <- v .: "desc"
        vAttribs <- v .: "attributes" :: Aeson.Parser (Map Text [Text])
        pure $ BintrayPackage
            { _bpPackageName = vName
            , _bpRepository = vRepository
            , _bpOwner = vOwner
            , _bpVersions = vVersions
            , _bpDescription = vDescription
            , _bpAttributes = vAttribs
            }
    parseJSON invalid = Aeson.typeMismatch "Attribute" invalid


-----------------------------
-- SQLite-simple instances

withSQLText :: Typeable a => (Text -> Ok a) -> Field -> Ok a
withSQLText f (Field (SQLText txt) _) = f txt
withSQLText _ x                       = returnError ConversionFailed x "need a text"

instance ToField Subject where
    toField (Subject t) = toField t

instance FromField Subject where
    fromField = withSQLText (Ok . Subject)

instance ToField Repository where
    toField (Repository t) = toField t

instance FromField Repository where
    fromField = withSQLText (Ok . Repository)

instance ToField PackageName where
    toField (PackageName t) = toField t

instance FromField PackageName where
    fromField = withSQLText (Ok . PackageName)
