-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-} -- for the orphan ToYAML / FromYAML instances

-- | This module defines types and functions for working with configuration. The
-- configuration is made up of individual properties that have a name and a
-- value. These properties are represented by a sum type with one constructor
-- each.

-- We read configuration properties from different sources -- built-in defaults,
-- config files, environment variables, command-line -- into an ordered list
-- where the rightmost property takes precedence. We then process list
-- (validation) which results in either a fully-specified configuration object
-- -- 'Conf' or 'Project' -- or a failure consisting of a list of the properties
-- that are missing.

-- When we change the name of a configuration file property we add a parser for
-- the old name and add this to the 'parsersDeprecatedInVersion' function. This
-- allows us to parse old config files into the most recent property types. We
-- can then choose to overwrite the configuration file which will automatically
-- write it as the latest version.

-- The reason we use a list of property values instead of a single record type
-- with Maybe for each field is that a list affords better diagnostics. We could
-- use it to understand what each source provides and the order of preference.
-- Note that to actually do this we'll want to extend the 'Prop' with a source
-- field of some sort. The list representation makes the conversion function to
-- 'Conf' slower (since it traverses the full list for each property), but this
-- should not be a performance bottleneck in practice.

module DA.Sdk.Cli.Conf
    ( module DA.Sdk.Cli.Conf.Types
    , module DA.Sdk.Cli.Conf.Consts
    , Name (..)
    , ProjectWarning (..)
    , Prop (..)
    , Props (..)
    , ConfError(..)
    , getProject
    , requireConf
    , propsToConf
    , getDefaultConfProps
    , L.getDefaultConfigFilePath
    , getSdkTempDirPath
    , defaultConfigFileName
    , getEffectiveConfigPath
    , findProjectRootDir
    , findProjectMasterFile
    , findProjectRootDirAndLoadProps
    , staticConfProps
    , configHelp
    , validateCredentials
    , validateRepositoryURLs
    , joinProps
    , pathToName
    , nameValToProp
    , parsePositive

    , defaultPropSelfUpdateVal
    , decideUpdateToWhichVersion

      -- * Parsing
    , bintrayAPIURLParser
    , bintrayDownloadURLParser

      -- * Pretty printing
    , prettyNames
    , prettyConfError
    ) where

import           Control.Lens                  ((^.), each, preview)
import           Control.Lens.Fold             (findMOf, unfolded)
import           DA.Sdk.Cli.Credentials
import qualified DA.Sdk.Cli.Message            as M
import           DA.Sdk.Cli.Monad.Locations    as L
import           DA.Sdk.Cli.Locations
import qualified DA.Sdk.Cli.Locations.Types    as L
import           DA.Sdk.Cli.Conf.Types
import           DA.Sdk.Cli.Conf.Consts
import           DA.Sdk.Cli.SelfUpgrade.Types
import           DA.Sdk.Prelude                hiding (displayException)
import qualified DA.Sdk.Pretty                 as P
import           DA.Sdk.Version                (BuildVersion (..))
import           Data.Aeson.Extra.Merge.Custom (customMerge)
import           Data.Aeson                    (eitherDecodeStrict')
import qualified Data.Aeson.Types              as Aeson
import           Data.Foldable                 (foldl', foldlM)
import           Data.Maybe                    (catMaybes, fromMaybe)
import qualified Data.Map.Strict               as TreeMap
import           Data.Monoid                   (Last (..), getLast)
import qualified Data.Text.Extended            as T
import           Data.Either.Validation
import           Data.Yaml                     ((.:), (.=))
import qualified Data.Yaml                     as Yaml
import qualified Filesystem.Path.CurrentOS     as F
import           Servant.Client                (BaseUrl(..), parseBaseUrl, showBaseUrl)
import           DA.Sdk.Cli.Job.Types
import           Control.Exception
import           Control.Monad.Except
import qualified Data.Map.Strict               as Mp
import           System.IO.Error               (isDoesNotExistError)
import           DA.Sdk.Cli.Conf.NameToProp
import           DA.Sdk.Cli.Monad.UserInteraction
import           DA.Sdk.Cli.Monad.FileSystem
import           DA.Sdk.Cli.Monad.MockIO

-- TODO: There's a lot of clean-up possible here. We should include all relevant
-- property info here, including whether it's a project property, what the
-- default is, optional or not, example, etc. and use that in the respective
-- functions in this file.

-- | Try to find the Name associated with a path based on the Name infoKeys.
--
-- If the given path doesn't exist, an error is returned with optionally the closest
-- path known.
pathToName :: Text -> Either Text Name
pathToName path =
    case TreeMap.lookupLE path pathToNameMap of
        Just (path', n) | path' == path  -> Right n
        maybeLtPath ->
            let maybeGtPath = TreeMap.lookupGT path pathToNameMap
                -- search the closest config key and propose it as suggestion to the user
                suggestion :: Text
                suggestion   = maybe "" (\t -> ", do you mean '" <> t <> "'?") $
                               fmap snd $ headMay $ sort $
                               filter (\(d, _) -> d < 2 ) $ -- arbitrary minimun distance to avoid suggesting random keys
                               fmap (\(k, _) -> (distanceFrom k, k)) $
                               catMaybes [maybeLtPath, maybeGtPath]
            in Left $ "Configuration path '" <> path <> "' not found" <> suggestion
  where
    distanceFrom t = T.length path - (length $ takeWhile (uncurry (==)) $ T.zip path t)
    nameToInfoKeys = infoKeysToPath . infoKeys . nameInfo
    pathToNameMap = TreeMap.fromList $ fmap (\n -> (nameToInfoKeys n, n)) [minBound..maxBound]

-- | Default configuration properties. Don't put properties that must come from
-- the user here, clearly.
getDefaultConfProps :: MonadUserOutput m => m [Prop]
getDefaultConfProps = do
    termWidth <- getTermWidth
    return
        [ PropLogLevel defaultLogLevel
        , PropIsScript defaultIsScript
        , PropSDKDefaultVersion Nothing
        , PropTermWidth termWidth
        , PropSDKIncludedTags defaultIncludedTags
        , PropSDKExcludedTags defaultExcludedTags
        , PropBintrayAPIURL defaultBintrayAPIURL
        , PropBintrayDownloadURL defaultBintrayDownloadURL
        , PropUpgradeIntervalSeconds defaultUpgradeIntervalSeconds
        , PropUpdateChannel Nothing
        , PropNameSpaces [defaultNameSpace]
        , PropSelfUpdate defaultPropSelfUpdateVal
        , PropOutputPath Nothing
        ]

defaultPropSelfUpdateVal :: UpdateSetting
defaultPropSelfUpdateVal = RemindLater

-- | Static properties such as the config file version.
-- These override all other properties when writing out the configuration.
staticConfProps :: [Prop]
staticConfProps =
    [ PropConfVersion latestConfVersion ]

getDefaultProjectProps :: Monad m => m [Prop]
getDefaultProjectProps = do
    return
        [ PropDAMLScenario Nothing
        , PropProjectParties ["OPERATOR"]
        ]

getProjectDarDependencies :: (MonadFS m, MockIO m) => FilePath -> ExceptT ProjectMessage m [FilePath]
getProjectDarDependencies projectDir = do
    let toDarDepErr e = M.Error $ ProjectErrorDarDepMalformedFile darDepFile $ T.pack e
        darDepFile = projectDir </> "daml-dependencies.json"
    bytesOrErr <- lift $ readFile' darDepFile
    case bytesOrErr of
      Left (ReadFileFailed _ err) ->
        if isDoesNotExistError err then
            return []
        else
            throwError $ toDarDepErr $ displayException err
      Right bytes -> do
        dDepF <- withExceptT toDarDepErr $ ExceptT $ return $ eitherDecodeStrict' bytes
        let flatDeps = flattenDependencies dDepF
        _namesToFiles <- checkDeps flatDeps
        return $ nubOrd $ snd <$> flatDeps
  where
    flattenDependencies (DarDependencyFile deps) =
        concatMap flattenDependencies' deps
    flattenDependencies' (DarDependency file name deps) =
        (name, file):concatMap flattenDependencies' deps
    checkDeps deps =
        foldlM checkDep Mp.empty deps
    checkDep :: Monad m => Mp.Map Text FilePath -> (Text, FilePath) -> ExceptT ProjectMessage m (Mp.Map Text FilePath)
    checkDep namesToFiles (name, file1) =
        case Mp.lookup name namesToFiles of
          Just file | file == file1 ->
            return namesToFiles
          Just file2 ->
            throwError $ M.Error $ ProjectErrorDarDepFilePathClash name file1 file2
          Nothing ->
            return $ Mp.insert name file1 namesToFiles

instance Yaml.ToJSON Prop where
    toJSON (PropConfVersion ver)          = propToObject NameConfVersion (Yaml.toJSON ver)
    toJSON (PropUserEmail email)          = propToObject NameUserEmail (Yaml.toJSON email)
    toJSON (PropIsScript script)          = propToObject NameIsScript (Yaml.toJSON script)
    toJSON (PropProjectSDKVersion ver)    = propToObject NameProjectSDKVersion (Yaml.toJSON ver)
    toJSON (PropProjectName name)         = propToObject NameProjectName (Yaml.toJSON name)
    toJSON (PropProjectParties ps)        = propToObject NameProjectParties (Yaml.toJSON ps)
    toJSON (PropDAMLSource file)          = propToObject NameDAMLSource (Yaml.toJSON file)
    toJSON (PropDAMLScenario name)        = propToObject NameDAMLScenario (Yaml.toJSON name)
    toJSON (PropBintrayUsername a)        = propToObject NameBintrayUsername (Yaml.toJSON a)
    toJSON (PropBintrayKey a)             = propToObject NameBintrayKey (Yaml.toJSON a)
    toJSON (PropBintrayAPIURL url)        = propToObject NameBintrayAPIURL (Yaml.toJSON $ showBaseUrl url)
    toJSON (PropBintrayDownloadURL url)   = propToObject NameBintrayDownloadURL (Yaml.toJSON $ showBaseUrl url)
    toJSON (PropSDKDefaultVersion ver)    = propToObject NameSDKDefaultVersion (Yaml.toJSON ver)
    toJSON (PropSDKIncludedTags tags)     = propToObject NameSDKIncludedTags (Yaml.toJSON tags)
    toJSON (PropSDKExcludedTags tags)     = propToObject NameSDKExcludedTags (Yaml.toJSON tags)
    toJSON (PropUpgradeIntervalSeconds i) = propToObject NameUpgradeIntervalSeconds (Yaml.toJSON i)
    toJSON (PropUpdateChannel uc)         = propToObject NameUpdateChannel (Yaml.toJSON uc)
    toJSON (PropNameSpaces nss)           = propToObject NameNameSpaces (Yaml.toJSON nss)
    toJSON (PropSelfUpdate updS)          = propToObject NameSelfUpdate (Yaml.toJSON updS)
    toJSON (PropOutputPath outP)          = propToObject NameOutputPath (Yaml.toJSON $ pathToString <$> outP)
    -- Not all properties are supported in the config file (yet?)
    toJSON _                              = Yaml.object []

instance Yaml.ToJSON Props where
    -- | We first generate a potentially nested JSON object for each property,
    -- then we do a deep merge of these.
    toJSON (Props ps) =
        let ps' = ps <> [PropConfVersion latestConfVersion]
        in  foldl' customMerge (Yaml.object []) (map Yaml.toJSON ps')

instance Yaml.FromJSON Props where
    -- | We start with a list of prop parsers, one for each prop, then we give
    -- each of these the full config file and any parsers that failed. Not sure
    -- if that's the best thing we can do. Just writing a Prop parser and using
    -- a many combinator doesn't seem to work because it starts over each time,
    -- parsing the same Prop infinitely many times. There should be a nicer way
    -- to do this, but am pressed for time so we should look at this later.
    parseJSON v = do
        mbVersion <- optional . parseKeys ["version"] $ v
        let version = fromMaybe 0 mbVersion
        props <- fmap catMaybes (traverse ($ v) (fmap (optional .) (propParsers version)))
        return $ Props (props <> [PropConfVersion version])

joinProps :: Props -> Props -> Props
joinProps p1 p2 = Props (fromProps p1 ++ fromProps p2)

-- | Return a list of parsers we want to each try once. We deal with
-- deprecations by also trying all parsers that have been deprecated after the
-- given version.
propParsers :: Int -> [Yaml.Value -> Yaml.Parser Prop]
propParsers version =
    foldMap parsersDeprecatedInVersion [version+1..latestConfVersion] <>
    [ p NameUserEmail PropUserEmail
    , p NameIsScript PropIsScript
    , p NameProjectSDKVersion PropProjectSDKVersion
    , p NameProjectName PropProjectName
    , p NameProjectParties PropProjectParties
    , p NameDAMLSource PropDAMLSource
    , p NameDAMLScenario PropDAMLScenario
    , p NameBintrayUsername PropBintrayUsername
    , p NameBintrayKey PropBintrayKey
    , bintrayAPIURLParser
    , bintrayDownloadURLParser
    , p NameSDKDefaultVersion PropSDKDefaultVersion
    , p NameSDKIncludedTags PropSDKIncludedTags
    , p NameSDKExcludedTags PropSDKExcludedTags
    , p NameNameSpaces PropNameSpaces
    , p NameSelfUpdate PropSelfUpdate
    , p NameTermWidth PropTermWidth
    , p NameUpgradeIntervalSeconds PropUpgradeIntervalSeconds
    , p NameUpdateChannel PropUpdateChannel
    , p NameOutputPath (PropOutputPath . fmap textToPath)
    ]
  where
    p :: Yaml.FromJSON a => Name -> (a -> Prop) -> Yaml.Value -> Yaml.Parser Prop
    p name ctor = fmap ctor . (parseKeys . infoKeys . nameInfo $ name)

-- this helper has been added to avoid having a Orphan instance of FromJSON for BaseUrl
parseBaseUrlProp :: Name -> (BaseUrl -> Prop) -> Yaml.Value -> Yaml.Parser Prop
parseBaseUrlProp name ctor value =
        parseKeys path value >>= textToBaseUrlParser >>= return . ctor
    where
        path = infoKeys $ nameInfo name
        textToBaseUrlParser :: Text -> Yaml.Parser BaseUrl
        textToBaseUrlParser = toParser . parseBaseUrl . T.unpack
        toParser = either (\e -> fail $ "unable to parse baseUrl " <> show name <> ": " <> show e) return

bintrayAPIURLParser, bintrayDownloadURLParser :: Yaml.Value -> Yaml.Parser Prop
bintrayAPIURLParser      = parseBaseUrlProp NameBintrayAPIURL      PropBintrayAPIURL
bintrayDownloadURLParser = parseBaseUrlProp NameBintrayDownloadURL PropBintrayDownloadURL

-- | This function returns a list of the parsers that were deprecated in the
-- given version. These parsers will be run when we encounter a config file
-- older than the given version so that we retain the ability to read old config
-- files.
parsersDeprecatedInVersion :: Int -> [Yaml.Value -> Yaml.Parser Prop]
parsersDeprecatedInVersion 2 =
    [ fmap PropDAMLSource . parseKeys ["project", "source-dir"]
    ]
parsersDeprecatedInVersion _ = []

parseKeys :: (Yaml.FromJSON b) => [Text] -> Yaml.Value -> Yaml.Parser b
parseKeys [] v     = Yaml.parseJSON v
parseKeys (k:ks) v = Aeson.withObject (T.unpack k) (.: k) v >>= parseKeys ks

-- | Given a list of keys and a value, construct corresponding nested objects.
propToObject :: Name -> Yaml.Value -> Yaml.Value
propToObject name val = foldr (\k v -> Yaml.object [k .= v]) val (infoKeys . nameInfo $ name)

-- TODO
getEnvProps :: Monad m => m [Prop]
getEnvProps = return []

getEffectiveConfigPath :: MonadLocations m => Maybe FilePath -> m FilePath
getEffectiveConfigPath mbCliFile = do
    defaultCfg <- L.getDefaultConfigFilePath
    let mbEnvFile = Nothing -- TODO
        defaultCfgFilePath = L.unwrapFilePathOf defaultCfg
    return (fromMaybe defaultCfgFilePath (mbCliFile <|> mbEnvFile))

-- | Returns a fully specified 'Conf' or throws an exception. Takes a config
-- file path and a list of properties specified on the CLI.
requireConf :: (MonadUserOutput m, MonadFS m) => FilePath -> [Prop] -> m (Either ConfError Conf)
requireConf configFile cliProps = testfile' configFile >>= \case
    False -> pure $ Left $ ConfErrorMissingFile configFile
    True -> do
        defaultProps <- getDefaultConfProps
        envProps <- getEnvProps
        -- TODO: Consider doing the config file migration explicitly and
        -- separately, possibly in the CliM monad. That allows us to do
        -- logging and maybe ask the user explicitly if the actually want to
        -- do the migration. For now this happens without the user being
        -- able to do anything about it.
        filePropsOrError <- decodeYamlFile' configFile
        case filePropsOrError of
          Left _e -> pure $ Left $ ConfErrorMalformedFile configFile
          Right fileProps -> case propsToConf (defaultProps <> fromProps fileProps <> envProps <> cliProps) of
              Left names -> pure $ Left $ ConfErrorMissingProperties names
              Right conf -> pure $ Right conf

-- | Convert a list of ordered properties to a fully specified 'Conf' or return
-- a list of the the names of properties that are missing.
-- Later properties overwrite earlier ones.
propsToConf :: [Prop] -> Either [Name] Conf
propsToConf ps = case validated of
    Failure names -> Left names
    Success conf  -> Right conf
  where
    validated :: Validation [Name] Conf
    validated = Conf
        <$> validate (preview _PropConfVersion) NameConfVersion ps
        <*> validate (preview _PropLogLevel) NameLogLevel ps
        <*> validate (preview _PropIsScript) NameIsScript ps
        <*> optional (validate (preview _PropUserEmail) NameUserEmail ps)
        <*> validateCredentials ps
        <*> validateRepositoryURLs ps
        <*> validate (preview _PropSDKDefaultVersion) NameSDKDefaultVersion ps
        <*> validate (preview _PropSDKIncludedTags) NameSDKIncludedTags ps
        <*> validate (preview _PropSDKExcludedTags) NameSDKExcludedTags ps
        <*> validate (preview _PropTermWidth) NameTermWidth ps
        <*> validate (preview _PropUpgradeIntervalSeconds) NameUpgradeIntervalSeconds ps
        <*> validate (preview _PropUpdateChannel) NameUpdateChannel ps
        <*> fmap (\nss -> nubOrd $ defaultNameSpace : nss)
            (validate (preview _PropNameSpaces) NameNameSpaces ps)
        <*> validate (preview _PropSelfUpdate) NameSelfUpdate ps
        <*> validate (preview _PropOutputPath) NameOutputPath ps

-- | Validate a prop, giving it's name back if it fails.
validate :: (Prop -> Maybe a) -> Name -> [Prop] -> Validation [Name] a
validate get name =
    maybe (Failure [name]) Success . getLast . foldMap (Last . get)

-- | Validate all credentials.
validateCredentials :: [Prop] -> Validation [Name] Credentials
validateCredentials ps =
    Credentials <$> validateBintrayCredentials ps

-- | Validate optional Bintray credentials.
validateBintrayCredentials :: [Prop] -> Validation [Name] (Maybe BintrayCredentials)
validateBintrayCredentials ps = optional $
    BintrayCredentials
        <$> validate (preview _PropBintrayUsername) NameBintrayUsername ps
        <*> validate (preview _PropBintrayKey) NameBintrayKey ps

-- | Validate the Bintray repository URLs.
validateRepositoryURLs :: [Prop] -> Validation [Name] RepositoryURLs
validateRepositoryURLs ps =
    RepositoryURLs
        <$> validate (preview _PropBintrayAPIURL) NameBintrayAPIURL ps
        <*> validate (preview _PropBintrayDownloadURL) NameBintrayDownloadURL ps

--------------------------------------------------------------------------------
-- Project
--------------------------------------------------------------------------------

masterFile :: FilePath -> FilePath
masterFile = (</> "da.yaml")

-- | Find the root directory of the project to which the given directory belongs to.
--
-- The project root directory is a directory containing the da.yaml file that is
-- either the given directory or one of its ancestors.
--
-- If the project root directory exists then it is returned wrapped in @Just@, otherwise
-- @Nothing@ is returned.
--
-- Note: this function does not check whether the da.yaml file is well formatted.
findProjectRootDir :: MonadFS m
                   => FilePath            -- ^ the starting directory
                   -> m (Maybe FilePath)
findProjectRootDir startingDir =
    findMOf each (testfile' . masterFile) candidateDirs
  where
    candidateDirs = startingDir : (startingDir ^. unfolded justParentOrNothing)
    -- | the exit condition is d == p because the parent of the root is the root itself
    justParentOrNothing d = let p = F.parent d in if d == p then Nothing else Just ([p], p)

-- | Like 'findProjectRootDir' but returns the master file of the project instead of the
-- root directory.
findProjectMasterFile :: FilePath         -- ^ the starting directory
                      -> IO (Maybe FilePath)
findProjectMasterFile startingDir = fmap masterFile <$> findProjectRootDir startingDir

-- | Like 'findProjectRootDir' but also loads the properties from the master file.
--
-- Returns either a failure or a pair composed by the root directory and the properties
-- loaded.
findProjectRootDirAndLoadProps :: MonadFS m
                               => FilePath   -- ^ the starting directory
                               -> m (Either ProjectMessage (FilePath, Props))
findProjectRootDirAndLoadProps candidateDir = findProjectRootDir candidateDir >>= \case
    Nothing -> return $ Left $ M.Warning ProjectWarningNoProjectFound
    Just rootDir -> do
        let candidateFile = masterFile rootDir
        decodeYamlFile' candidateFile >>= \case
            Left _e ->
                pure $ Left $ M.Error $ ProjectErrorMalformedYAMLFile candidateFile
            Right props -> return $ Right (candidateDir, props)

-- | Load a 'Project' by
--
--     1. finding the root directory from the current working directory;
--     2. loading the project properties from the master file and merge them with the
--        default properties and global properties (priority: default < global < project);
--     3. convert the properties to a 'Project' and in the meanwhile validate them
--
-- The global properties must be passed as first argument.
--
-- The result can either be the reason why the project cannot be loaded or the project
-- fully loaded.
getProject :: (MockIO m, MonadFS m)
           => [Prop]                             -- ^ the global properties to add to the project
           -> m (Either ProjectMessage Project) -- ^ either an error or the project
getProject globalProps =
    runExceptT $ do
        candidateDir <- withExceptT (\_ -> M.Error ProjectErrorCannotPwd) $ ExceptT pwd'
        (projectDir, Props projectProps) <- ExceptT $ findProjectRootDirAndLoadProps candidateDir
        defaultProps <- lift getDefaultProjectProps
        darDeps <- getProjectDarDependencies projectDir
        withExceptT (M.Error . ProjectErrorMissingProps) $ ExceptT $ pure
            $ propsToProject projectDir darDeps (defaultProps <> globalProps <> projectProps)
    -- TODO: Throw custom exception

-- | Convert a list of ordered properties to a fully specified 'Project' or
-- return a list of the the names of properties that are missing.
propsToProject :: FilePath -> [FilePath] -> [Prop] -> Either [Name] Project
propsToProject projectDir darDeps ps = case validated of
    Failure names -> Left names
    Success conf  -> Right conf
  where
    validated :: Validation [Name] Project
    validated = Project projectDir
        <$> validate (preview _PropProjectName) NameProjectName ps
        <*> validate (preview _PropProjectSDKVersion) NameProjectSDKVersion ps
        <*> validate (preview _PropDAMLSource) NameDAMLSource ps
        <*> validate (preview _PropDAMLScenario) NameDAMLScenario ps
        <*> validate (preview _PropProjectParties) NameProjectParties ps
        <*> pure darDeps

configHelp :: P.Doc ann
configHelp =
    P.reflow "The following properties are configurable in a da.yaml file:"
        P.<> P.hardline P.<> prettyNameInfos (map nameInfo [minBound..maxBound])

decideUpdateToWhichVersion :: (Monad m) => UpdateDecisionHandle m -> BuildVersion -> m (Either SelfUpgradeError (Maybe BuildVersion))
decideUpdateToWhichVersion decisionH currentVsn = runExceptT $ do
  if currentVsn == HeadVersion
  then return Nothing
  else do
    updateS <- lift $ getUpdateSetting decisionH
    case updateS of
      Never  -> return Nothing
      Always -> do
        newest  <- ExceptT $ getLatestAvailVsn decisionH
        if currentVsn /= newest
        then do
            lift $ presentVsnInfo decisionH newest
            return $ Just newest
        else return Nothing
      RemindLater -> do
        timePassed <- ExceptT $ checkNotificationInterval decisionH
        newest     <- ExceptT $ getLatestAvailVsn decisionH
        if timePassed && (currentVsn /= newest)
        then askUser newest
        else return Nothing
      RemindNext  -> do
        newest   <- ExceptT $ getLatestAvailVsn decisionH
        notified <- ExceptT $ getLastNotifiedVsn decisionH
        if notified /= newest
        then askUser newest
        else return Nothing
  where
    askUser vsn = do
        lift $ presentVsnInfo decisionH vsn
        userAnswer <- ExceptT $ askUserForApproval decisionH vsn
        case userAnswer of
            YesAndAskNext      -> do
                ExceptT $ setLastNotifiedVsn decisionH vsn
                return $ Just vsn
            Remembered setting -> do
              ExceptT $ setUpdateSetting decisionH setting
              case setting of
                Always         ->
                    return $ Just vsn
                Never          ->
                    return Nothing
                RemindLater    -> do
                    ExceptT $ saveNotificationTime decisionH
                    return Nothing
                RemindNext     -> do
                    ExceptT $ setLastNotifiedVsn decisionH vsn
                    return Nothing

--------------------------------------------------------------------------------
-- Pretty printing
--------------------------------------------------------------------------------

prettyNameInfos :: [NameInfo] -> P.Doc ann
prettyNameInfos = P.vcat . map prettyNameInfo

prettyNameInfo :: NameInfo -> P.Doc ann
prettyNameInfo NameInfo{..} = maybe mempty (formatLine infoKeys) infoHelp
  where
    formatLine keys help =
        P.pretty (infoKeysToPath keys)
            <> P.column (\col -> P.indent (25 - col) (P.reflow help))

prettyNames :: [Name] -> P.Doc ann
prettyNames = prettyNameInfos . map nameInfo

prettyConfError :: ConfError -> P.Doc ann
prettyConfError = \case
    ConfErrorMissingFile configFilePath -> P.vsep
        [ P.sep
            [ "Can't find config file: "
            , P.p configFilePath
            ]
        , "Please run 'da setup' to setup your da tools correctly."
        ]
    ConfErrorMalformedFile configFilePath -> P.vsep
        [ P.sep
           [
             "Invalid config file: "
           , P.p configFilePath
           ]
        , "Please check or remove the file and run 'da setup' to setup your tools correctly."
        ]
    ConfErrorMissingProperties names -> P.vsep
        [ "Missing configuration properties: "
        , prettyNames names
        , "Please run 'da setup' to configure the missing properties."
        ]

prettyProjectError :: ProjectError -> P.Doc ann
prettyProjectError = \case
    ProjectErrorMalformedYAMLFile path -> P.fillSep
        [ "da.yaml found at"
        , P.p path
        , ", but could not be parsed."
        ]
    ProjectErrorMissingProps names -> P.sep
        [ P.s "Missing configuration properties for project:"
        , prettyNames names
        , P.s "Please specify these missing properties in da.yaml file in the project folder."
        ]
    ProjectErrorDarDepMalformedFile path err -> P.fillSep
        [ "malformed DAR dependency descriptor file at"
        , P.p path
        , ", error:"
        , P.t err
        ]
    ProjectErrorDarDepFilePathClash name path1 path2 -> P.fillSep
        [ "A name from DAR dependency descriptor file appears with multiple " <>
          "different dependency paths"
        , P.p path1
        , "and"
        , P.p path2
        , "Name:"
        , P.t name
        ]
    ProjectErrorCannotPwd -> P.t "Cannot get current working directory."

prettyProjectWarning :: ProjectWarning -> P.Doc ann
prettyProjectWarning = \case
    ProjectWarningNoProjectFound -> P.t "Couldn't find any project"

--------------------------------------------------------------------------------
-- Instances
--------------------------------------------------------------------------------

instance P.Pretty ConfError where
    pretty = prettyConfError

instance P.Pretty ProjectError where
    pretty = prettyProjectError

instance P.Pretty ProjectWarning where
    pretty = prettyProjectWarning
