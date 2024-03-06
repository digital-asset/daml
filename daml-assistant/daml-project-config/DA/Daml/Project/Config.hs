-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Project.Config
    ( DamlConfig
    , ProjectConfig
    , SdkConfig
    , projectConfigExistsWithEnv
    , readSdkConfig
    , readProjectConfig
    , readDamlConfig
    , readMultiPackageConfig
    , releaseVersionFromProjectConfig
    , sdkVersionFromSdkConfig
    , listSdkCommands
    , queryDamlConfig
    , queryProjectConfig
    , querySdkConfig
    , queryMultiPackageConfig
    , queryDamlConfigRequired
    , queryProjectConfigRequired
    , querySdkConfigRequired
    , queryMultiPackageConfigRequired
    ) where

import DA.Daml.Project.Consts
import DA.Daml.Project.Types
import DA.Daml.Project.Util
import Data.Aeson (Result (..), fromJSON)
import qualified Data.Aeson.Key as A
import qualified Data.Array as Array
import Data.Bifunctor (bimap)
import Data.Generics.Uniplate.Data (transformM, universe)
import qualified Data.Text as T
import Data.Text (Text)
import qualified Data.Yaml as Y
import Data.Yaml ((.:?))
import Data.Either.Extra
import Data.Foldable
import System.Environment
import System.FilePath
import Control.Exception.Safe
import Control.Monad.IO.Class
import Control.Monad.Trans.Except
import Text.Regex.TDFA

-- | Read daml config file.
-- Throws a ConfigError if reading or parsing fails.
readDamlConfig :: DamlPath -> IO DamlConfig
readDamlConfig (DamlPath path) = readConfig "daml" (path </> damlConfigName)

-- | Read project config file.
-- Throws a ConfigError if reading or parsing fails.
readProjectConfig :: ProjectPath -> IO ProjectConfig
readProjectConfig (ProjectPath path) = readConfigWithEnv "project" (path </> projectConfigName)

-- | Checks if a project config contains environment variables.
projectConfigExistsWithEnv :: ProjectPath -> IO Bool
projectConfigExistsWithEnv (ProjectPath path) = configExistsWithEnv (path </> projectConfigName)

-- | Read sdk config file.
-- Throws a ConfigError if reading or parsing fails.
readSdkConfig :: SdkPath -> IO SdkConfig
readSdkConfig (SdkPath path) = readConfig "SDK" (path </> sdkConfigName)

-- | Read multi package config file.
-- Throws a ConfigError if reading or parsing fails.
readMultiPackageConfig :: ProjectPath -> IO MultiPackageConfig
readMultiPackageConfig (ProjectPath path) = readConfigWithEnv "multi-package" (path </> multiPackageConfigName)

-- | (internal) Helper function for defining 'readXConfig' functions.
-- Throws a ConfigError if reading or parsing fails.
readConfig :: Y.FromJSON b => Text -> FilePath -> IO b
readConfig name path = do
    configE <- Y.decodeFileEither path
    fromRightM (throwIO . ConfigFileInvalid name) configE

-- Substring for text (inclusive start, exclusive end)
textSub :: Int -> Int -> Text -> Text
textSub start end = T.take (end - start) . T.drop start

data SubMatch = SubMatch
  { smText :: Text
  , smStart :: Int
  , smEnd :: Int
  , smLength :: Int
  }

-- TDFA has you working with less than ideal data types
-- transform them into something more friendly
transformSubmatch :: Array.Array Int (MatchText Text) -> [[SubMatch]]
transformSubmatch = fmap (fmap toSubMatch . Array.elems) . Array.elems
  where
    toSubMatch :: (Text, (Int, Int)) -> SubMatch
    toSubMatch (smText, (smStart, smLength)) = let smEnd = smStart + smLength in SubMatch {..}

-- Finds any ${...} where the dollar isn't preceeded by (an odd number of) '\'
-- Replaces the inner name with the value provided by the below mapping
-- Note that `.` in the name are replaced with `_`, to allow for future support for heirarchical names
interpolateEnvVariables :: [(String, String)] -> T.Text -> Either String T.Text
interpolateEnvVariables env str = do
  let matchPositions :: [[SubMatch]] = transformSubmatch $ getAllTextMatches (str =~ ("(^|[^\\\\])(\\\\*)\\${([^}]+)}" :: String))
      -- First 2 matches aren't needed (whole match + start of line check)
      --   TDFA doesn't support non capturing groups, which is why that match exists in the first place
      replaceWithPrefix (s, start) [_, _, escapeSlashes, name] = do
        -- Divide by 2 and floor to get the number of `\` that should be in the final string
        let finalSlashesText = T.replicate (smLength escapeSlashes `div` 2) "\\"
            prefix = textSub start (smStart escapeSlashes) str
        if smLength escapeSlashes `mod` 2 == 1 -- Odd number of backslashes, thiss expression is escaped
          then pure
                ( s <> prefix
                    <> finalSlashesText
                    <> textSub (smEnd escapeSlashes) (smEnd name + 1) str
                , smEnd name + 1
                )
          else do
            let envVarName = T.unpack $ T.replace "." "_" $ smText name
                mEnvVar = lookup envVarName env
            envVarValue <- maybeToEither ("Couldn't find environment variable " <> envVarName <> " in value " <> T.unpack str) mEnvVar
            pure 
              ( s <> prefix <> finalSlashesText <> T.pack envVarValue
              , smEnd name + 1
              )
      -- Impossible case to appease the warnings
      replaceWithPrefix _ _ = Left "Impossible incorrect regex submatches"
  (replacedPrefix, prefixEnd) <- foldlM replaceWithPrefix ("" :: Text, 0) matchPositions
  pure $ replacedPrefix <> textSub prefixEnd (T.length str) str

-- | (internal) Helper function for defining 'readXConfig' functions.
-- Throws a ConfigError if reading or parsing fails.
-- Also performs environment variable interpolation on all string fields in the form of ${ENV_VAR}.
-- TODO: ensure everything that reads the daml.yaml uses this, e.g. assistant for daml.yaml, multi-package for dependencies, etc.
readConfigWithEnv :: Y.FromJSON b => Text -> FilePath -> IO b
readConfigWithEnv name path = do
  configE <- runExceptT $ do
    (configValue :: Y.Value) <- ExceptT $ Y.decodeFileEither path
    env <- liftIO getEnvironment
    configValueTransformed <- 
      except $ flip transformM configValue $ \case
        Y.String str -> bimap Y.AesonException Y.String $ interpolateEnvVariables env str
        v -> pure v
    case fromJSON configValueTransformed of
      Success x -> pure x
      Error str -> throwE $ Y.AesonException str
  fromRightM (throwIO . ConfigFileInvalid name) configE

-- Returns false if the file doesn't exist or isn't valid yaml.
-- Does not attempt to replace values, so will not fail if the environment variables do not exist.
configExistsWithEnv :: FilePath -> IO Bool
configExistsWithEnv path =
  fmap (fromRight False) $ runExceptT $ do
    (configValue :: Y.Value) <- ExceptT $ Y.decodeFileEither path
    pure $ flip any (universe configValue) $ \case
      Y.String str -> str =~ ("(^|[^\\\\])(\\\\\\\\)*\\${[^}]+}" :: String)
      _ -> False

-- | Determine pinned sdk version from project config, if it exists.
releaseVersionFromProjectConfig :: ProjectConfig -> Either ConfigError (Maybe UnresolvedReleaseVersion)
releaseVersionFromProjectConfig = queryProjectConfig ["sdk-version"]

-- | Determine sdk version from sdk config, if it exists.
sdkVersionFromSdkConfig :: SdkConfig -> Either ConfigError SdkVersion
sdkVersionFromSdkConfig = querySdkConfigRequired ["version"]

-- | Read sdk config to get list of sdk commands.
listSdkCommands :: SdkPath -> EnrichedCompletion -> SdkConfig -> Either ConfigError [SdkCommandInfo]
listSdkCommands sdkPath enriched sdkConf = map (\f -> f sdkPath enriched) <$> querySdkConfigRequired ["commands"] sdkConf

-- | Query the daml config by passing a path to the desired property.
-- See 'queryConfig' for more details.
queryDamlConfig :: Y.FromJSON t => [Text] -> DamlConfig -> Either ConfigError (Maybe t)
queryDamlConfig path = queryConfig "daml" "DamlConfig" path . unwrapDamlConfig

-- | Query the project config by passing a path to the desired property.
-- See 'queryConfig' for more details.
queryProjectConfig :: Y.FromJSON t => [Text] -> ProjectConfig -> Either ConfigError (Maybe t)
queryProjectConfig path = queryConfig "project" "ProjectConfig" path . unwrapProjectConfig

-- | Query the sdk config by passing a list of members to the desired property.
-- See 'queryConfig' for more details.
querySdkConfig :: Y.FromJSON t => [Text] -> SdkConfig -> Either ConfigError (Maybe t)
querySdkConfig path = queryConfig "SDK" "SdkConfig" path . unwrapSdkConfig

-- | Query the multi-package config by passing a list of members to the desired property.
-- See 'queryConfig' for more details.
queryMultiPackageConfig :: Y.FromJSON t => [Text] -> MultiPackageConfig -> Either ConfigError (Maybe t)
queryMultiPackageConfig path = queryConfig "multi-package" "MultiPackageConfig" path . unwrapMultiPackageConfig

-- | Like 'queryDamlConfig' but returns an error if the property is missing.
queryDamlConfigRequired :: Y.FromJSON t => [Text] -> DamlConfig -> Either ConfigError t
queryDamlConfigRequired path = queryConfigRequired "daml" "DamlConfig" path . unwrapDamlConfig

-- | Like 'queryProjectConfig' but returns an error if the property is missing.
queryProjectConfigRequired :: Y.FromJSON t => [Text] -> ProjectConfig -> Either ConfigError t
queryProjectConfigRequired path = queryConfigRequired "project" "ProjectConfig" path . unwrapProjectConfig

-- | Like 'querySdkConfig' but returns an error if the property is missing.
querySdkConfigRequired :: Y.FromJSON t => [Text] -> SdkConfig -> Either ConfigError t
querySdkConfigRequired path = queryConfigRequired "SDK" "SdkConfig" path . unwrapSdkConfig

-- | Like 'queryMultiPackageConfig' but returns an error if the property is missing.
queryMultiPackageConfigRequired :: Y.FromJSON t => [Text] -> MultiPackageConfig -> Either ConfigError t
queryMultiPackageConfigRequired path = queryConfigRequired "multi-package" "MultiPackageConfig" path . unwrapMultiPackageConfig

-- | (internal) Helper function for querying config data. The 'path' argument
-- represents the location of the desired property within the config file.
-- For example, if you had a YAML file like so:
--
--     a:
--        b:
--           c:
--              <desired property>
--
-- Then you would pass ["a", "b", "c"] as path to get the desired property.
--
-- This distinguishes between a missing property and a poorly formed property:
--    * If the property is missing, this returns (Right Nothing).
--    * If the property is poorly formed, this returns (Left ...).
queryConfig :: Y.FromJSON t => Text -> Text -> [Text] -> Y.Value -> Either ConfigError (Maybe t)
queryConfig name root path cfg
    = mapLeft (ConfigFieldInvalid name path)
    . flip Y.parseEither cfg
    $ \v0 -> do
        let initial = (root, Just v0)
            step (p, Nothing) _ = pure (p, Nothing)
            step (p, Just v) n = do
                v' <- Y.withObject (T.unpack p) (.:? A.fromText n) v
                pure (p <> "." <> n, v')

        (_,v1) <- foldlM step initial path
        mapM Y.parseJSON v1

-- | (internal) Like 'queryConfig' but returns an error if property is missing.
queryConfigRequired :: Y.FromJSON t => Text -> Text -> [Text] -> Y.Value -> Either ConfigError t
queryConfigRequired name root path cfg = do
    resultM <- queryConfig name root path cfg
    fromMaybeM (Left $ ConfigFieldMissing name path) resultM
