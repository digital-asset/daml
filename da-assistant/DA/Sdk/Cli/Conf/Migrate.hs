-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Conf.Migrate
    ( migrateConfigFile
    , backupConfigFile
    , updateConfigFile
    ) where

import           DA.Sdk.Prelude

import           Control.Lens       (preview)
import           Data.List          (isPrefixOf)
import           Data.Monoid        (Last (..), getLast)
import qualified Data.Text          as T
import qualified Data.Yaml          as Yaml
import           Turtle             ((<.>))

import           DA.Sdk.Cli.Conf    (Prop, Props (..), latestConfVersion, staticConfProps,
                                     _PropConfVersion, ConfigMigrationError(..), ConfigUpdateError(..), ConfigWriteFailed(..))
import           DA.Sdk.Cli.Monad   (logInfo)
import           DA.Sdk.Cli.System  (FileMode, ownerReadMode, ownerWriteMode, unionFileModes)
import qualified Control.Monad.Logger as L
import           DA.Sdk.Cli.Monad.FileSystem
import           Control.Monad.Trans.Except
import           Control.Monad.Trans.Class (lift)

-- | @configFileMode@ is the mode for the user's personal config file. It can
-- contain sensitive information, so should not be group or world readable.
configFileMode :: FileMode
configFileMode = unionFileModes ownerReadMode ownerWriteMode

migrateConfigFile :: (MonadFS m, L.MonadLogger m) => FilePath -> m (Either ConfigMigrationError ())
migrateConfigFile configFile = runExceptT $ do
    exists <- testfile' configFile
    when exists $ do
        ensureMode
        migrate
    ensureBackupConfigFileModes
  where
    ensureMode =
      withExceptT MigrationEnsureModeFailed $ ExceptT $ setFileMode' configFile configFileMode
    migrate :: (MonadFS m, L.MonadLogger m) => ExceptT ConfigMigrationError m ()
    migrate = do
      mbProps <- decodeYamlFile' configFile
      case fmap fromProps mbProps of
          Left _e ->
              return ()
          Right props -> 
              when (shouldMigrate props) $ do
                -- We should migrate the config, but we have already read the
                -- old version into the current format (using the deprecated
                -- parsers) so migration simply requires writing out the config
                -- file using the latest code.
                backupFile <- withExceptT MigrationBackupFailed $ ExceptT $ backupConfigFile configFile
                logInfo $ "Migrating config file. Saving old config file as " <> pathToText backupFile
                withExceptT MigrationWriteFailed $ ExceptT $
                    writeConfigFile backupFile (props <> staticConfProps)
    ensureBackupConfigFileModes = do
      -- We avoid using Turtle here because it does not support setting group
      -- and world file permissions on POSIX systems.
      let configDir = directory configFile
      allFiles <- withExceptT ConfigDirListingError $ ExceptT $ ls' configDir
      let backupConfigFilePrefix = pathToString $ filename configFile <.> "bak"
          backupConfigFileNames = filter (backupConfigFilePrefix `isPrefixOf`) $ map pathToString allFiles
      withExceptT MigrationEnsureModeFailed $ forM_ backupConfigFileNames $ \fileName ->
            ExceptT $ setFileMode' (configDir </> stringToPath fileName) configFileMode

shouldMigrate :: [Prop] -> Bool
shouldMigrate props =
    case getLast $ foldMap (Last . preview _PropConfVersion) props of
        Nothing      -> False
        Just version -> version < latestConfVersion

-- TODO: Probably move backupConfigFile and updateConfigFile to main Conf module.

backupConfigFile :: MonadFS m => FilePath -> m (Either CpFailed FilePath)
backupConfigFile configFile = runExceptT $ do
    backupFile <- findName (configFile <.> "bak") Nothing
    ExceptT $ cp' configFile backupFile
    return backupFile

updateConfigFile :: MonadFS m => FilePath -> [Prop] -> m (Either ConfigUpdateError [Prop])
updateConfigFile configFile props = runExceptT $ do
    exists <- lift $ testfile' configFile
    cfgProps <-
        if exists
        then do
            _ <- withExceptT UpdateBackupFailed $ ExceptT $ backupConfigFile configFile
            withExceptT ConfigUpdateDecodeError $ ExceptT $ decodeYamlFile' configFile
        else return $ Props []
    let allProps = fromProps cfgProps <> props
    withExceptT UpdateConfigWriteFailed $ ExceptT $ writeConfigFile configFile props
    return allProps

writeConfigFile :: MonadFS m => FilePath -> [Prop] -> m (Either ConfigWriteFailed ())
writeConfigFile configFile props = runExceptT $ do
    let yamlContent = Yaml.encode (Props props)
    withExceptT ConfigWriteFailed $ ExceptT $ writeFile' configFile yamlContent
    withExceptT ConfigWriteSetModeFailed $ ExceptT $ setFileMode' configFile configFileMode

findName :: MonadFS io => FilePath -> Maybe Int -> io FilePath
findName file Nothing = testfile' file >>= \case
    True -> findName file (Just 1)
    False -> return file
findName file (Just c) = if c > 100
    then error "Couldn't find a suitable filename for config backup."
    else do
        let file' = file <.> (T.pack $ show c)
        testfile' file' >>= \case
            True -> findName file (Just (c + 1))
            False -> return file'
