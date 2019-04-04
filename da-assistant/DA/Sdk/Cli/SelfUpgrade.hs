-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | This module defines a function that attempts to do a self-upgrade. It will
-- not attempt one if we recently checked for an upgrade. It catches any
-- exception raised so even if the upgrade attempt fails with an exception (no
-- internet connection for example) that will be caught. Thus, this function
-- should never expose any exceptions.

module DA.Sdk.Cli.SelfUpgrade
  ( attemptSelfUpgrade
  , displayUpdateChannelInfo
  , displayUpdateChannelNote
  ) where

import           Control.Exception.Safe        (MonadThrow)
import           DA.Sdk.Cli.Conf.Types         (Conf(..), UpdateSetting(..))
import           DA.Sdk.Cli.SelfUpgrade.Types
import           DA.Sdk.Cli.Command.Config     (runConfigSet, runConfigGet')
import qualified DA.Sdk.Cli.Monad.Locations    as L
import           DA.Sdk.Cli.Locations          (FilePathOfDaBinary, pathOfToString, pathOfToString,
                                                pathOfToText, unwrapFilePathOf, IsFilePath, FilePathOf)
import           DA.Sdk.Cli.Monad
import qualified DA.Sdk.Cli.Monad.Timeout      as TO
import           DA.Sdk.Cli.OS                 (OS (..), currentOS)
import           DA.Sdk.Cli.Conf               (decideUpdateToWhichVersion, defaultPropSelfUpdateVal)
import qualified DA.Sdk.Cli.Repository         as Repo
import qualified DA.Sdk.Cli.Repository.Bintray as Bintray
import qualified DA.Sdk.Cli.Repository.Types   as RTy
import           DA.Sdk.Cli.Monad.Repository
import           DA.Sdk.Cli.Version            (version)
import           DA.Sdk.Version
import           DA.Sdk.Prelude
import qualified DA.Sdk.Pretty                 as P
import qualified Data.Text.Extended            as T
import           Data.Time.Clock               (diffUTCTime)
import           DA.Sdk.Cli.System             (executeFile, runInstaller)
import           Data.Time.Clock.POSIX         (getPOSIXTime)
import qualified Data.Yaml                     as Yaml
import           System.IO.Error
import           Turtle                        hiding (env)
import           DA.Sdk.Cli.Monad.UserInteraction
import           DA.Sdk.Cli.Monad.MockIO
import qualified Control.Monad.Logger as L
import           Control.Monad.Reader
import           Control.Monad.Trans.Except
import           DA.Sdk.Cli.Monad.FileSystem   hiding (writeFile)
import qualified DA.Sdk.Cli.Monad.FileSystem   as FS
import           Control.Exception.Safe        hiding (catchJust)
import           Data.Bifunctor (first)
import           Data.Text.Encoding (encodeUtf8)
-- | Attempt upgrade and catch all exceptions. We never want the CLI to crash
-- because of a failed upgrade attempt, but we log a warning if that happens.
attemptSelfUpgrade :: (TO.MonadTimeout m, MonadRepo m,
                       MockIO m, L.MonadLogger m, MonadUserInput m, MonadMask m,
                       MonadUserOutput m, MonadFS m, L.MonadLocations m)
                   => Env -> FilePath -> Bool -> Bool -> m (Either SelfUpgradeError ())
attemptSelfUpgrade env effCfgPath shouldLogError force = runExceptT $
    if force
    then do
            -- Check for update channel. Default is Production for users with DA email addresses.
      let updateChannel = getChannel $ getUpdateChannel conf
      repoHandle <- lift $ makeHandle conf

      let package = channelToAssistantPackage updateChannel

      actualVsn  <- maybe (throwE (BadLocalVersion version)) return (parseBuildVersion version)
      latestVsn  <- ExceptT $ fetchLatestVersion 10000 repoHandle package
      when (actualVsn /= HeadVersion && actualVsn /= latestVsn) $
          ExceptT $ performSelfUpgrade conf updateChannel latestVsn
    else lift $ attemptAutoSelfUpgrade conf effCfgPath shouldLogError
  where
    conf = envConf env

attemptAutoSelfUpgrade :: (MonadMask m, MonadUserOutput m, MonadUserInput m,
                           MonadRepo m, TO.MonadTimeout m, MonadFS m,
                           MockIO m, L.MonadLogger m, L.MonadLocations m)
                       => Conf -> FilePath -> Bool -> m () -- TODO (GH): We could propagate these errors
attemptAutoSelfUpgrade conf effCfgPath shouldLogError = do
    seconds <- getSecondsSinceLastCheck
    errOrOk <- decideSelfUpgrade conf effCfgPath seconds
    case errOrOk of
      Left (Timeout t)
        -> logDebug $ "Self-upgrade: Version check timed out after " <> T.show t <> " ms."
      Left (BadLocalVersion v)
        -> logWarn $ "Self-upgrade: Can't parse current version: " <> v
      Left (UnknownOS os)
        -> logWarn $ "Self-upgrade: " <> os <> " is not a supported OS."
      Left (SetConfigError sce)
        -> logWarn $ "Self-upgrade: There was an error when trying to set the update-setting value in the config: "
                          <> sce
      Left (MigrationError _mige) -- TODO (GH): Print 'mige' as well.
        -> logWarn "Self upgrade: Failed to migrate the config file."
      Left (UserApprovalError (ReadInputLineFailed ioErr))
        -> logWarn $ "Self upgrade: Failed to read user input: " <> T.show ioErr
      Left (LastCheckFileError (TouchFailed _f fErr))
        -> logWarn $ "Self upgrade: Failed to create the last check file: " <> T.show fErr
      Left (RepoError re)
        -> when shouldLogError $ logError $ "Self-upgrade:\n" <> P.renderPlain (P.pretty re)
      Left (CannotSaveNotifTime _)
        -> logWarn "Self upgrade: Unable to save notification time."
      Left (CannotSaveLastNotifVsn _)
        -> logWarn "Self upgrade: Unable to save last version the user was notified about."
      Left (CannotCreateTmpDir _)
        -> logWarn "Self upgrade: Cannot create temporary directory."
      Left (CannotReadLastNotifVsn _)
        -> logWarn "Self upgrade: Unable to read last version the user was notified about."
      Left (CannotCheckInterval _)
        -> logWarn "Self upgrade: Cannot check whether the notification interval has passed."
      Right ()
        -> return ()

-- | Perform an upgrade if necessary
decideSelfUpgrade :: forall m. (MonadFS m, MonadRepo m, MonadUserOutput m, MonadUserInput m,
                      TO.MonadTimeout m, L.MonadLocations m,
                      MockIO m, MonadMask m, L.MonadLogger m)
                  => Conf -> FilePath -> Maybe Int -> m (Either SelfUpgradeError ())
decideSelfUpgrade conf effCfgPath mbSecondsSinceLastCheck =
    case mbSecondsSinceLastCheck of
      Nothing                    ->
        -- Set timeout to 10s if we don't know when we last checked for an upgrade
        decideSelfUpgradeWithDialog 10000
      Just secondsSinceLastCheck -> do
        let upgradeIntervalSeconds = confUpgradeIntervalSeconds conf
        if secondsSinceLastCheck < upgradeIntervalSeconds
          then Right <$> logDebug "Less than one hour since we last checked for update. Skipping."
          else decideSelfUpgradeWithDialog (if secondsSinceLastCheck < 3 * upgradeIntervalSeconds then 5000 else 10000) -- 5s or 10s timeout
  where
    twoDays :: Int
    twoDays = 60 * 60 * 24 * 2
    readFileSafely :: (MonadFS m, IsFilePath a) => FilePathOf a -> m (Either ReadFileUtf8Failed String)
    readFileSafely file = do
      errOrTxt <- readFileUtf8 file
      case errOrTxt of
        Left (ReadFileUtf8Failed (ReadFileFailed _f err0)) | isDoesNotExistError err0 ->
          return $ Right ""
        Left e0 ->
          return $ Left e0
        Right txt ->
          return $ Right $ T.unpack txt
    setLastNotifiedVsnImpl :: BuildVersion -> m (Either SelfUpgradeError ())
    setLastNotifiedVsnImpl vsn = do
      file <- L.getLastNotifiedVsnFilePath
      first CannotSaveLastNotifVsn <$> FS.writeFile file (encodeUtf8 $ formatVersion vsn)
    getLastNotifiedVsnImpl :: BuildVersion -> m (Either SelfUpgradeError BuildVersion)
    getLastNotifiedVsnImpl currentVsn = runExceptT $ do
      file    <- L.getLastNotifiedVsnFilePath
      content <- withExceptT CannotReadLastNotifVsn $ ExceptT $ readFileSafely file
      case parseVersion $ T.pack content of
        Just vsn -> return vsn
        Nothing  -> return currentVsn
        -- ^ We either do not have the file yet or it is of the wrong format.
        -- The next call for `setLastNotifiedVsnImpl` will save a right file.
    getUpdateSettingImpl :: (MonadFS m, MockIO m, L.MonadLogger m) => m UpdateSetting
    getUpdateSettingImpl = do
      errOrYamlVal <- runConfigGet' effCfgPath (Just "update-setting")
      case errOrYamlVal of
        Right (Yaml.String str) -> do
            let setting = fromMaybe defaultPropSelfUpdateVal $ readMay $ T.unpack str
            logDebug ("Update setting to be used: " <> str <> " " <>
                      "(config has value '" <> T.show setting <> "')")
            return setting
        _notFound               -> do
            logDebug ("Config key update-setting not found. Using default: " <>
                      T.show defaultPropSelfUpdateVal)
            return defaultPropSelfUpdateVal -- error, we use the default
    saveNotificationTimeImpl :: (MockIO m) => m (Either SelfUpgradeError ())
    saveNotificationTimeImpl = do
      t    <- mockGetTime getPOSIXTime
      file <- L.getLastUpgradeReminderTimeFilePath
      first CannotSaveNotifTime <$>  FS.writeFile file (encodeUtf8 $ T.show (ceiling t :: Integer))
    checkNotificationIntervalImpl :: (MockIO m) => m (Either SelfUpgradeError Bool)
    checkNotificationIntervalImpl = runExceptT $ do
      t      <- ceiling <$> mockGetTime getPOSIXTime
      file   <- L.getLastUpgradeReminderTimeFilePath
      secStr <- withExceptT CannotCheckInterval $ ExceptT $ readFileSafely file
      case readMay secStr of
        Just t0 -> return (t - t0 > twoDays)
        Nothing -> return True
        -- ^ We either do not have the file yet or it is of the wrong format.
        -- The next call for `saveNotificationTimeImpl` will save a right file.
    askUserForApprovalImpl :: (MonadUserOutput m, MonadUserInput m) => BuildVersion -> m (Either SelfUpgradeError UpdateChoice)
    askUserForApprovalImpl vsn = runExceptT $ do
      display ("Do you want to install the new SDK Assistant version " <>
           showBuildVersion vsn <> "? (Answer with a number, 1..5)")
      display "(1) Yes, always install new versions"
      display "(2) Yes, but ask again for next version"
      display "(3) No, but ask again for next version"
      display "(4) No, but ask again for this version"
      display "(5) No, never install new versions"
      answer <- withExceptT UserApprovalError $ ExceptT readInputLine
      case answer of
        "1" -> return $ Remembered Always
        "2" -> return YesAndAskNext
        "3" -> return $ Remembered RemindNext
        "4" -> return $ Remembered RemindLater
        "5" -> return $ Remembered Never
        _   -> ExceptT $ askUserForApprovalImpl vsn
    decideSelfUpgradeWithDialog :: (MockIO m, MonadFS m, MonadRepo m, MonadUserOutput m,
                                    TO.MonadTimeout m, MonadUserInput m,
                                    L.MonadLogger m, MonadMask m)
                                => Int -> m (Either SelfUpgradeError ())
    decideSelfUpgradeWithDialog timeOutMs = runExceptT $ do
      repoHandle     <- lift $ makeHandle conf
      currentVersion <- maybe (throwE (BadLocalVersion version)) return (parseBuildVersion version)
      -- Check for update channel. Default is Production for users with DA email addresses.
      let updateChannel = getChannel $ getUpdateChannel conf

      let package              = channelToAssistantPackage updateChannel
          updateDecisionHandle = UpdateDecisionHandle
            { getUpdateSetting          = getUpdateSettingImpl
            , setUpdateSetting          = \setting -> first SetConfigError <$> runConfigSet (Just effCfgPath) "update-setting" (T.show setting)
            , getCurrentVsn             = (return . maybe (Left $ BadLocalVersion version) Right) (parseBuildVersion version)
            , getLatestAvailVsn         = fetchLatestVersion timeOutMs repoHandle package
            , getLastNotifiedVsn        = getLastNotifiedVsnImpl currentVersion
            , setLastNotifiedVsn        = setLastNotifiedVsnImpl
            , saveNotificationTime      = saveNotificationTimeImpl
            , checkNotificationInterval = checkNotificationIntervalImpl
            , presentVsnInfo            = \vsn -> display ("Version of SDK Assistant to be installed: " <>
                                                               showBuildVersion vsn) -- TODO (GH): We need a proper printout here
            , askUserForApproval        = askUserForApprovalImpl
            }
      mbVsn <- ExceptT $ decideUpdateToWhichVersion updateDecisionHandle currentVersion
      case mbVsn of
        Just vsn ->
          ExceptT $ performSelfUpgrade conf updateChannel vsn
        Nothing ->
          return ()

performSelfUpgrade :: (MonadMask m, L.MonadLogger m, MonadUserOutput m,
                       MonadRepo m, MonadFS m, MockIO m, L.MonadLocations m)
                   => Conf -> UpdateChannel -> BuildVersion -> m (Either SelfUpgradeError ())
performSelfUpgrade conf updateChannel vsn = runExceptT $ do
  -- Set the last check file first. We do not want to retry continuously
  -- if the server is down.
  _ <- ExceptT setLastCheck
  repoH <- lift $ makeHandle conf

  display "Performing self-update. For more info on updates type `da update-info`."
  unless (updateChannel == Production)
    $ logInfo ("Assistant self-update channel: " <> T.show updateChannel <> ".")
  display $ "Found latest version: " <> showBuildVersion vsn
  display "Downloading installer."

  daBin <- lift L.getDaBinPath
  withExceptT CannotCreateTmpDir $ ExceptT $ FS.withSystemTempDirectory' "da-cli" $ \tmpD ->
    downloadAndRunInstaller repoH updateChannel currentOS vsn tmpD daBin
  -- Replace process by running the current command using the new version.
  display "Update installed. Re-launching the CLI."
  -- Set last check here because re-launching will exit out of this process.
  args <- mockGetArgs arguments
  logDebug ("CLI binary: " <> pathOfToText daBin <> " Arguments: " <> T.show args)
  mockIO_ $ executeFile (pathOfToString daBin) (fmap T.unpack args) Nothing

-- | What is the current update channel? If the the config key `cli.update-channel` exists,
-- then that value is used, otherwise it's "pre" if the user has @digitalasset.com email address,
-- and "production" otherwise.
getUpdateChannel :: Conf -> UpdateChannelWithReason
getUpdateChannel conf = do
  let defaultChannel = getDefaultChannel conf

  maybe defaultChannel (withConfigSetReason . parseUpdateChannel) (confUpdateChannel conf)
  where
    withConfigSetReason c = UpdateChannelWithReason c ReasonConfigSet

-- | Calculate the default channel.
-- "pre" for @digitalasset.com email addresses, Production otherwise
getDefaultChannel :: Conf -> UpdateChannelWithReason
getDefaultChannel Conf{..} =
    if maybe False ("@digitalasset.com" `T.isSuffixOf`) confUserEmail
        then UpdateChannelWithReason (AltUpdateChannel "pre") ReasonDaEmail
        else UpdateChannelWithReason Production ReasonDefault

-- | displayStr information about the current update channel and the reason behind it to the user.
displayUpdateChannelInfo :: MonadUserOutput m => Conf -> m ()
displayUpdateChannelInfo conf = do
  display "The SDK Assistant has a built-in auto-update feature, which makes it possible"
  display "to keep it up to date with upcoming DAML SDK releases,"
  display "without you having to worry about it."
  display "A channel is where we look for updates for the SDK Assistant."
  display "The \"production\" channel has gotten the full testing and blessing"
  display "of the DA Product Team. To see what's next, with minimal risk, you can change"
  display "to the \"pre\" release channel, which brings a preview of upcoming releases."
  displayNewLine
  display "Please note that the SDK Assistant updates are independent from SDK releases."
  displayNewLine
  displayInfo $ getUpdateChannel conf
  where
    displayInfo (UpdateChannelWithReason _ ReasonDefault) = do
      display "You're subscribed to the default, production update channel."
    displayInfo (UpdateChannelWithReason channel reason)  = do
      let defaultChannelWithReason = getDefaultChannel conf
          defaultChannel  = getChannel defaultChannelWithReason
          defaultChannelT = T.show defaultChannel
          channelT        = T.show channel

      displayNewLine
      case reason of
        ReasonDaEmail -> do
          display $ "You're subscribed to the update channel \"" <> channelT <> "\"."
          displayNewLine
          display "This is the default for DA employees, so you have a chance to try out"
          display "and provide early feedback on the Assistant updates before they are released to the public."
          displayNewLine
          display "If you'd like to switch back to the \"production\" channel, "
          display "type `da config set cli.update-channel production`."
          displayNewLine
          display "Then, you can switch to the latest production release by running `da upgrade`."
        ReasonConfigSet | channel /= defaultChannel -> do
          display $ "You're subscribed to the update channel \"" <> channelT <> "\"."
          display "The reason for this is that you have it set up in your `~/.da/da.yaml`, such as:"
          displayNewLine
          display $ "cli:\n\
                \  script-mode: false\n\
                \  update-channel: " <> channelT <> " # <----"
          displayNewLine
          display $ "To switch back to the default update channel (\"" <> defaultChannelT <> "\"), " <>
            "remove the aforementioned `cli.update-channel` setting."
          display $ "Then, you can switch to the latest release of \"" <> defaultChannelT <> "\" " <>
             "by running `da upgrade`."

        _ -> return () -- no need to displayStr info text for the default case

-- | Displays a short note if an alternative update channel is used.
displayUpdateChannelNote :: CliM ()
displayUpdateChannelNote = do
  env <- ask
  let conf = envConf env
  case getUpdateChannel conf of
      UpdateChannelWithReason (AltUpdateChannel c) _ -> do
          displayNewLine
          display $ "SDK Assistant self-update channel: \"" <> c <> "\". Type `da update-info` for more information."
      _                                              -> return ()


-- | How many seconds ago did we last successfully check and, if applicable,
-- install a newer version of the da tool?
getSecondsSinceLastCheck :: (L.MonadLogger m, MockIO m, MonadFS m, L.MonadLocations m)
                         => m (Maybe Int)
getSecondsSinceLastCheck = L.getLastCheckPath >>= FS.testfile >>= \case
  False -> do
    logDebug "No upgrade check file found."
    return Nothing
  True -> do
    errOrCheckedAt <- FS.datefile =<< L.getLastCheckPath
    case errOrCheckedAt of
      Left _err ->
        return Nothing
      Right checkedAt -> do
        now <- mockGetTime' date
        let seconds = round (realToFrac (diffUTCTime now checkedAt) :: Double)
        logDebug $ "Last upgrade check " <> T.show seconds <> " seconds ago."
        return $ Just seconds

-- | Set timestamp on last successful check file.
setLastCheck :: (MockIO m, MonadFS m, L.MonadLocations m) => m (Either SelfUpgradeError ())
setLastCheck = L.getLastCheckPath >>= fmap (first LastCheckFileError) . touch' . unwrapFilePathOf

fetchLatestVersion :: TO.MonadTimeout m => Int -> Repo.Handle m -> RTy.Package -> m (Either SelfUpgradeError BuildVersion)
fetchLatestVersion timeoutMilliseconds repoH package =
    TO.timeout (TO.milliseconds timeoutMilliseconds) (Repo.hLatestPackageVersion repoH package) >>= \case
        Nothing ->
          return $ Left $ Timeout timeoutMilliseconds
        Just (Left er) ->
          return $ Left $ RepoError er
        Just (Right v) ->
          return $ Right v

downloadAndRunInstaller ::
       (MockIO m, MonadThrow m)
    => Repo.Handle m
    -> UpdateChannel
    -> OS
    -> BuildVersion
    -> FilePath
    -> FilePathOfDaBinary
    -> m ()
downloadAndRunInstaller repoH updateChannel os vers tempDir daBin = do
    installerPath <- Repo.hDownloadSdkAssistant repoH package os vers tempDir >>= \case
        Left er -> throwM $ RepoError er
        Right p -> pure $ pathToString p
    mockIO_ $ runInstaller (fromString installerPath) daBin
  where
    package = channelToAssistantPackage updateChannel

channelToAssistantPackage :: UpdateChannel -> RTy.Package
channelToAssistantPackage (AltUpdateChannel channel) = Bintray.sdkAssistantAltPackage channel
channelToAssistantPackage Production                 = Bintray.sdkAssistantPackage

parseUpdateChannel :: Text -> UpdateChannel
parseUpdateChannel "production" = Production
parseUpdateChannel other        = AltUpdateChannel other
