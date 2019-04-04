-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Command.Config
    ( runConfigGet
    , runConfigSet
    , runConfigGet'
    , listSubscription
    , subscribe
    ) where

import           DA.Sdk.Cli.Conf
import           DA.Sdk.Cli.Monad
import           DA.Sdk.Prelude
import qualified DA.Sdk.Cli.Command.Types   as Ty
import qualified DA.Sdk.Cli.Repository.Types as Repo
import qualified Data.HashMap.Strict        as HMS
import qualified Data.ByteString.Char8      as BS
import qualified Data.Text.Extended         as T
import qualified Data.Yaml                  as Yaml
import           System.Exit (exitFailure)
import qualified Data.Vector                as V
import           Control.Monad.Logger       (MonadLogger)
import           DA.Sdk.Cli.Monad.UserInteraction
import           DA.Sdk.Cli.Monad.MockIO
import qualified Control.Monad.Logger       as L
import           DA.Sdk.Cli.Monad.FileSystem
import           Control.Monad.Except

runConfigGet :: FilePath -> Maybe T.Text -> CliM ()
runConfigGet configFile mbKey = do
    mbVal <- runConfigGet' configFile mbKey
    case mbVal of
      (Right val) -> liftIO $ BS.putStrLn $ Yaml.encode val
      (Left err)  -> logError err >> liftIO exitFailure

runConfigGet' :: (MonadFS m, MockIO m, L.MonadLogger m) => FilePath -> Maybe T.Text -> m (Either T.Text Yaml.Value)
runConfigGet' configFile mbKey = runExceptT $ do
    -- Load props from global and project configuration
    globProps <- lift $ getProps configFile
    mbProjProps <- withExceptT T.show $ ExceptT getProjectProps
    let projProps = fromMaybe [] mbProjProps
        props = Props $ globProps <> projProps

    -- Convert to JSON object and lookup by key or show all if no key
    let value = Yaml.toJSON props
    case mbKey of
      Just key -> do
        let path = T.split (== '.') key
        let mbNeedle = lookupByPath path value
        case mbNeedle of
          Just needle ->
            return needle
          Nothing ->
            throwError $ "Configuration property not found for '" <> key <> "'"
      Nothing ->
        return value
  where
    lookupByPath :: [T.Text] -> Yaml.Value -> Maybe Yaml.Value
    lookupByPath [] value = Just value
    lookupByPath (k:ks) (Yaml.Object obj) = lookupByPath ks =<< HMS.lookup k obj
    lookupByPath _ _ = Nothing

runConfigSet :: (MonadLogger m, MonadFS m, MockIO m) => Maybe FilePath -> T.Text -> T.Text -> m (Either Text ())
runConfigSet maybeConfigFile mbKey val = runExceptT $ do
    newProp    <- ExceptT $ return $ pathToName mbKey >>= flip nameValToProp val
    masterFile <- maybe (ExceptT getProjectMasterFile') return maybeConfigFile
    oldProps   <- getProps masterFile
    withExceptT T.show $ ExceptT $
        encodeYamlFile' masterFile (Props (oldProps ++ [newProp]))

deleteNameSpacesConf :: Maybe FilePath -> CliM (Either Text ())
deleteNameSpacesConf maybeConfigFile = runExceptT $ do
    masterFile <- maybe (ExceptT getProjectMasterFile') return maybeConfigFile
    oldProps   <- getProps masterFile
    let oldProps' = filter matchNSProp oldProps
    liftIO $ Yaml.encodeFile (pathToString masterFile) (Props oldProps')
  where
    matchNSProp (PropNameSpaces _) = False
    matchNSProp _                  = True

listSubscription :: FilePath -> CliM (Either Text ())
listSubscription cfgFile = do
    errOrV <- runConfigGet' cfgFile $ Just namespacesKey
    case errOrV of
      Right (Yaml.Array a) -> runExceptT $ do
        nsTxts <- ExceptT $ toNameSpaceTxts a
        mapM_ display nsTxts
      Right othertype ->
        wrongCfgType othertype
      Left _keyNotFound ->
        Right <$> display "You are not subscribed to any namespaces."

subscribe :: FilePath -> Repo.NameSpace -> Ty.SubscriptionOp -> CliM (Either Text ())
subscribe cfgFile (Repo.NameSpace n) subscrOrUnsubscr = do
    errOrV <- runConfigGet' cfgFile $ Just namespacesKey
    case errOrV of
      Right (Yaml.Array a) -> runExceptT $ do
            nps <- ExceptT $ toNameSpaceTxts a
            let nps2 = if subscrOrUnsubscr == Ty.OpUnsubscribe
                       then filter (\x -> x /= n) nps
                       else nubOrd (n : nps)
                newV = toValStr nps2
            if T.null newV
            then ExceptT $ deleteNameSpacesConf (Just cfgFile)
            else ExceptT $ runConfigSet (Just cfgFile) namespacesKey newV
      Right othertype ->
            wrongCfgType othertype
      Left _keyNotFound | subscrOrUnsubscr == Ty.OpSubscribe ->
            runConfigSet (Just cfgFile) namespacesKey n
      -- unsubscribe
      Left _keyNotFound -> return $ Right ()

wrongCfgType :: (MonadLogger m, Show a) => a -> m (Either Text r)
wrongCfgType t = do
    logError ("Config file contains value of wrong type " <>
                    "for field '" <> namespacesKey <> "': " <>
                        T.show t)
    return $ Left $ "Wrong config value type for " <> namespacesKey

namespacesKey :: Text
namespacesKey = "templates.namespaces"

toValStr :: [Text] -> Text
toValStr = T.intercalate ","

toNameSpaceTxts :: MonadLogger m => V.Vector Yaml.Value -> m (Either Text [Text])
toNameSpaceTxts = runExceptT . mapM toNameSpaceTxt . V.toList
  where
    toNameSpaceTxt = \case
        Yaml.String t -> return t
        othertype     -> ExceptT $ wrongCfgType othertype

-------------
-- Helpers --

-- | Return the properties found in the given file
--
-- If the file is not there, log an error and return an empty list.
getProps :: (MockIO m, L.MonadLogger m)
         => FilePath    -- ^ the file to load for the properties
         -> m [Prop]
getProps configFile = do
    errorOrProps <- mockGetErrOrProps $ Yaml.decodeFileEither (pathToString configFile)
    case errorOrProps of
        Right p -> return $ fromProps p
        Left  e -> handleDecodeError e >> return []
  where
    handleDecodeError = logError . T.pack . Yaml.prettyPrintParseException

-- | Get the current working directory project master file if a project exists.
--
-- Return @Just@ with the master file if a project exists, @Nothing@ otherwise.
getProjectMasterFile :: (MockIO m, MonadFS m) => m (Either PwdFailed (Maybe FilePath))
getProjectMasterFile = runExceptT $ do
    p <- ExceptT pwd'
    mockProjectMasterFile $ findProjectMasterFile p

-- | Like 'getProjectMasterFile' but logs an error if a project doesn't exist.
getProjectMasterFile' :: (MockIO m, MonadFS m) => m (Either Text FilePath)
getProjectMasterFile' = runExceptT $ do -- TODO (GH) Do not use Text error.
    wd <- withExceptT (const "Unable to get current working directory.") $ ExceptT pwd'
    lift (mockProjectMasterFile $ findProjectMasterFile wd) >>= \case
        Nothing         -> throwError $ "No Project found for directory" <> pathToText wd
        Just masterFile -> return masterFile

-- | Get the current working directory project properties if a project exists.
getProjectProps :: (MockIO m, MonadFS m, L.MonadLogger m) => m (Either PwdFailed (Maybe [Prop]))
getProjectProps = runExceptT $ do
    mbFile <- ExceptT getProjectMasterFile
    maybe (return Nothing) (fmap Just . getProps) mbFile
