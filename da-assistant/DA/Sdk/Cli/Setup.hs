-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Setup
    ( setup
    , wizard
    ) where

import qualified Control.Monad.Logger    as L
import           Control.Monad.Catch     ( MonadMask )
import qualified Data.Text               as T
import qualified Data.Text.Extended      as T
import           Data.Either.Validation
import           DA.Sdk.Cli.Conf         ( Conf(..), Name (..), Prop (..)
                                         , Props (..), ProjectMessage
                                         , getDefaultConfProps
                                         , propsToConf, staticConfProps
                                         , getProject, prettyNames
                                         , validateRepositoryURLs
                                         )
import           DA.Sdk.Cli.Conf.Migrate (updateConfigFile)
import qualified DA.Sdk.Cli.Env          as Env
import           DA.Sdk.Cli.Monad
import           DA.Sdk.Cli.Monad.MockIO
import           DA.Sdk.Cli.Monad.Repository
import qualified DA.Sdk.Cli.Sdk          as Sdk
import           DA.Sdk.Prelude
import qualified DA.Sdk.Pretty           as P
import qualified DA.Sdk.Cli.Locations    as Loc
import qualified DA.Sdk.Cli.Monad.Locations as L
import qualified DA.Sdk.Cli.Monad.FileSystem as FS
import Control.Monad.Except
import DA.Sdk.Cli.Message as M
import DA.Sdk.Cli.Monad.UserInteraction
import DA.Sdk.Cli.Binaries

data Error
    = ErrorMissingProps [Name]
    | SdkErr Sdk.SdkError
    | ProjMsg ProjectMessage
    | BinPopError BinDirPopulationError
    | MkBinDirError FS.MkTreeFailed

setup :: (MonadUserInput m, MonadUserOutput m, L.MonadLogger m, MonadMask m,
          MonadRepo m, FS.MonadFS m, MockIO m, L.MonadLocations m)
      => FilePath -> m () -- TODO improve type of input filepath
setup configFile = do
    -- TODO: This a bit messy; we should get the home dir in one canonical way
    -- at the start for everything once we allow customising it.

    -- First, ensure the SDK Assistant home dir exists
    daHomeDir <- L.getDaHomePath
    daTempDir <- L.getSdkTempDirPath
    daBinDir <- L.getDaBinDirPath
    let daPackagesDir = Loc.packagesPath daHomeDir

    result <- runExceptT $ do
        withExceptT MkBinDirError $ ExceptT $ FS.mktree daBinDir
        -- Then run the configuration wizard.
        (conf, props) <- ExceptT $ wizard configFile
        -- TODO: This should only show conditionally on if we actually
        -- download the latest SDK release or not..
        display "Downloading latest SDK release..."
        -- TODO: Move the install functions into CliM and read packages path
        -- from a monadic function to ensure consistency.
        handle <- lift $ makeHandle conf
        latestSDKVersion <- withExceptT SdkErr $ ExceptT $ Sdk.installLatestSdk handle daTempDir daPackagesDir

        withExceptT BinPopError $ ExceptT $ populateBinWithPackagesBinaries daPackagesDir daBinDir latestSDKVersion

        mockIO_ $ void $ updateConfigFile
            configFile
            (props <> [PropSDKDefaultVersion (Just latestSDKVersion)])

        -- Switch into the CLI Monad.
        -- TODO(ng): check if we need to pass props here
        errOrProject <- getProject []
        case errOrProject of
            Left er@(M.Error _) -> do
                throwError $ ProjMsg er
            -- We ignore the warning here, since it's okay to be outside of
            -- a project.
            Left (M.Warning _w) ->
                return $ Env conf Nothing
            Right p ->
                return $ Env conf $ Just p

    case result of
        Left err ->
            L.logErrorN $ showError err
        Right env ->
            mockIO_ $ runCliM afterInstall env

  where
    afterInstall :: CliM ()
    afterInstall =
        -- Check if all required binaries are on the path, warn if not.
        Env.warnOnMissingBinaries Env.requiredCoreBinaries


wizard :: (MonadUserInput m, MonadUserOutput m, L.MonadLogger m, MonadRepo m, MockIO m, FS.MonadFS m)
       => FilePath -> m (Either Error (Conf, [Prop]))
wizard configFile = do

    defaultConfProps <- getDefaultConfProps
    -- TODO(mp): standardise this way of decoding props from file
    -- including error reporting
    configExists <- FS.testfile' configFile
    props <- if configExists
        then do
            errOrProps <- FS.decodeYamlFile' configFile
            case errOrProps of
                Left errorDecoding -> do
                    logError $ P.renderPlain . P.pretty $ show errorDecoding
                    pure []
                Right props ->
                    pure $ fromProps props
        else pure []

    let extendedProps = defaultConfProps <> props

    -- The repository URLs must be set because even
    -- if they are not set via config, the system should fallback
    -- to the defaults. See decodePropsFile.
    -- We try to extract them and if it doesn't work we just return
    -- the missing repository URLs to be displayed as error to the
    -- user.
    --
    -- TODO(mp): this wouldn't require a runtime configuration if
    -- the type of props was not a list of Props. Configuration with
    -- defaults must exist and a better type would make error handling
    -- much easier.
    case validateRepositoryURLs extendedProps of
        Success _ -> do
            case propsToConf (extendedProps <> staticConfProps) of
                Left names -> pure $ Left $ ErrorMissingProps names
                Right conf -> pure $ Right (conf, extendedProps <> staticConfProps)
        Failure missingNames -> do
            pure $ Left $ ErrorMissingProps missingNames

--------------------------------------------------------------------------------
-- Pretty printing
--------------------------------------------------------------------------------

showError :: Error -> Text
showError = \case
    ErrorMissingProps names ->
        T.unlines
            [ "Something went wrong with the wizard,"
            , "some configuration properties are still missing: "
            , P.renderPlain $ prettyNames names
            ]
    SdkErr sdkErr ->
        P.renderPlain $ P.pretty sdkErr
    ProjMsg projMsg ->
        P.renderPlain $ P.pretty projMsg
    BinPopError err ->
        T.show err
    MkBinDirError (FS.MkTreeFailed f _) ->
        "Failed to create the SDK Assistant binary directory: " <> pathToText f
