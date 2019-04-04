-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Package
    ( InstallPackageFailed (..)
    , InstallPackageError (..)
    , InstallResult (..)
    , installPackage
    , getPath

    , FetchError(..)
    , fetchPackage
    ) where

import           DA.Sdk.Data.Either.Extra (fmapLeft, joinLefts)
import           Control.Monad.Except
import qualified DA.Sdk.Cli.Monad.FileSystem as FS
import           DA.Sdk.Cli.Metadata
import           DA.Sdk.Prelude
import qualified DA.Sdk.Cli.Repository         as Repo
import qualified DA.Sdk.Shell.Tar      as Tar
import qualified DA.Sdk.Version        as V
import           DA.Sdk.Cli.Locations
import qualified DA.Sdk.Pretty         as P
import           DA.Sdk.Cli.Monad.Repository
import           DA.Sdk.Cli.Conf (Conf)
import           Data.Set
import qualified Data.Text as T
import           Control.Monad.Extra

data InstallResult p
    = AlreadyInstalled (FilePathOfPackageVersion p)
    | Success (FilePathOfPackageVersion p)

getPath :: InstallResult p -> FilePathOfPackageVersion p
getPath (AlreadyInstalled p) = p
getPath (Success p) = p

data InstallPackageFailed v p = InstallPackageFailed
    { ipfRepo       :: Repo.Repository
    , ipfPkg        :: Repo.DownloadablePackage v
    , ipfSdkTmpPath :: FilePathOfSdkTmpDir
    , ipfPkgVsnPath :: FilePathOfPackageVersion p
    , ipfError      :: InstallPackageError
    } deriving (Show)

data InstallPackageError
    = RepoError          Repo.Error
    | TempDirError       FS.WithTempDirectoryFailed
    | ExtractMkTreeError FS.MkTreeFailed
    | ExtractError       FS.UnTarGzipFailed
    | DestMkTreeError    FS.MkTreeFailed
    | DestCpTreeError    FS.CpTreeFailed
    | DestCpError        FS.CpFailed
    deriving (Show)

data FetchError
    = FetchRepoError Repo.Error
    | FetchCannotCreateTargetDir FS.MkTreeFailed
    | FetchTargetDirAlreadyExists FilePath
    | FetchTarExtractionError FS.UnTarGzipFailed
    | FetchTempDirError FS.WithTempDirectoryFailed
    | FetchGetLatestVsnError Repo.Error
    | FetchTagNotFound Text
    | FetchGetTagError Repo.Error
    | FetchCannotCopyExtractedPkg FS.CpTreeFailed
    deriving (Show)

instance P.Pretty v => P.Pretty (InstallPackageFailed v p) where -- TODO: nice pretty instance
    pretty (InstallPackageFailed (Repo.Repository repo) pkg _ _ err) =
        P.sep
            [ P.t repo
            , P.pretty pkg
            , P.pretty err
            ]

instance P.Pretty InstallPackageError where
    pretty err = P.s $ show err

instance P.Pretty FetchError where
    pretty (FetchRepoError err) =
        P.pretty err
    pretty (FetchCannotCreateTargetDir (FS.MkTreeFailed f _)) =
        P.t $ "Failed to create fetch target directory: " <> pathToText f
    pretty (FetchTargetDirAlreadyExists f) =
        P.t $ "Fetch target directory already exists: " <> pathToText f
    pretty (FetchTarExtractionError (FS.UnTarGzipFailed f _ t)) =
        P.t $ "Unable to extract file: " <> pathToText f <> " Error: " <> t
    pretty (FetchTempDirError (FS.WithTempDirectoryFailed _ _ err)) =
        P.s $ "Unable to create temporary directory, error: " <> displayException err
    pretty (FetchGetLatestVsnError err) =
        P.label "Error when getting latest version of package:" $ P.pretty err
    pretty (FetchTagNotFound p) =
        P.t $ "Fetch tag not found for package: " <> p
    pretty (FetchGetTagError err) =
        P.label "Error when getting fetch tag:" $ P.pretty err
    pretty (FetchCannotCopyExtractedPkg (FS.CpTreeFailed f1 f2 _)) =
        P.t $ "Error when copying the extracted package from " <> pathToText f1 <> " to " <> pathToText f2

-- | @installPackage handle downloadablePackage installPath tempPath@
-- Installs a 'DownloadablePackage' with the help of the handler, unpacks it if its a "tar.gz"
-- and installs it to the installPath in a folder named after the version of the artifact.
installPackage :: forall m version p.
       (FS.MonadFS m, V.Versioned version)
    => Repo.Handle m
    -> Repo.Repository
    -> Repo.DownloadablePackage version
    -> FilePathOfSdkTmpDir
    -> FilePathOfPackageVersion p
    -> m (Either (InstallPackageFailed version p) (InstallResult p))
installPackage handle pkgRepo downloadablePackage tempDir installPath = runExceptT $ do
    isAlreadyInstalled <- FS.testdir installPath
    if isAlreadyInstalled
        then pure $ AlreadyInstalled installPath
        else mapExceptT (fmap joinLefts) . wrap TempDirError
           . FS.withTempDirectory tempDir "da-sdk-download" $ \installTempDir -> do
                downloadedFile <- ExceptT . wrap RepoError $
                    Repo.hDownloadPackage handle pkgRepo
                        downloadablePackage installTempDir
                case packaging of
                    PackagingTarGz _ -> do
                        let extractFolder = installTempDir </> textToPath "extract"
                        ExceptT . wrap ExtractMkTreeError $ FS.mktree' extractFolder
                        -- TODO check result!
                        _result <- ExceptT . wrap ExtractError $
                            FS.unTarGzip' (Tar.defaultUnTarGzipOptions {Tar._stripComponents = Just 1})
                                          downloadedFile
                                          extractFolder

                        ExceptT . wrap DestMkTreeError $ FS.mktree installPath
                        ExceptT . wrap DestCpTreeError $ FS.cptree extractFolder installPath
                        successfulInstall

                    PackagingJar -> do
                        copyInstall downloadedFile
                        successfulInstall

                    PackagingExtension _ext -> do
                        copyInstall downloadedFile
                        successfulInstall
  where
    wrap :: Monad m'
         => (a -> InstallPackageError)
         -> m' (Either a b)
         -> m' (Either (InstallPackageFailed version p) b)
    wrap f = fmapLeft (InstallPackageFailed pkgRepo downloadablePackage tempDir installPath . f)

    successfulInstall = pure $ Success installPath
    packaging = Repo._dpPackaging downloadablePackage

    copyInstall :: FilePath -> ExceptT (InstallPackageFailed version p) m ()
    copyInstall downloadedFile = do
        let execPath = unwrapFilePathOf installPath </> filename downloadedFile
        ExceptT . wrap DestMkTreeError $ FS.mktree installPath
        ExceptT . wrap DestCpError     $ FS.cp' downloadedFile execPath

fetchPackage :: (MonadRepo m, FS.MonadFS m)
             => FilePathOfSdkTmpDir -> Conf -> Repo.FetchArg -> Maybe FilePath -> Bool -> m (Either FetchError ())
fetchPackage tmpDir conf fetchArg mbTarget dontCheckFetchTag = runExceptT $ do
    whenM (FS.testdir' target) $
        throwError $ FetchTargetDirAlreadyExists target

    handle <- lift $ makeHandle conf

    unless dontCheckFetchTag $ do
        tags <- withExceptT FetchGetTagError $ ExceptT $ Repo.hGetPackageTags handle $ Repo.PackageName pkgName
        unless (member "fetch" tags) $ do
            throwError $ FetchTagNotFound pkgName

    vsn :: Repo.GenericVersion <- withExceptT FetchGetLatestVsnError $ ExceptT $ Repo.hLatestPackageVersion handle pkg

    let dlPackage = Repo.DownloadablePackage Nothing (PackagingTarGz TarGz) pkg vsn
    res <- FS.withTempDirectory tmpDir "fetch" $ \downloadTempDir -> do
            dlFile <- withExceptT FetchRepoError $ ExceptT $
                Repo.hDownloadPackage handle Repo.daRepository dlPackage downloadTempDir
            let extractTarget = downloadTempDir </> "extraction-dir"
            withExceptT FetchTarExtractionError $ ExceptT $
                FS.unTarGzip' (Tar.defaultUnTarGzipOptions {Tar._stripComponents = Just 1}) dlFile extractTarget
            withExceptT FetchCannotCreateTargetDir $ ExceptT $ FS.mktree' target
            withExceptT FetchCannotCopyExtractedPkg $ ExceptT $ FS.cptree' extractTarget target
    withExceptT FetchTempDirError $ ExceptT $ return res
  where
    target = fromMaybe (textToPath $ Repo.unwrapFetchName $ Repo.fetchName fetchArg) mbTarget
    fetchPrefixToGroup = T.splitOn "." . Repo.unwrapFetchPrefix
    grp = ["com", "digitalasset"] ++ maybe [] fetchPrefixToGroup (Repo.fetchPrefix fetchArg)
    pkgName = Repo.unwrapFetchName $ Repo.fetchName fetchArg
    pkg = Repo.Package pkgName grp