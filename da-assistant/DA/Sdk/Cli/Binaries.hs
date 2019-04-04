-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}

module DA.Sdk.Cli.Binaries
    ( getPackagePath
    , populateBinWithPackagesBinaries
    , BinDirPopulationError
    ) where

import qualified DA.Sdk.Cli.Locations as Loc
import           DA.Sdk.Prelude
import qualified DA.Sdk.Version as Version
import           DA.Sdk.Cli.Metadata
import           DA.Sdk.Cli.Monad.FileSystem as FS
import           Control.Monad.Except
import qualified Data.Map.Strict as MS
import Control.Monad.Extra (whenM)
import qualified Data.MultiMap as MM
import           DA.Sdk.Cli.System (daRelativeBinName, allBinaryExtensions, osSpecificBinaryExtensions)
import qualified Control.Monad.Logger as L
import           Turtle ((<.>), filename)
import qualified DA.Sdk.Pretty as P

getPackagePath :: Loc.DirContains ('Loc.SomePackageDir p) b
               => Loc.FilePathOfPackagesDir
               -> Text
               -> Bool
               -> Version.SemVersion
               -> Loc.FilePathOf b
getPackagePath packagesPath packageName versioned version =
    if versioned
    then Loc.addPathSuffix (textToPath $ Version.showSemVersion version) packagePathNoVsn
    else Loc.addPathSuffix "" packagePathNoVsn
  where
    packagePathNoVsn = Loc.addPathSuffix (textToPath packageName) packagesPath :: Loc.FilePathOfPackage p

data BinDirPopulationError
    = CannotListContent FS.LsFailed
    | CannotRemoveBin FS.RemoveFailed
    | CannotReadMeta ReadMetadataError
    | CannotCreateBinStarter BinaryStarterCreationFailed
    | CannotCreateBinDir FS.MkTreeFailed
    deriving (Show)

instance P.Pretty BinDirPopulationError where
    pretty (CannotListContent (FS.LsFailed f _)) =
        P.t $ "Cannot populate binary starter directory: " <>
                "Cannot list directory content: " <> pathToText f
    pretty (CannotRemoveBin (FS.RemoveFailed f _)) =
        P.t $ "Cannot populate binary starter directory: " <>
                "Cannot remove old binary: " <> pathToText f
    pretty (CannotReadMeta readMetaErr) =
        P.label ("Cannot populate binary starter directory: " <>
                "Cannot read SDK package metadata: ") $ P.pretty readMetaErr
    pretty (CannotCreateBinStarter (BinaryStarterCreationFailed f1 f2 _)) =
        P.t $ "Cannot populate binary starter directory: " <>
                "Cannot create binary starter for " <> pathToText f1 <> 
                " at " <> pathToText f2
    pretty (CannotCreateBinDir (FS.MkTreeFailed f _)) =
        P.t $ "Cannot populate binary starter directory: " <>
                "Cannot create binary directory: " <> pathToText f

populateBinWithPackagesBinaries
    :: (L.MonadLogger m, FS.MonadFS m)
    => Loc.FilePathOfPackagesDir
    -> Loc.FilePathOfDaBinaryDir
    -> Version.SemVersion
    -> m (Either BinDirPopulationError ())
populateBinWithPackagesBinaries packagesDirPath daBinDir sdkVersion = runExceptT $ do
    exc CannotCreateBinDir $ FS.mktree daBinDir
    oldContent <- exc CannotListContent $ lsNames daBinDir
    forM_ oldContent $ \fname -> do
        unless (fname == daRelativeBinName) $ do
            let binFileToRemove = Loc.addPathSuffix fname daBinDir :: Loc.FilePathOfDaBinary
            exc CannotRemoveBin $ FS.rm binFileToRemove
    let sdkPackageVersionDirPath = Loc.addPathSuffix (vsnToPath sdkVersion) $
                                                     Loc.sdkPackagePath packagesDirPath
    metadata <- exc CannotReadMeta $ readSdkMetadata sdkPackageVersionDirPath
    pkgsL <-
        forM (MS.toList $ _mGroups metadata) $ \(_grpname, grp) -> do
            forM (MS.toList $ _gPackages grp) $ \(artifactId, _pkg) -> do
                return (artifactId, _gVersion grp)
    forM_ (sort $ concat pkgsL) $ \(artifactId, vsn) -> do
        let vsnDir = vsnToPath vsn
            artifactDir = textToPath artifactId
            pkgDir = Loc.addPathSuffix artifactDir packagesDirPath
                            :: Loc.FilePathOfSomePackage
            pkgVsnDir = Loc.addPathSuffix vsnDir pkgDir
                            :: Loc.FilePathOfSomePackageVersion
            pkgVsnBinDir = Loc.addPathSuffix (textToPath "bin") pkgVsnDir
                            :: Loc.FilePathOfSomePackageVersionBinDir
            vsnTxt = Version.showSemVersion vsn
        whenM (FS.testdir pkgVsnBinDir) $ do
            binDirContent <- exc CannotListContent $ lsNames pkgVsnBinDir
            let (notBins, bins) = partitionEithers $ map eitherNotBinaryOrBinary binDirContent
                binsWithExtlist = MM.assocs $ MM.fromList $ map splitExtension bins
            forM_ notBins $ \notBinFile ->
                L.logWarnN $ pathToText notBinFile
                          <> " (" <> artifactId <> " - " <> vsnTxt <> ") "
                          <> "was ignored because it has an unrecognised extension."
            forM_ binsWithExtlist $ \(fileName, extlist) -> do
            -- ^ every file has at least 1 extension (incl. Nothing), exts /= []
            -- Select the "most valuable" extension:
                case headMay $ filter (`elem` extlist) osSpecificBinaryExtensions of
                  Just winnerExt -> do
                    let fileNameWithExt = maybe fileName (fileName <.>) winnerExt
                        fileToAddStarterFor = Loc.addPathSuffix fileNameWithExt pkgVsnBinDir
                        binTargetInDaBinDir = Loc.addPathSuffix fileName daBinDir
                    binStarterAlreadyExists <- FS.testfile binTargetInDaBinDir
                    if binStarterAlreadyExists then
                        L.logWarnN $ "The executable " <> Loc.pathOfToText fileToAddStarterFor
                                <> " was not added to " <> Loc.pathOfToText daBinDir
                                <> " because it already exists." -- TODO (GH) add 'and it refers to XYZ'
                    else
                        exc CannotCreateBinStarter $
                            FS.createBinaryStarter fileToAddStarterFor binTargetInDaBinDir
                  Nothing ->
                    L.logWarnN $ "Component " <> artifactId <> " (" <> vsnTxt <> ") "
                              <> "has no suitable executable for this OS."
  where
    vsnToPath = textToPath . Version.showSemVersion
    eitherNotBinaryOrBinary :: FilePath -> Either FilePath FilePath
    eitherNotBinaryOrBinary f =
        if extension f `elem` allBinaryExtensions then Right f else Left f
    exc e a = withExceptT e $ ExceptT a
    lsNames path = do
        listOrErr <- FS.ls path
        return $ map Turtle.filename <$> listOrErr
