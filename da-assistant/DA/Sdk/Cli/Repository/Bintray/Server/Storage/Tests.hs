-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Repository.Bintray.Server.Storage.Tests
    ( bintrayMockTests
    ) where

import           Control.Arrow                                ((&&&))
import qualified Control.Foldl                                as Fold
import qualified DA.Sdk.Cli.Repository.Bintray.Server.Storage as Storage
import qualified DA.Sdk.Cli.Repository.Bintray.Server.Types   as STy
import qualified DA.Sdk.Cli.Repository.Bintray.Server         as Srv
import qualified DA.Sdk.Cli.Repository.Types                  as Ty
import           DA.Sdk.Prelude
import qualified Data.Map                                     as Map
import qualified Data.Set                                     as Set
import qualified Data.Text                                    as Text
import           Database.SQLite.Simple
import qualified System.IO.Temp                               as Tmp
import           Test.Tasty
import           Test.Tasty.HUnit
import qualified Turtle

withTmpDir :: (STy.ServerConf -> Assertion) -> Assertion
withTmpDir f =
    Tmp.withSystemTempDirectory "bintray-mock-test" $ f . STy.confFromBaseDir . stringToPath

withConnection' :: STy.ServerConf -> (Connection -> IO a) -> IO a
withConnection' c q = withConnection (Text.unpack $ STy.serverSQLiteFile c) q

bintrayMockTests :: TestTree
bintrayMockTests = testGroup "DA.Sdk.Cli.Repository.Bintray.Server.Storage"
    [ createTablesTest
    , persistConfigTest
    , createPackageTest
    , getPackageTest
    , createGetVersionTest
    , attributesTest
    , attributeSearchTest
    , getRepositoriesTest
    , uploadDownloadContentTest
    , getVersionFilesTest
    ]

createTablesTest :: TestTree
createTablesTest = testCase "createTables should create the tables" $ withTmpDir $ \c -> do
    tablesFound <- queryExistingTables c
    tablesFound @?= Set.empty
    Storage.createTables c
    tablesFound' <- queryExistingTables c
    tablesFound' @?= Set.fromList tables
  where
    stm conn t = fmap fromOnly <$> queryNamed conn "SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name"
                   [ ":table_name" := t ] :: IO [Text]
    queryExistingTables c = do
        tablesFound <- concat <$> withConnection' c (\conn -> for tables (stm conn)) :: IO [Text]
        pure $ Set.fromList tablesFound
    tables = ["package", "attribute", "attribute_value", "pkg_to_path", "server_config"]

persistConfigTest :: TestTree
persistConfigTest = testCase "persistConfig should store the given config in it's table" $ withTmpDir $ \c -> do
    Storage.createTables c
    Storage.persistConfig c [Srv.mkDAExternalFilePath (Ty.PackageName "pn") (Ty.GenericVersion "7.7.7")
                                "com/mystuff/pn/7.7.7/sdk-0.2.10.tar.gz" (stringToPath "blabla/dir/file.tar.gz")]
    packageDirs <- withConnection' c queryPackageDir
    let expectedPackageDirs = [STy.serverPkgsDir c]
    packageDirs @?= expectedPackageDirs

    found <- STy.runServerM' c Storage.getPackageDir
    found @?= Right (STy.serverPkgsDir c)
  where
    queryPackageDir conn = (fmap fromOnly <$> queryNamed conn "SELECT package_directory FROM server_config" []) :: IO [Text]

createPackageTest :: TestTree
createPackageTest = testCase "createPackage should create a package" $ withTmpDir $ \c -> do
    let subject      = Ty.Subject "subject"
    let repo         = Ty.Repository "repository"
    let packageDescr = Ty.PackageDescriptor "foo/bar/foo/packageName" "packageDescr"
    let packageName  = Ty.PackageName $ Ty.descPkgName packageDescr
    Storage.createTables c

    res <- withConnection' c $ \conn -> fmap fromOnly <$> queryNamed conn "SELECT package_id FROM package" [] :: IO [Text]
    res @?= []

    res' <- STy.runServerM' c (Storage.createPackage subject repo packageDescr)
    res' @?= Right ()

    res'' <- withConnection' c $ \conn ->
        queryNamed conn "SELECT subject, repo, name, description, version FROM package" [] :: IO [(Ty.Subject, Ty.Repository, Ty.PackageName, Text, Maybe Ty.GenericVersion)]
    res'' @?= [(subject, repo, packageName, Ty.descDescription packageDescr, Nothing)]

getPackageTest :: TestTree
getPackageTest = testCase "getPackage should return a stored package" $ withTmpDir $ \c -> do
    let subject      = Ty.Subject "subject"
    let repo         = Ty.Repository "repository"
    let packageDescr = Ty.PackageDescriptor "com/digitalasset/packageName" "packageDescr"
    let packageName  = Ty.PackageName $ Ty.descPkgName packageDescr
    let description  = Ty.descDescription packageDescr
    Storage.createTables c

    res <- STy.runServerM' c (Storage.createPackage subject repo packageDescr)
    res @?= Right ()

    res' <- STy.runServerM' c (Storage.getPackage subject repo packageName)
    res' @?= Right (Just Ty.BintrayPackage {
          Ty._bpPackageName  = packageName
        , Ty._bpRepository  = repo
        , Ty._bpOwner       = subject
        , Ty._bpVersions    = []
        , Ty._bpDescription = description
        , Ty._bpAttributes  = Map.empty
    })

createGetVersionTest :: TestTree
createGetVersionTest = testCase "createVersion should create a version of an existing package" $ withTmpDir $ \c -> do
    let subject      = Ty.Subject "subject"
    let repo         = Ty.Repository "repository"
    let packageDescr = Ty.PackageDescriptor "packageName" "packageDescr"
    let packageName  = Ty.PackageName $ Ty.descPkgName packageDescr
    let description  = Ty.descDescription packageDescr
    let version      = Ty.GenericVersion "1.0.0"
    let filePath     = ["empty.txt"]
    let content      = ""
    Storage.createTables c

    res <- STy.runServerM' c (Storage.createPackage subject repo packageDescr)
    res @?= Right ()

    res' <- STy.runServerM' c (Storage.uploadContent subject repo packageName version filePath content)
    res' @?= Right ()

    res'' <- STy.runServerM' c (Storage.getPackage subject repo packageName)
    res'' @?= Right (Just Ty.BintrayPackage {
          Ty._bpPackageName  = packageName
        , Ty._bpRepository  = repo
        , Ty._bpOwner       = subject
        , Ty._bpVersions    = [ version ]
        , Ty._bpDescription = description
        , Ty._bpAttributes  = Map.empty
    })


attributesTest :: TestTree
attributesTest = testCase "getAttributes should return the stored attributes" $ withTmpDir $ \c -> do
    let subject      = Ty.Subject "subject"
    let repo         = Ty.Repository "repository"
    let packageDescr = Ty.PackageDescriptor "packageName" "packageDescr"
    let packageName  = Ty.PackageName $ Ty.descPkgName packageDescr

    Storage.createTables c
    attrs <- STy.runServerM' c (Storage.getAttributes subject repo packageName Storage.noVersion)
    attrs @?= Right []

    res <- STy.runServerM' c (Storage.createPackage subject repo packageDescr)
    res @?= Right ()

    let expectedAttrs = [Ty.Attribute "a1" (Ty.AVText ["b1", "c1"]), Ty.Attribute "a2" (Ty.AVText ["b2", "c2"])]
    res' <- STy.runServerM' c (Storage.updateAttributes subject repo packageName Storage.noVersion expectedAttrs)
    res' @?= Right ()

    foundAttrs <- STy.runServerM' c (Storage.getAttributes subject repo packageName Storage.noVersion)
    foundAttrs @?= Right expectedAttrs

attributeSearchTest :: TestTree
attributeSearchTest = testCase "attributeSearch should return the package associated with the stored attributes" $ withTmpDir $ \c -> do
    let subject       = Ty.Subject "subject"
    let repo          = Ty.Repository "repository"
    let packageDescr1 = Ty.PackageDescriptor "packageName1" "packageDescr"
    let packageDescr2 = Ty.PackageDescriptor "packageName2" "packageDescr"
    let attrs1        = [Ty.Attribute "a1" (Ty.AVText ["b1", "c1"]), Ty.Attribute "a2" (Ty.AVText ["b2", "c2"]), Ty.Attribute "a3" (Ty.AVText ["b3"])]
    let attrs2        = [Ty.Attribute "a1" (Ty.AVText ["b1", "c2"]), Ty.Attribute "a4" (Ty.AVText ["b4", "c4"])]
    let pkgsWithAttrs = [(packageDescr1, attrs1), (packageDescr2, attrs2)]
    let searchAttrs1  = fmap Ty.SearchAttribute [Ty.Attribute "a1" (Ty.AVText ["c1"])]
    let searchAttrs2  = fmap Ty.SearchAttribute [Ty.Attribute "a1" (Ty.AVText ["b1"]), Ty.Attribute "a3" (Ty.AVText ["b3"])]
    let searchAttrs3  = []
    let searchAttrs4  = fmap Ty.SearchAttribute [Ty.Attribute "a1" (Ty.AVText ["z"])]
    let packageInfo1  = Ty.PackageInfo {
                          Ty.infoPkgName     = Ty.PackageName $ Ty.descPkgName packageDescr1
                        , Ty.infoVersions    = []
                        , Ty.infoDescription = Ty.descDescription packageDescr1
                        , Ty.infoAttributes  = Ty.attributesToMap attrs1
                        }
    let packageInfo2  = Ty.PackageInfo {
                          Ty.infoPkgName     = Ty.PackageName $ Ty.descPkgName packageDescr2
                        , Ty.infoVersions    = []
                        , Ty.infoDescription = Ty.descDescription packageDescr2
                        , Ty.infoAttributes  = Ty.attributesToMap attrs2
                        }

    Storage.createTables c

    res <- STy.runServerM' c $ for (fmap fst pkgsWithAttrs) $ Storage.createPackage subject repo
    res @?= Right [(), ()]

    res0 <- STy.runServerM' c $
                for (fmap (Ty.PackageName . Ty.descPkgName . fst &&& snd) pkgsWithAttrs) $ \(packageName, attrs) ->
                    Storage.updateAttributes subject repo packageName Storage.noVersion attrs
    res0 @?= Right [(), ()]

    res1 <- STy.runServerM' c $ Storage.attributeSearch subject repo searchAttrs1
    res1 @?= Right [packageInfo1]

    res2 <- STy.runServerM' c $ Storage.attributeSearch subject repo searchAttrs2
    res2 @?= Right [packageInfo1]

    res3 <- STy.runServerM' c $ Set.fromList <$> Storage.attributeSearch subject repo searchAttrs3
    res3 @?= Right (Set.fromList [packageInfo1, packageInfo2])

    res4 <- STy.runServerM' c $ Storage.attributeSearch subject repo searchAttrs4
    res4 @?= Right []

getRepositoriesTest :: TestTree
getRepositoriesTest = testCase "getRepositories should return the repositories in the system" $ withTmpDir $ \c -> do
    let subject       = Ty.Subject "subject"
    let repo1         = Ty.Repository "rep1"
    let repo2         = Ty.Repository "rep2"
    let packageDescr1 = Ty.PackageDescriptor "packageName1" "packageDescr"
    let packageDescr2 = Ty.PackageDescriptor "packageName2" "packageDescr"
    let packageDescr3 = Ty.PackageDescriptor "packageName3" "packageDescr"

    Storage.createTables c

    res <- STy.runServerM' c $
            for [(repo1, packageDescr1), (repo2, packageDescr2), (repo1, packageDescr3)] $ \(repo, packageDescr) ->
                Storage.createPackage subject repo packageDescr
    res @?= Right [(), (), ()]

    res1 <- STy.runServerM' c $ Storage.getRepositories subject
    res1 @?= Right [repo1, repo2]

uploadDownloadContentTest :: TestTree
uploadDownloadContentTest = testCase "uploadContent should upload a new version of a package" $ withTmpDir $ \c -> do
    let subject      = Ty.Subject "subject"
    let repo         = Ty.Repository "repository"
    let packageName  = Ty.PackageName "packageName"
    let version      = Ty.GenericVersion "0.0.0"
    let relPath      = ["path", "to", "fileUploaded.txt"]
    let content      = "This is an autogenerated file to test uploadContent (see uploadContentTest)"

    Storage.createTables c

    res <- STy.runServerM' c $
        Storage.uploadContent subject repo packageName version relPath content
    res @?= Right ()

    files <- Turtle.fold (Turtle.lstree $ textToPath $ STy.serverPkgsDir c) Fold.list >>= filterM (fmap Turtle.isRegularFile . Turtle.stat)

    -- testes the internal of the Storage, see the downloadContent call after this line for a test of the API
    files @?= [textToPath $ STy.serverPkgsDir c <> "/subject/repository/path/to/fileUploaded.txt"]

    res' <- STy.runServerM' c $ Storage.downloadContent subject repo relPath
    res' @?= Right content

getVersionFilesTest :: TestTree
getVersionFilesTest = testCase "getVersionFiles should list all the filepath of a release" $ withTmpDir $ \c -> do
    let subject      = Ty.Subject "subject"
    let repo         = Ty.Repository "repository"
    let packageName  = Ty.PackageName "packageName"
    let version      = Ty.GenericVersion "0.0.0"
    let relPath1     = ["path", "to", "fileUploaded.txt"]
    let relPath2     = ["path", "foobar"]
    let relPath3     = ["to", "fileUploaded.txt"]
    let expected     = fmap (Ty.PkgFileInfo . Text.intercalate "/") [relPath1, relPath2, relPath3]
    let content      = "This is an autogenerated file to test uploadContent (see uploadContentTest)"

    Storage.createTables c

    res <- STy.runServerM' c $
            for [relPath1, relPath2, relPath3] $ \relPath ->
                Storage.uploadContent subject repo packageName version relPath content
    res @?= Right [(), (), ()]

    res' <- STy.runServerM' c $ Storage.getVersionFiles subject repo packageName version
    fmap sort res' @?= Right (sort expected) -- we enforce the order to allow list comparison to work
