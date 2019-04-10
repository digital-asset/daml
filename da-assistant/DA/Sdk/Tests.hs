-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Tests
    ( runTests
    ) where

import qualified Data.Yaml                     as Yaml
import DA.Sdk.Prelude
import qualified DA.Sdk.Cli.Env as Env
import qualified DA.Sdk.Cli.Repository as Repo
import qualified DA.Sdk.Cli.Command as Command
import qualified Data.Set as Set
import qualified Data.Attoparsec.Text as A
import Test.Tasty
import Test.Tasty.HUnit
import qualified Data.Text.Extended as T
import DA.Sdk.Cli.Conf ( bintrayAPIURLParser
                       , bintrayDownloadURLParser
                       , defaultBintrayAPIURL
                       , defaultBintrayDownloadURL
                       , parsePositive
                       , Prop(..)
                       )
import qualified DA.Sdk.Cli.Repository.Bintray as BT
import DA.Sdk.Cli.Repository.Bintray.Server.Storage.Tests (bintrayMockTests)
import qualified DA.Sdk.Cli.Metadata           as Metadata
import qualified DA.Sdk.Cli.Template as Tp
import qualified DA.Sdk.Cli.Project as Pr
import qualified DA.Sdk.Cli.Conf as Conf
import Control.Monad.Trans.Except (throwE)
import Data.IORef
import Servant.Client.Core.Internal.Request as S
import Network.HTTP.Types.Status as N
import Network.HTTP.Types.Version as NV
import Data.Sequence.Internal as SeqI
import Data.Aeson.Extra.Merge.Custom.Test (customMergeTests)
import DA.Sdk.Version as Version hiding (parseVersion)
import Servant.Client (BaseUrl(..), Scheme(Https))
import Data.Map as MP

runTests :: IO ()
runTests = defaultMain $ testGroup "SDK Assistant"
    [ customMergeTests
    , envTests
    , tags
    , genericTests
    , bintrayMockTests
    ]


--------------------------------------------------------------------------------
-- Env
--------------------------------------------------------------------------------

envTests :: TestTree
envTests = testGroup "DA.Sdk.Cli.Env"
    [ javacVersion
    ]
  where
    javacVersion = testGroup "Javac Version"
        [ javaVersionTest "Test 1" (Env.JdkVersion (8, 0, 0)) "javac 1.8.0_144"
        , javaVersionTest
            "Test 2"
            (Env.JdkVersion (7, 0, 0))
            "[Warn] javac 1.7.0-u60-unofficial"
        , javaVersionTest "Test 3" (Env.JdkVersion (9, 0, 0)) "javac 9"
        , javaVersionTest "Test 4" (Env.JdkVersion (8, 0, 0)) $ T.unlines
            [ "[dev-env] Building tools.jdk..."
            , "javac 1.8.0_144"
            ]
        , javaVersionTest "Test 5" (Env.JdkVersion (8, 0, 0)) $ T.unlines
            [ "javac 1.8.0_144"
            , "[dev-env] Building tools.jdk..."
            ]
        ]
      where
        javaVersionTest = versionTest Env.parseJavacVersion


    versionTest ::
           (Show version, Eq version)
        => (Text -> Either Env.Error version)
        -> String
        -> version
        -> Text
        -> TestTree
    versionTest parseVersion testName expected input =
        testCase testName $
            case parseVersion input of
                Left err -> assertFailure $ show err
                Right vers ->
                    assertEqual "Versions don't match" expected vers

--------------------------------------------------------------------------------
-- Tags
--------------------------------------------------------------------------------

-- versionMatchesTags :: Ty.TagsFilter -> Set.Set Text -> Bool

tags :: TestTree
tags = testGroup "DA.Sdk.Cli.Bintray"
    [ wildcards
    , includes
    , excludes
    ]
  where
    s = Set.fromList

    wildcards = testGroup "Wildcards"
        [ versionShouldMatch
            "Included contains just a wildcard and should match any version"
            (Repo.TagsFilter (s ["*"]) (s ["excluded-tag"]))
            (s ["a-tag", "another-one"])
        , versionShouldMatch
            "Included contains a wildcard and should match any version"
            (Repo.TagsFilter (s ["included-tag", "*"]) (s ["excluded-tag"]))
            (s ["a-tag", "another-one"])
        , versionShouldntMatch
            "Excluded contains a wildcard and should match no version"
            (Repo.TagsFilter (s ["included-tag"]) (s ["excluded-tag", "*"]))
            (s ["included-tag"])
        ]

    includes = testGroup "Includes"
        [ versionShouldMatch
            "An included Tag is present on the server"
            Repo.defaultTagsFilter
            (s ["visible-external", "visible-internal"])
        , versionShouldntMatch
            "No included Tag is present on the server"
            Repo.defaultTagsFilter
            (s ["a-random-tag"])
        ]

    excludes = testGroup "Excludes"
        [ versionShouldntMatch
            "An excluded Tag is present on the server"
            Repo.defaultTagsFilter
            (s ["visible-external", "deprecated"])
        ]


    versionShouldMatch :: TestName -> Repo.TagsFilter -> Set.Set Text -> TestTree
    versionShouldMatch descr tagsFilter versionTags = testCase descr $
        if BT.versionMatchesTags tagsFilter versionTags then
            pure ()
        else
            assertFailure
          $ "Version doesn't match but should: \n" ++ show tagsFilter ++ "\n" ++ show versionTags

    versionShouldntMatch :: TestName -> Repo.TagsFilter -> Set.Set Text -> TestTree
    versionShouldntMatch descr tagsFilter versionTags = testCase descr $
        if BT.versionMatchesTags tagsFilter versionTags then
            assertFailure
          $ "Version matches, but should not: \n" ++ show tagsFilter ++ "\n" ++ show versionTags
        else
            pure ()

--------------------------------------------------------------------------------
-- Parsers
--------------------------------------------------------------------------------

genericTests :: TestTree
genericTests = testGroup "Generic functionality"
    [ bintrayAPIURLParserTest
    , bintrayDownloadURLParserTest
    , attributePairParserTest
    , attributePairParserFailTest
    , destinationParserTest
    , destinationParserTooManyPkgTest
    , destinationParserWrongVersionFailTest
    , groupByAttrNameTest
    , alphaNumWithDashesParserTest
    , alphaNumWithDashesParserFailTest
    , latestToVersionedTest
    , latestToVersioned2Test
    , latestToVersionedFailureTest
    , publishNewTemplateTest
    , publishTemplateForExistingPackageTest
    , publishTemplateFailureTest
    , downloadExactProjectTemplateTest
    , downloadReleaseLineProjectTemplateTest
    , downloadTemplateNotAvailableTest
    , listPublishedTemplatesTest
    , listReposTest
    , parseWholeSemVersionTest
    , parseWholeSemVersionWithPrereleaseTest
    , parseMajorMinorSemVersionTest
    , parseMajorSemVersionTest
    , parsePositiveTest
    , qualifiedTemplateParserReleaseLineTest
    , qualifiedTemplateParserExactReleaseTest
    , qualifiedTemplateParserFail1Test
    , qualifiedTemplateParserFail2Test
    , qualifiedTemplateParserFail3Test
    , fetchArgParserTest1
    , fetchArgParserTest2
    , fetchArgParserTest3
    , fetchArgParserFailTest1
    , fetchArgParserFailTest2
    , fetchArgParserFailTest3
    , fetchArgParserFailTest4
    , listUsableAddonTemplatesTest
    , listUsableProjectTemplatesTest
    , createDownloadUrlTest
    , downloadPackagePathTest1
    , downloadPackagePathTest2
    , downloadPackagePathTest3
    , downloadPackagePathTest4
    , semVersionComparisonEq
    , semVersionComparisonGt1
    , semVersionComparisonGt2
    , semVersionComparisonGt3
    , semVersionComparisonGtOrEq
    , semVersionComparisonGtFalse1
    , semVersionComparisonGtFalse2
    ]

-- Helper to check decode.encode === identity === encode.decode
checkIdentity :: String -> String -> Prop -> (Yaml.Value -> Yaml.Parser Prop) -> TestTree
checkIdentity name desc expected valueToParser =
    testCase desc $ case Yaml.parseEither valueToParser (Yaml.toJSON expected) of
        Right found -> assertEqual errorMsg expected found
        Left err -> assertFailure err
    where
        errorMsg = "Decoded " <> name <> " doesn't match the original"

bintrayAPIURLParserTest :: TestTree
bintrayAPIURLParserTest = checkIdentity name desc expected valueToParser
    where
        name          = "BintrayAPIURL"
        desc          = "bintrayAPIURLParser should parse BintrayAPIURL"
        expected      = PropBintrayAPIURL defaultBintrayAPIURL
        valueToParser = bintrayAPIURLParser

bintrayDownloadURLParserTest :: TestTree
bintrayDownloadURLParserTest = checkIdentity name desc expected valueToParser
    where
        name          = "BintrayDownloadURL"
        desc          = "bintrayDownloadURLParser should parse BintrayDownloadURL"
        expected      = PropBintrayDownloadURL defaultBintrayDownloadURL
        valueToParser = bintrayDownloadURLParser

checkParser :: (Eq a, Show a) => String -> String -> Text -> Maybe a -> A.Parser a -> TestTree
checkParser name desc text mbExpected parser =
    testCase desc $ case (A.parseOnly (parser <* A.endOfInput) text, mbExpected) of
        (Right found, Just ex)  -> assertEqual errMsg2 ex found
        (Left _err,   Nothing)  -> return ()
        (Right found, Nothing)  -> assertFailure (errMsg1 found)
        (Left err,    _)        -> assertFailure err
    where
        errMsg1 e = "The parser should have failed. (" <> show e <> ")"
        errMsg2 = "Decoded " <> name <> " doesn't match the original"

semVersionComparisonEq :: TestTree
semVersionComparisonEq =
    testCase "SemVersion equality works." $
    assertEqual "SemVersion equality does not work."
        True (Version.SemVersion 1 2 3 Nothing == Version.SemVersion 1 2 3 Nothing)

semVersionComparisonGt1 :: TestTree
semVersionComparisonGt1 =
    testCase "SemVersion < works." $
    assertEqual "SemVersion < does not work."
        True (Version.SemVersion 1 2 3 Nothing < Version.SemVersion 1 2 4 Nothing)

semVersionComparisonGt2 :: TestTree
semVersionComparisonGt2 =
    testCase "SemVersion < works." $
    assertEqual "SemVersion < does not work."
        True (Version.SemVersion 1 2 3 Nothing < Version.SemVersion 1 3 3 Nothing)

semVersionComparisonGt3 :: TestTree
semVersionComparisonGt3 =
    testCase "SemVersion < works." $
    assertEqual "SemVersion < does not work."
        True (Version.SemVersion 1 2 3 Nothing < Version.SemVersion 2 2 3 Nothing)

semVersionComparisonGtOrEq :: TestTree
semVersionComparisonGtOrEq =
    testCase "SemVersion <= works." $
    assertEqual "SemVersion <= does not work."
        True (Version.SemVersion 1 2 3 Nothing <= Version.SemVersion 1 2 3 Nothing)

semVersionComparisonGtFalse1 :: TestTree
semVersionComparisonGtFalse1 =
    testCase "SemVersion < works." $
    assertEqual "SemVersion < does not work."
        False (Version.SemVersion 1 2 3 Nothing < Version.SemVersion 1 2 3 Nothing)

semVersionComparisonGtFalse2 :: TestTree
semVersionComparisonGtFalse2 =
    testCase "SemVersion < works." $
    assertEqual "SemVersion < does not work."
        False (Version.SemVersion 1 2 3 Nothing < Version.SemVersion 0 2 3 Nothing)

parseWholeSemVersionTest :: TestTree
parseWholeSemVersionTest = checkParser name desc valueToParse expected Version.semVersionParser
    where
        name          = "wholeSemanticVersion"
        desc          = "parseSemVersion should parse well-formed semantic versions"
        expected      = Just $ Version.SemVersion 1 2 3 Nothing
        valueToParse  = "1.2.3"

parseWholeSemVersionWithPrereleaseTest :: TestTree
parseWholeSemVersionWithPrereleaseTest = checkParser name desc valueToParse expected Version.semVersionParser
    where
        name          = "wholeSemanticVersionWithPrelease"
        desc          = "parseSemVersion should parse well-formed semantic versions with pre-release"
        expected      = Just $ Version.SemVersion 1 2 3 (Just "abc")
        valueToParse  = "1.2.3-abc"

parseMajorMinorSemVersionTest :: TestTree
parseMajorMinorSemVersionTest = checkParser name desc valueToParse expected Version.semVersionParser
    where
        name          = "semanticVersionWithMajorAndMinor"
        desc          = "parseSemVersion should parse semantic versions with major and minor only"
        expected      = Just $ Version.SemVersion 1 2 0 Nothing
        valueToParse  = "1.2"

parseMajorSemVersionTest :: TestTree
parseMajorSemVersionTest = checkParser name desc valueToParse expected Version.semVersionParser
    where
        name          = "semanticVersionWithMajorOnly"
        desc          = "parseSemVersion should parse semantic versions with major only"
        expected      = Just $ Version.SemVersion 1 0 0 Nothing
        valueToParse  = "1"

attributePairParserTest :: TestTree
attributePairParserTest = checkParser name desc valueToParse expected Command.attributePairParser
    where
        name          = "attributePair"
        desc          = "attributePairParser should parse an attribute pair"
        expected      = Just ("a", "b3ofk")
        valueToParse  = "a=b3ofk"

attributePairParserFailTest :: TestTree
attributePairParserFailTest = checkParser name desc valueToParse expected Command.attributePairParser
    where
        name          = "attributePair"
        desc          = "attributePairParser should not parse wrong pairs"
        expected      = Nothing
        valueToParse  = "alflflf"

destinationParserTest :: TestTree
destinationParserTest = checkParser name desc valueToParse expected Command.destinationParser
    where
        name          = "destination"
        desc          = "destinationParser should parse a destination"
        expected      = Just (Repo.PackageName "appletree", Repo.GenericVersion "1.2.1")
        valueToParse  = "appletree/1.2.1"

fetchArgParserTest1 :: TestTree
fetchArgParserTest1 = checkParser name desc valueToParse expected Command.fetchArgP
    where
        name          = "fetch argument"
        desc          = "fetchArgP should parse a fetch argument (1)"
        expected      = Just $ Repo.FetchArg (Just $ Repo.FetchPrefix "prefix") $ Repo.FetchName "name"
        valueToParse  = "prefix/name"

fetchArgParserTest2 :: TestTree
fetchArgParserTest2 = checkParser name desc valueToParse expected Command.fetchArgP
    where
        name          = "fetch argument"
        desc          = "fetchArgP should parse a fetch argument (2)"
        expected      = Just $ Repo.FetchArg Nothing $ Repo.FetchName "name"
        valueToParse  = "name"

fetchArgParserTest3 :: TestTree
fetchArgParserTest3 = checkParser name desc valueToParse expected Command.fetchArgP
    where
        name          = "fetch argument"
        desc          = "fetchArgP should parse a fetch argument (3)"
        expected      = Just $ Repo.FetchArg (Just $ Repo.FetchPrefix "some.prefix") $ Repo.FetchName "name"
        valueToParse  = "some.prefix/name"

fetchArgParserFailTest1 :: TestTree
fetchArgParserFailTest1 = checkParser name desc valueToParse expected Command.fetchArgP
    where
        name          = "fetch argument"
        desc          = "fetchArgP should not parse malformed input (1)"
        expected      = Nothing
        valueToParse  = "some.prefix/"

fetchArgParserFailTest2 :: TestTree
fetchArgParserFailTest2 = checkParser name desc valueToParse expected Command.fetchArgP
    where
        name          = "fetch argument"
        desc          = "fetchArgP should not parse malformed input (2)"
        expected      = Nothing
        valueToParse  = "/name"

fetchArgParserFailTest3 :: TestTree
fetchArgParserFailTest3 = checkParser name desc valueToParse expected Command.fetchArgP
    where
        name          = "fetch argument"
        desc          = "fetchArgP should not parse malformed input (3)"
        expected      = Nothing
        valueToParse  = "a/b/c"

fetchArgParserFailTest4 :: TestTree
fetchArgParserFailTest4 = checkParser name desc valueToParse expected Command.fetchArgP
    where
        name          = "fetch argument"
        desc          = "fetchArgP should not parse malformed input (4)"
        expected      = Nothing
        valueToParse  = "f@nclub/c"

destinationParserTooManyPkgTest :: TestTree
destinationParserTooManyPkgTest = checkParser name desc valueToParse expected Command.destinationParser
    where
        name          = "destination"
        desc          = "destinationParser should not parse malformed package names"
        expected      = Nothing
        valueToParse  = "a/b/c/d"

destinationParserWrongVersionFailTest :: TestTree
destinationParserWrongVersionFailTest = checkParser name desc valueToParse expected Command.destinationParser
    where
        name          = "destination"
        desc          = "destinationParser should not parse malformed versions"
        expected      = Nothing
        valueToParse  = "a/9."

alphaNumWithDashesParserTest :: TestTree
alphaNumWithDashesParserTest = checkParser name desc valueToParse expected Command.alphaNumWithDashesParser
    where
        name          = "alphaNumWithDashes"
        desc          = "alphaNumWithDashesParser should parse a well-formed string"
        expected      = Just "a-1-bc-2d"
        valueToParse  = "a-1-bc-2d"

alphaNumWithDashesParserFailTest :: TestTree
alphaNumWithDashesParserFailTest = checkParser name desc valueToParse expected Command.alphaNumWithDashesParser
    where
        name          = "alphaNumWithDashes"
        desc          = "alphaNumWithDashesParser should not parse a malformed string"
        expected      = Nothing
        valueToParse  = "-a-1-bc-2d"

qualifiedTemplateParserReleaseLineTest :: TestTree
qualifiedTemplateParserReleaseLineTest = checkParser name desc valueToParse expected Command.qualifiedTemplateParser
    where
        name          = "qualifiedTemplate"
        desc          = "qualifiedTemplateParser should parse well-formed qualified templates"
        expected      = Just (Repo.NameSpace "tempate-repo123", Repo.TemplateName "my-template001", Repo.ReleaseLine 1 2)
        valueToParse  = "tempate-repo123/my-template001/1.2.x"

qualifiedTemplateParserExactReleaseTest :: TestTree
qualifiedTemplateParserExactReleaseTest = checkParser name desc valueToParse expected Command.qualifiedTemplateParser
    where
        name          = "qualifiedTemplate"
        desc          = "qualifiedTemplateParser should parse well-formed qualified templates"
        expected      = Just (Repo.NameSpace "tempate-repo123", Repo.TemplateName "my-template001", Repo.ExactRelease 1 2 3)
        valueToParse  = "tempate-repo123/my-template001/1.2.3"

qualifiedTemplateParserFail1Test :: TestTree
qualifiedTemplateParserFail1Test = checkParser name desc valueToParse expected Command.qualifiedTemplateParser
    where
        name          = "qualifiedTemplate"
        desc          = "qualifiedTemplateParser should not parse malformed input"
        expected      = Nothing
        valueToParse  = "tempate-repo123/my-template001"

qualifiedTemplateParserFail2Test :: TestTree
qualifiedTemplateParserFail2Test = checkParser name desc valueToParse expected Command.qualifiedTemplateParser
    where
        name          = "qualifiedTemplate"
        desc          = "qualifiedTemplateParser should not parse malformed input"
        expected      = Nothing
        valueToParse  = "tempate-repo123/my-template001/"

qualifiedTemplateParserFail3Test :: TestTree
qualifiedTemplateParserFail3Test = checkParser name desc valueToParse expected Command.qualifiedTemplateParser
    where
        name          = "qualifiedTemplate"
        desc          = "qualifiedTemplateParser should not parse malformed input"
        expected      = Nothing
        valueToParse  = "tempate-repo123/my-template001/1.2"

latestToVersionedTest :: TestTree
latestToVersionedTest = testCase "Function `latestVersionToVersioned` should turn latest version into a Versioned type." $
        assertEqual err expected (BT.latestToVersioned valueToParse)
    where
        err           = "Function `latestVersionToVersioned` should parse well-formed input"
        expected      = Right $ Version.BuildVersion 123 "abc"
        valueToParse  = Repo.LatestVersion (Repo.GenericVersion "\"123-abc\"")

latestToVersioned2Test :: TestTree
latestToVersioned2Test = testCase "Function `latestVersionToVersioned` should turn latest version into a Versioned type." $
        assertEqual err expected (BT.latestToVersioned valueToParse)
    where
        err           = "Function `latestVersionToVersioned` should parse well-formed input"
        expected      = Right $ Version.BuildVersion 123 "abc"
        valueToParse  = Repo.LatestVersion (Repo.GenericVersion "123-abc")

latestToVersionedFailureTest :: TestTree
latestToVersionedFailureTest = testCase "Function `latestVersionToVersioned` should fail on improper input." $
        assertEqual err expected (BT.latestToVersioned valueToParse :: Either Repo.Error BuildVersion)
    where
        err           = "Function `latestVersionToVersioned` should not parse malformed input"
        expected      = Left $ Repo.ErrorJSONParse "Error in $: expected BuildVersion, encountered String"
        valueToParse  = Repo.LatestVersion (Repo.GenericVersion "something-weird")

groupByAttrNameTest :: TestTree
groupByAttrNameTest = testCase "Function `groupByAttrName` should group attributes by name." $
    assertEqual err expected (Command.groupByAttrName [("a", "1"), ("b", "2"), ("a", "3")])
    where
        err      = "Function `groupByAttrName` gave a wrong result."
        expected = [Repo.Attribute "a" (Repo.AVText ["1", "3"]), Repo.Attribute "b" (Repo.AVText ["2"])]

createDownloadUrlTest :: TestTree
createDownloadUrlTest = testCase "Function `createDownloadUrl` creates a valid download URL." $
    assertEqual err expected (Conf.createDownloadUrl Conf.defaultBintrayAPIURL)
    where
        err      = "Function `createDownloadUrl` gave a wrong result."
        expected = BaseUrl Https "dl.bintray.com" 443 ""

downloadPackagePathTest1 :: TestTree
downloadPackagePathTest1 = testCase "Function `downloadPackagePath` creates a valid download path." $
    assertEqual err expected (BT.downloadPackagePath dlPkg)
    where
        err      = "Function `downloadPackagePath` gave a wrong result."
        expected = ["my-custom-package", "1.2.3", "my-custom-package-1.2.3.tar.gz"]
        dlPkg    = Repo.DownloadablePackage Nothing (Metadata.PackagingTarGz Metadata.TarGz)
                    (Repo.Package "my-custom-package" []) (Repo.GenericVersion "1.2.3")

downloadPackagePathTest2 :: TestTree
downloadPackagePathTest2 = testCase "Function `downloadPackagePath` creates a valid download path." $
    assertEqual err expected (BT.downloadPackagePath dlPkg)
    where
        err      = "Function `downloadPackagePath` gave a wrong result."
        expected = ["com", "digitalasset", "my-custom-package", "1.2.3", "my-custom-package-1.2.3-class.tar.gz"]
        dlPkg    = Repo.DownloadablePackage (Just "class") (Metadata.PackagingTarGz Metadata.TarGz)
                    (Repo.Package "my-custom-package" ["com", "digitalasset"])
                            (Repo.GenericVersion "1.2.3")

downloadPackagePathTest3 :: TestTree
downloadPackagePathTest3 = testCase "Function `downloadPackagePath` creates a valid download path." $
    assertEqual err expected (BT.downloadPackagePath dlPkg)
    where
        err      = "Function `downloadPackagePath` gave a wrong result."
        expected = ["my-custom-package", "1.2.3", "my-custom-package-1.2.3.tgz"]
        dlPkg    = Repo.DownloadablePackage Nothing (Metadata.PackagingTarGz Metadata.Tgz)
                    (Repo.Package "my-custom-package" []) (Repo.GenericVersion "1.2.3")

downloadPackagePathTest4 :: TestTree
downloadPackagePathTest4 = testCase "Function `downloadPackagePath` creates a valid download path." $
    assertEqual err expected (BT.downloadPackagePath dlPkg)
    where
        err      = "Function `downloadPackagePath` gave a wrong result."
        expected = ["com", "digitalasset", "my-custom-package", "1.2.3", "my-custom-package-1.2.3-class.tgz"]
        dlPkg    = Repo.DownloadablePackage (Just "class") (Metadata.PackagingTarGz Metadata.Tgz)
                    (Repo.Package "my-custom-package" ["com", "digitalasset"])
                            (Repo.GenericVersion "1.2.3")
--------------------------------------------------------------------------------
-- Template management related requests
--------------------------------------------------------------------------------

-- The following symbols are used to mark for us in the tests which methods
-- were called with which arguments and in what order.
data TemplateReq = Create Repo.Subject Repo.Repository Repo.PackageDescriptor |
                   Upload Repo.Subject Repo.Repository Repo.PackageName Repo.GenericVersion [Text] |
                   AttribSet Repo.Subject Repo.Repository Repo.PackageName [Repo.Attribute] |
                   SearchPkgByAttrib Repo.Subject Repo.Repository [Repo.SearchAttribute] |
                   ListRepos Repo.Subject |
                   SearchPkgVersionFiles Repo.Subject Repo.Repository Repo.PackageName Repo.GenericVersion (Maybe Repo.BitFlag) |
                   Download Repo.Subject Repo.Repository Repo.PackageName Repo.GenericVersion |
                   GetInfo Repo.Subject Repo.Repository Repo.PackageName |
                   GetLatestVsn Repo.Subject Repo.Repository Repo.PackageName
    deriving (Eq, Show)

-- We use an IORef that has a list of TemplateReq symbols. IORef is like a mutable variable,
-- a list that is appended with the method that was called last time.
newMockHandle :: IORef [TemplateReq]
              -> (Maybe Repo.Error, Maybe Repo.Error, Maybe Repo.Error,
                  Either Repo.Error [Repo.PackageInfo],
                  Either Repo.Error [Repo.Repository],
                  Either Repo.Error [Repo.PkgFileInfo],
                  (Repo.PackageName -> Bool, Either Repo.Error Repo.BintrayPackage),
                  Either Repo.Error Repo.LatestVersion,
                  Maybe Repo.Error)
              -> Repo.RequestHandle
newMockHandle ops (r1, r2, r3, r4, r5, r6, (pred7, r7), r8, r9) = Repo.RequestHandle
    { Repo.hReqCreatePackage = \s r pd _auth ->
        addOp (++ [Create s r pd]) >> ret r1
    , Repo.hReqSetAttributes = \s r pn attrs _auth ->
        addOp (++ [AttribSet s r pn attrs]) >> ret r2
    , Repo.hReqUploadPackage = \s r pn gv f _cnt _publ _auth ->
        addOp (++ [Upload s r pn gv f]) >> ret r3
    , Repo.hReqSearchPkgsWithAtts = \s r attrs _auth ->
        addOp (++ [SearchPkgByAttrib s r attrs])
            >> either throwE return r4
    , Repo.hListRepos = \s _auth ->
        addOp (++ [ListRepos s]) >> either throwE return r5
    , Repo.hSearchPkgVersionFiles = \s r pn gv incl _auth ->
        addOp (++ [SearchPkgVersionFiles s r pn gv incl]) >> either throwE return r6
    , Repo.hGetPackageInfoReq = \s r pn _auth ->
        addOp (++ [GetInfo s r pn]) >>
            either throwE (\x -> if pred7 pn then return x else throwE notFoundError) r7
    , Repo.hGetLatestVersionReq = \s r pn _auth ->
        addOp (++ [GetLatestVsn s r pn]) >> either throwE return r8
    , Repo.hDownloadPackageReq = \s r pn gv f _auth ->
        addOp (++ [Download s r pn gv]) >> maybe (return f) throwE r9
    }
  where
    addOp = liftIO . modifyIORef ops
    ret   = maybe (return ()) throwE

alwaysOK :: Repo.PackageName -> Bool
alwaysOK _pn = True

notFoundError :: Repo.Error
notFoundError = Repo.ErrorRequest $ Repo.ServantError $ S.FailureResponse $
    S.Response (N.mkStatus 404 "Not found.") SeqI.empty NV.http11 ""

fakeSubject :: Repo.Subject
fakeSubject = Repo.Subject "s"

fakeRepo :: Repo.Repository
fakeRepo = Repo.Repository "r"

fakeCreds :: Repo.Credentials
fakeCreds = Repo.Credentials (Just (Repo.BintrayCredentials "a" "b"))

fakePkgNameTxt :: Text
fakePkgNameTxt = "p"

fakePkgName :: Repo.PackageName
fakePkgName = Repo.PackageName fakePkgNameTxt

fakeVersionTxt :: Text
fakeVersionTxt = "0.0"

fakeVersion :: Repo.GenericVersion
fakeVersion = Repo.GenericVersion fakeVersionTxt

fakeVersion2 :: Repo.GenericVersion
fakeVersion2 = Repo.GenericVersion "0.1"

pkgAttrs :: [Repo.Attribute]
pkgAttrs = [Repo.Attribute "template" (Repo.AVText ["true"])]

--
someBtPkg :: Repo.BintrayPackage
someBtPkg = Repo.BintrayPackage fakePkgName fakeRepo
              fakeSubject [] "dummy" MP.empty

fakeNameSpace :: Repo.NameSpace
fakeNameSpace = Repo.NameSpace "test-namespace"

fakeTemplateName :: Repo.TemplateName
fakeTemplateName = Repo.TemplateName "test-template"

fakeReleaseLine :: Repo.ReleaseLine
fakeReleaseLine = Repo.ReleaseLine 1 2

fakeRepo' :: Repo.Repository
fakeRepo' = Repo.Repository "test-namespace"

fakeTemplateNameTxt' :: Text
fakeTemplateNameTxt' = "test-template"

fakeReleaseLineTxt' :: Text
fakeReleaseLineTxt' = "1.2.x"

fakePkgNameTxt' :: Text
fakePkgNameTxt' =
    fakeTemplateNameTxt' <> "-" <> fakeReleaseLineTxt'

fakeDesc' :: Repo.PackageDescriptor
fakeDesc' = Repo.PackageDescriptor fakePkgNameTxt'
            ("Template package: " <> fakePkgNameTxt')

fakePkgName' :: Repo.PackageName
fakePkgName' = Repo.PackageName fakePkgNameTxt'

fakePkgNameExact' :: Repo.PackageName
fakePkgNameExact' = Repo.PackageName "test-template-1.2.3"

publishingFileName' :: Text -> Text
publishingFileName' v = fakePkgNameTxt' <> "-" <> v <> ".tar.gz"

pkgAttrs' :: Text -> Repo.TemplateType -> [Repo.Attribute]
pkgAttrs' rl tp =
    [Repo.Attribute "template" (Repo.AVText ["true"]),
     Repo.Attribute "release-line" (Repo.AVText [rl]),
     Repo.Attribute "type" (Repo.AVText [T.show tp])]

lv :: Text -> Repo.LatestVersion
lv = Repo.LatestVersion . Repo.GenericVersion
--

-- Publishing a new template.
publishNewTemplateTest :: TestTree
publishNewTemplateTest = testCase "A new template can be published." tc
  where
    -- note that we publish a brand new package, so no last version should exist
    ex = [GetLatestVsn fakeSubject fakeRepo' fakePkgName',
          Create fakeSubject fakeRepo' fakeDesc',
          Upload fakeSubject fakeRepo' fakePkgName' (Repo.GenericVersion "1")
                [fakePkgNameTxt', "1", publishingFileName' "1"],
          AttribSet fakeSubject fakeRepo' fakePkgName' (pkgAttrs' "1.2.x" Repo.Addon)]
    tc = do
        opsR <- newIORef ([] :: [TemplateReq])
        let mockReqH = newMockHandle opsR (Nothing, Nothing, Nothing, Right [],
                            Right [], Right [], (alwaysOK, Right someBtPkg), Left notFoundError, Nothing)
        repoH <- BT.makeBintrayTemplateRepoHandleWithRequestLayer fakeCreds fakeSubject mockReqH
        res <- Tp.templatePublishInternal repoH fakeNameSpace fakeTemplateName fakeReleaseLine "" Repo.Addon
        ops <- readIORef opsR
        assertEqual "Publishing package resulted in error." (Right ()) res
        assertEqual "Template publishing logic does not work as expected." ex ops

-- The error "Package already exists" should not stop template publishing.
publishTemplateForExistingPackageTest :: TestTree
publishTemplateForExistingPackageTest = testCase "A template can be published for an existing package." tc
  where
    alreadyExistsError = Repo.ErrorRequest $ Repo.ServantError $ S.FailureResponse $
        S.Response (N.mkStatus 409 "Already exists.") SeqI.empty NV.http11 ""
    ex = [GetLatestVsn fakeSubject fakeRepo' fakePkgName',
          Create fakeSubject fakeRepo' fakeDesc',
          Upload fakeSubject fakeRepo' fakePkgName' (Repo.GenericVersion "2")
              [fakePkgNameTxt', "2", publishingFileName' "2"],
          AttribSet fakeSubject fakeRepo' fakePkgName' (pkgAttrs' "1.2.x" Repo.Addon)]
    tc = do
        opsR <- newIORef ([] :: [TemplateReq])
        let mockReqH = newMockHandle opsR (Just alreadyExistsError, Nothing, Nothing,
                        Right [], Right [], Right [], (alwaysOK, Right someBtPkg), Right $ lv "1", Nothing)
        repoH <- BT.makeBintrayTemplateRepoHandleWithRequestLayer fakeCreds fakeSubject mockReqH
        res <- Tp.templatePublishInternal repoH fakeNameSpace fakeTemplateName fakeReleaseLine "" Repo.Addon
        ops <- readIORef opsR
        assertEqual "Publishing package resulted in error." (Right ()) res
        assertEqual "Template publishing logic does not work as expected." ex ops

-- In this test, an error happens after submitting a package creation request.
-- The error should be propagated.
publishTemplateFailureTest :: TestTree
publishTemplateFailureTest = testCase "Template publishing propagates unexpected errors." tc
  where
    internalError = Repo.ErrorRequest $ Repo.ServantError $ S.FailureResponse $
        S.Response (N.mkStatus 503 "Internal error.") SeqI.empty NV.http11 ""
    ex = [GetLatestVsn fakeSubject fakeRepo' fakePkgName',
          Create fakeSubject fakeRepo' fakeDesc']
    tc = do
        opsR <- newIORef ([] :: [TemplateReq])
        let mockReqH = newMockHandle opsR (Just internalError, Nothing, Nothing,
                        Right [], Right [], Right [], (alwaysOK, Right someBtPkg), Right $ lv "1", Nothing)
        repoH <- BT.makeBintrayTemplateRepoHandleWithRequestLayer fakeCreds fakeSubject mockReqH
        res <- Tp.templatePublishInternal repoH fakeNameSpace fakeTemplateName fakeReleaseLine "" Repo.Addon
        ops <- readIORef opsR
        assertEqual "Publishing package resulted in error." (Left internalError) res
        assertEqual "Template publishing logic does not work as expected." ex ops

downloadExactProjectTemplateTest :: TestTree
downloadExactProjectTemplateTest = testCase "A new project template with exact version can be downloaded." tc
  where
    filePath = "somePath"
    sdkVsn = SemVersion 1 2 3 Nothing
    pkgInfo = Repo.BintrayPackage fakePkgName fakeRepo' fakeSubject
                (fmap Repo.semToGenericVersion [SemVersion 1 0 0 Nothing, SemVersion 2 0 0 Nothing])
                "dummy"
                (MP.fromList [("template", ["true"]), ("release-line", ["1.2.3"]), ("type", ["project"])])
    onlyExact = \(Repo.PackageName pn) -> pn == "test-template-1.2.3"
    ex = [GetInfo fakeSubject fakeRepo' fakePkgNameExact',
          GetInfo fakeSubject fakeRepo' fakePkgName',
          GetLatestVsn fakeSubject fakeRepo' fakePkgNameExact',
          Download fakeSubject fakeRepo' fakePkgNameExact' (Repo.GenericVersion "1")]
    tc = do
        opsR <- newIORef ([] :: [TemplateReq])
        let mockReqH = newMockHandle opsR (Nothing, Nothing, Nothing, Right [],
                            Right [], Right [], (onlyExact, Right pkgInfo), Right $ lv "1", Nothing)
        repoH <- BT.makeBintrayTemplateRepoHandleWithRequestLayer fakeCreds fakeSubject mockReqH
        res <- Pr.createTemplateBasedProjectInternal repoH fakeNameSpace fakeTemplateName Repo.Project sdkVsn filePath
        ops <- readIORef opsR
        assertEqual "Downloading package resulted in error." (Right filePath) res
        assertEqual "Template downloading logic does not work as expected." ex ops

downloadReleaseLineProjectTemplateTest :: TestTree
downloadReleaseLineProjectTemplateTest = testCase "A new project template with release line version can be downloaded." tc
  where
    filePath = "somePath"
    sdkVsn = SemVersion 1 2 3 Nothing
    pkgInfo = Repo.BintrayPackage fakePkgName fakeRepo' fakeSubject
                (fmap Repo.semToGenericVersion [SemVersion 1 0 0 Nothing, SemVersion 2 0 0 Nothing])
                "dummy"
                (MP.fromList [("template", ["true"]), ("release-line", ["1.2.x"]), ("type", ["project"])])
    onlyExact = \(Repo.PackageName pn) -> pn == "test-template-1.2.x"
    ex = [GetInfo fakeSubject fakeRepo' fakePkgNameExact',
          GetInfo fakeSubject fakeRepo' fakePkgName',
          GetLatestVsn fakeSubject fakeRepo' fakePkgName',
          Download fakeSubject fakeRepo' fakePkgName' (Repo.GenericVersion "1")]
    tc = do
        opsR <- newIORef ([] :: [TemplateReq])
        let mockReqH = newMockHandle opsR (Nothing, Nothing, Nothing, Right [],
                            Right [], Right [], (onlyExact, Right pkgInfo), Right $ lv "1", Nothing)
        repoH <- BT.makeBintrayTemplateRepoHandleWithRequestLayer fakeCreds fakeSubject mockReqH
        res <- Pr.createTemplateBasedProjectInternal repoH fakeNameSpace fakeTemplateName Repo.Project sdkVsn filePath
        ops <- readIORef opsR
        assertEqual "Downloading package resulted in error." (Right filePath) res
        assertEqual "Template downloading logic does not work as expected." ex ops

downloadTemplateNotAvailableTest :: TestTree
downloadTemplateNotAvailableTest = testCase "An error is returned if template is not available." tc
  where
    filePath = "somePath"
    sdkVsn = SemVersion 1 2 3 Nothing
    onlyExact = const False
    ex = [GetInfo fakeSubject fakeRepo' fakePkgNameExact',
          GetInfo fakeSubject fakeRepo' fakePkgName']
    tc = do
        opsR <- newIORef ([] :: [TemplateReq])
        let mockReqH = newMockHandle opsR (Nothing, Nothing, Nothing, Right [],
                            Right [], Right [], (onlyExact, Right someBtPkg), Right $ lv "1", Nothing)
        repoH <- BT.makeBintrayTemplateRepoHandleWithRequestLayer fakeCreds fakeSubject mockReqH
        res <- Pr.createTemplateBasedProjectInternal repoH fakeNameSpace fakeTemplateName Repo.Project sdkVsn filePath
        ops <- readIORef opsR
        assertEqual "Downloading package resulted in error."
            (Left (Repo.ErrorNoSuchTemplate fakeTemplateName fakeReleaseLine Repo.Project)) res
        assertEqual "Template downloading logic does not work as expected." ex ops

listPublishedTemplatesTest :: TestTree
listPublishedTemplatesTest = testCase "Published templates can be listed." tc
  where
    ret = [Repo.PackageInfo fakePkgName [fakeVersion, fakeVersion2] "Text" MP.empty]
    exRet = [(fakePkgName, "Text", fakeVersion), (fakePkgName, "Text", fakeVersion2)]
    ex = [SearchPkgByAttrib fakeSubject fakeRepo $ fmap Repo.SearchAttribute pkgAttrs]
    tc = do
        opsR <- newIORef ([] :: [TemplateReq])
        let mockH = newMockHandle opsR (Nothing, Nothing, Nothing, Right ret,
                        Right [], Right [], (alwaysOK, Right someBtPkg), Right $ lv "1", Nothing)
        res <- BT.listPublishedTemplates mockH fakeSubject fakeRepo fakeCreds
        ops <- readIORef opsR
        assertEqual "Listing published templates resulted in error." (Right exRet) res
        assertEqual "Listing published templates logic does not work as expected." ex ops

listReposTest :: TestTree
listReposTest = testCase "Repositories can be listed." tc
  where
    ex = [ListRepos fakeSubject]
    tc = do
        opsR <- newIORef ([] :: [TemplateReq])
        let mockH = newMockHandle opsR (Nothing, Nothing, Nothing, Right [],
                        Right [fakeRepo], Right [], (alwaysOK, Right someBtPkg), Right $ lv "1", Nothing)
        res <- BT.listRepos mockH fakeSubject fakeCreds
        ops <- readIORef opsR
        assertEqual "Listing repositories resulted in error." (Right [fakeRepo]) res
        assertEqual "Repository listing logic does not work as expected." ex ops

-- this should be a property based test
parsePositiveTest :: TestTree
parsePositiveTest = testCase "parsePositive should parse 0 and positive numbers" $ do
    t "parsePositive should parse 0" (Right 0) "0"
    t "parsePositive should parse 1" (Right 1) "1"
    t "parsePositive should not parse '1a'" (Left "Cannot parse '1a' as ") "1a"
    t "parsePositive should not parse -1" (Left "The  cannot be a negative number (found: '-1')") "-1"
  where
      t desc expected raw = assertEqual desc expected (parsePositive "" raw)

listUsableAddonTemplatesTest :: TestTree
listUsableAddonTemplatesTest = testCase "Add-on templates can be listed." tc
  where
    sdkVsn = SemVersion 1 2 3 Nothing
    gv  = Repo.GenericVersion
    ret = [Repo.PackageInfo fakePkgName' [fakeVersion, gv "1", gv "2"] "Text" MP.empty,
           Repo.PackageInfo fakePkgName' [fakeVersion2, gv "1", gv "2"] "Text" MP.empty]
    templ = Repo.Template fakeTemplateNameTxt' "Text" [Repo.Revision 1, Repo.Revision 2]
                [] fakeNameSpace fakeReleaseLineTxt'
    attrs = fmap Repo.SearchAttribute [Repo.Attribute "release-line" $ Repo.AVText ["1.2.3","1.2.x"],
                                       Repo.Attribute "template" $ Repo.AVText ["true"],
                                       Repo.Attribute "type" $ Repo.AVText ["add-on"]]
    ex = [SearchPkgByAttrib fakeSubject fakeRepo' attrs,
          SearchPkgByAttrib fakeSubject fakeRepo' attrs]
    tc = do
        opsR <- newIORef ([] :: [TemplateReq])
        let mockReqH = newMockHandle opsR (Nothing, Nothing, Nothing, Right ret,
                        Right [], Right [], (alwaysOK, Right someBtPkg), Right $ lv "1", Nothing)
        repoH <- BT.makeBintrayTemplateRepoHandleWithRequestLayer fakeCreds fakeSubject mockReqH
        res <- Tp.listUsableTemplatesInternal repoH (Just Repo.Addon) [fakeNameSpace, fakeNameSpace] sdkVsn
        ops <- readIORef opsR
        -- note that listing should be done for each namespace and |ret| == 2, 2x2=4
        let lst = [templ, templ, templ, templ]
        assertEqual "Listing published add-on templates resulted in error." (Right lst) res
        assertEqual "Listing published add-on templates logic does not work as expected." ex ops

listUsableProjectTemplatesTest :: TestTree
listUsableProjectTemplatesTest = testCase "Project templates can be listed." tc
  where
    sdkVsn = SemVersion 1 2 3 Nothing
    gv  = Repo.GenericVersion
    ret = [Repo.PackageInfo fakePkgName' [fakeVersion, gv "1", gv "2"] "Text" MP.empty,
           Repo.PackageInfo fakePkgName' [fakeVersion2, gv "1", gv "2"] "Text" MP.empty]
    templ = Repo.Template fakeTemplateNameTxt' "Text" [Repo.Revision 1, Repo.Revision 2]
                [] fakeNameSpace fakeReleaseLineTxt'
    attrs = fmap Repo.SearchAttribute [Repo.Attribute "release-line" $ Repo.AVText ["1.2.3","1.2.x"],
                                       Repo.Attribute "template" $ Repo.AVText ["true"],
                                       Repo.Attribute "type" $ Repo.AVText ["project"]]
    ex = [SearchPkgByAttrib fakeSubject fakeRepo' attrs,
          SearchPkgByAttrib fakeSubject fakeRepo' attrs]
    tc = do
        opsR <- newIORef ([] :: [TemplateReq])
        let mockReqH = newMockHandle opsR (Nothing, Nothing, Nothing, Right ret,
                        Right [], Right [], (alwaysOK, Right someBtPkg), Right $ lv "1", Nothing)
        repoH <- BT.makeBintrayTemplateRepoHandleWithRequestLayer fakeCreds fakeSubject mockReqH
        res <- Tp.listUsableTemplatesInternal repoH (Just Repo.Project) [fakeNameSpace, fakeNameSpace] sdkVsn
        ops <- readIORef opsR
        -- note that listing should be done for each namespace and |ret| == 2, 2x2=4
        let lst = [templ, templ, templ, templ]
        assertEqual "Listing published project templates resulted in error." (Right lst) res
        assertEqual "Listing published project templates logic does not work as expected." ex ops
