-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Project.Tests
    ( module DA.Daml.Project.Tests
    ) where

import DA.Daml.Project.Config
import DA.Daml.Project.Types (ConfigError (..), PackagePath (..))
import DA.Daml.Project.Util (ascendants)
import System.Environment.Blank
import System.FilePath
import System.IO.Temp
import Data.List.Extra
import DA.Test.Util
import qualified Test.Tasty as Tasty
import qualified Test.Tasty.HUnit as Tasty
import Test.Tasty.HUnit ((@?=))
import qualified Test.Tasty.QuickCheck as Tasty
import qualified Data.Text as T
import Test.Tasty.QuickCheck ((==>))
import Data.Bifunctor (second)
import qualified Data.Map as Map
import qualified Data.Yaml as Y
import Control.Exception (catch, displayException)

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True -- we need this because we use withEnv in our tests
    Tasty.defaultMain $ Tasty.testGroup "DA.Daml.Assistant"
        [ testAscendants
        , testEnvironmentVariableInterpolation
        ]

testAscendants :: Tasty.TestTree
testAscendants = Tasty.testGroup "DA.Daml.Project.Util.ascendants"
    [ Tasty.testCase "unit tests" $ do
        Tasty.assertEqual "empty path" ["."] (ascendants "")
        Tasty.assertEqual "curdir path" ["."] (ascendants ".")
        Tasty.assertEqual "root path" ["/"] (ascendants "/")
        Tasty.assertEqual "home path" ["~"] (ascendants "~")
        Tasty.assertEqual "foo/bar" ["foo/bar", "foo", "."] (ascendants "foo/bar")
        Tasty.assertEqual "foo/bar/" ["foo/bar/", "foo", "."] (ascendants "foo/bar/")
        Tasty.assertEqual "./foo/bar" ["./foo/bar", "./foo", "."] (ascendants "./foo/bar")
        Tasty.assertEqual "../foo/bar" ["../foo/bar", "../foo", ".."] (ascendants "../foo/bar")
        Tasty.assertEqual "~/foo/bar" ["~/foo/bar", "~/foo", "~"] (ascendants "~/foo/bar")
        Tasty.assertEqual "/foo/bar/baz" ["/foo/bar/baz", "/foo/bar", "/foo", "/"]
            (ascendants "/foo/bar/baz")
    , Tasty.testProperty "ascendants is nonempty"
        (\p -> notNull (ascendants p))
    , Tasty.testProperty "head . ascendants == id"
        (\p -> notNull p ==> head (ascendants p) == p)
    , Tasty.testProperty "head . ascendants == id (2)"
        (\p1 p2 -> let p = p1 </> p2 in notNull p1 && notNull p2 && isRelative p2 ==>
                   head (ascendants p) == p)
    , Tasty.testProperty "tail . ascendants == ascendants . takeDirectory"
        (\p1 p2 -> let p = dropTrailingPathSeparator (p1 </> p2)
                   in notNull p1 && notNull p2 && p1 </> p2 /= p2 ==>
                      -- We use `p1 </> p2 /= p2` instead of `isRelative p2`
                      -- because, on Windows, `isRelative "\\foo" == True`,
                      -- while `x </> "\\foo" = "\\foo"`.
                      tail (ascendants p) == ascendants (takeDirectory p))
    ]

testEnvironmentVariableInterpolation :: Tasty.TestTree
testEnvironmentVariableInterpolation = Tasty.testGroup "daml.yaml environment variable interpolation"
    [ test "replace valid variable" [("MY_VERSION", "0.0.0")] "version: ${MY_VERSION}" $ withSuccess $ \p ->
        queryTopLevelField p "version" @?= "0.0.0"
    , test 
        "replace value with multiple variables"
        [("PACKAGE_TYPE", "production"), ("PACKAGE_VISIBILITY", "public")]
        "name: ${PACKAGE_TYPE}-package-${PACKAGE_VISIBILITY}"
        $ withSuccess $ \p -> queryTopLevelField p "name" @?= "production-package-public"
    , test "not replace escaped variable" [("MY_NAME", "name")] "name: \\${MY_NAME}" $ withSuccess $ \p ->
        queryTopLevelField p "name" @?= "${MY_NAME}"
    , test "replace double escaped variable" [("MY_NAME", "name")] "name: \\\\${MY_NAME}" $ withSuccess $ \p ->
        queryTopLevelField p "name" @?= "\\name"
    , test "not add syntax/structure" [("MY_NAME", "\n  - elem\n  - elem")] "name: ${MY_NAME}" $ withSuccess $ \p ->
        queryTopLevelField p "name" @?= "\n  - elem\n  - elem"
    , test "fail when variable doesn't exist" [] "name: ${MY_NAME}" $ withFailure $ \case
        ConfigFileInvalid _ (Y.AesonException "Couldn't find environment variable MY_NAME in value ${MY_NAME}") -> pure ()
        e -> Tasty.assertFailure $ "Expected failed to find environment variable error, got " <> show e
    , test "not interpolate when feature is disabled via field" [] "name: ${MY_NAME}\nenvironment-variable-interpolation: false" $ withSuccess $ \p ->
        queryTopLevelField p "name" @?= "${MY_NAME}"
    , test "replace in object names (i.e. module prefixes)" [("MY_NAME", "package")] "module-prefixes:\n  ${MY_NAME}-0.0.1: V1" $ withSuccess $ \p ->
        either (error . show) id (queryPackageConfigRequired @(Map.Map String String) ["module-prefixes"] p)
          @?= Map.singleton "package-0.0.1" "V1"
    ]
  where
    test :: String -> [(String, String)] -> String -> (Either ConfigError PackageConfig -> IO ()) -> Tasty.TestTree
    test name env damlyaml pred = 
      Tasty.testCase name $
        withSystemTempDirectory "daml-yaml-env-var-test" $ \ base -> do
          writeFile (base </> "daml.yaml") damlyaml
          res <- withEnv (second Just <$> env) (Right <$> readPackageConfig (PackagePath base))
            `catch` \(e :: ConfigError) -> pure (Left e)
          pred res
    withSuccess :: (PackageConfig -> IO ()) -> Either ConfigError PackageConfig -> IO ()
    withSuccess act (Right p) = act p
    withSuccess _ (Left e) = Tasty.assertFailure $ displayException e
    withFailure :: (ConfigError -> IO ()) -> Either ConfigError PackageConfig -> IO ()
    withFailure act (Left e) = act e
    withFailure _ (Right _) = Tasty.assertFailure "Expected failure but got success"
    queryTopLevelField :: PackageConfig -> T.Text -> String
    queryTopLevelField p field = either (error . show) id $ queryPackageConfigRequired [field] p
