-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Main entry-point of the Daml compiler
module DA.Cli.Damlc.Test (
    execTest
    , UseColor(..)
    , ShowCoverage(..)
    , RunAllTests(..)
    ) where

import Control.Monad.Except
import Control.Monad.Extra
import DA.Daml.Compiler.Output
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.PrettyScenario as SS
import qualified DA.Daml.LF.ScenarioServiceClient as SSC
import DA.Daml.Options.Types
import qualified DA.Pretty
import qualified DA.Pretty as Pretty
import Data.Either
import qualified Data.HashSet as HashSet
import Data.List.Extra
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import Data.Tuple.Extra
import qualified Data.Vector as V
import Development.IDE.Core.API
import Development.IDE.Core.IdeState.Daml
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Service.Daml
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import qualified Development.Shake as Shake
import Safe
import qualified ScenarioService as SS
import System.Console.ANSI (SGR(..), setSGRCode, Underlining(..), ConsoleIntensity(..))
import System.Directory (createDirectoryIfMissing)
import System.Exit (exitFailure)
import System.FilePath
import qualified Text.XML.Light as XML


newtype UseColor = UseColor {getUseColor :: Bool}
newtype ShowCoverage = ShowCoverage {getShowCoverage :: Bool}
newtype RunAllTests = RunAllTests {getRunAllTests :: Bool}

-- | Test a Daml file.
execTest :: [NormalizedFilePath] -> RunAllTests -> ShowCoverage -> UseColor -> Maybe FilePath -> Options -> IO ()
execTest inFiles runAllTests coverage color mbJUnitOutput opts = do
    loggerH <- getLogger opts "test"
    withDamlIdeState opts loggerH diagnosticsLogger $ \h -> do
        testRun h inFiles (optDamlLfVersion opts) runAllTests coverage color mbJUnitOutput
        diags <- getDiagnostics h
        when (any (\(_, _, diag) -> Just DsError == _severity diag) diags) exitFailure

data LocalOrExternal
    = Local LF.Module
    | External LF.ExternalPackage
    deriving (Show, Eq)

loeGetModules :: LocalOrExternal -> [(Maybe LF.PackageId, LF.Module)]
loeGetModules (Local mod) = pure (Nothing, mod)
loeGetModules (External pkg) =
    [ (Just (LF.extPackageId pkg), mod)
    | mod <- NM.elems $ LF.packageModules $ LF.extPackagePkg pkg
    ]

testRun ::
       IdeState
    -> [NormalizedFilePath]
    -> LF.Version
    -> RunAllTests
    -> ShowCoverage
    -> UseColor
    -> Maybe FilePath
    -> IO ()
testRun h inFiles lfVersion (RunAllTests runAllTests) coverage color mbJUnitOutput  = do
    -- make sure none of the files disappear
    liftIO $ setFilesOfInterest h (HashSet.fromList inFiles)

    -- take the transitive closure of all imports and run on all of them
    -- If some dependencies can't be resolved we'll get a Diagnostic out anyway, so don't worry
    deps <- runActionSync h $ mapM getDependencies inFiles
    let files = nubOrd $ concat $ inFiles : catMaybes deps

    -- get all external dependencies
    extPkgs <- fmap (nubSortOn LF.extPackageId . concat) $ runActionSync h $
      Shake.forP files $ \file -> getExternalPackages file

    results <- runActionSync h $ do
        Shake.forP files $ \file -> do
            mod <- moduleForScenario file
            mbScenarioResults <- runScenarios file
            mbScriptResults <- runScripts file
            let mbResults = liftM2 (++) mbScenarioResults mbScriptResults
            return (file, mod, mbResults)

    extResults <-
        if runAllTests
        then case headMay inFiles of
                 Nothing -> pure [] -- nothing to test
                 Just file ->
                     runActionSync h $
                     forM extPkgs $ \pkg -> do
                         (_fileDiagnostics, mbResults) <- runScenariosScriptsPkg file pkg extPkgs
                         pure (pkg, mbResults)
        else pure []

    let -- All Packages / Modules mentioned somehow
        allPackages :: [LocalOrExternal]
        allPackages = [Local mod | (_, mod, _) <- results] ++ map External extPkgs

        -- All results: subset of packages / modules that actually got scenarios run
        allResults :: [(LocalOrExternal, [(VirtualResource, Either SSC.Error SS.ScenarioResult)])]
        allResults =
            [(Local mod, result) | (_file, mod, Just result) <- results]
            ++ [(External pkg, result) | (pkg, Just result) <- extResults]

    -- print test summary after all tests have run
    printSummary color (concatMap snd allResults)

    -- print total test coverage
    printTestCoverage
        coverage
        allPackages
        allResults

    whenJust mbJUnitOutput $ \junitOutput -> do
        createDirectoryIfMissing True $ takeDirectory junitOutput
        res <- forM results $ \(file, _mod, resultM) -> do
            case resultM of
                Nothing -> fmap (file, ) $ runActionSync h $ failedTestOutput h file
                Just scenarioResults -> do
                    let render =
                            either
                                (Just . T.pack . DA.Pretty.renderPlainOneLine . prettyErr lfVersion)
                                (const Nothing)
                    pure (file, map (second render) scenarioResults)
        writeFile junitOutput $ XML.showTopElement $ toJUnit res


-- We didn't get scenario results, so we use the diagnostics as the error message for each scenario.
failedTestOutput :: IdeState -> NormalizedFilePath -> Action [(VirtualResource, Maybe T.Text)]
failedTestOutput h file = do
    mbScenarioNames <- getScenarioNames file
    diagnostics <- liftIO $ getDiagnostics h
    let errMsg = showDiagnostics diagnostics
    pure $ map (, Just errMsg) $ fromMaybe [VRScenario file "Unknown"] mbScenarioNames


printSummary :: UseColor -> [(VirtualResource, Either SSC.Error SSC.ScenarioResult)] -> IO ()
printSummary color res =
  liftIO $ do
    putStrLn $
      unlines
        [ setSGRCode [SetUnderlining SingleUnderline, SetConsoleIntensity BoldIntensity]
        , "Test Summary" <> setSGRCode []
        ]
    printScenarioResults color res

printTestCoverage ::
    ShowCoverage
    -> [LocalOrExternal]
    -> [(LocalOrExternal, [(VirtualResource, Either SSC.Error SS.ScenarioResult)])]
    -> IO ()
printTestCoverage ShowCoverage {getShowCoverage} allPackages results
  | any (isLeft . snd) $ concatMap snd results = pure ()
  | otherwise = do
      putStrLn $
          unwords
              [ "test coverage: templates"
              , percentage coveredNrOfTemplates nrOfTemplates <> ","
              , "choices"
              , percentage coveredNrOfChoices nrOfChoices
              , unlines $
                  ["templates:"] <> [printFullTemplateName (fullTemplateName pidM m t) | (pidM, m, t) <- templates] <>
                  ["choices:"] <> [printFullTemplateName (fullTemplateName pidM m t) <> ":" <> T.unpack c | (pidM, m, t, LF.ChoiceName c) <- choices] <>
                  ["covered templates:"] <> map printFullTemplateName (S.toList coveredTemplates) <>
                  ["covered choices:"] <> [printFullTemplateName tName <> ":" <> T.unpack c | (tName, c) <- S.toList coveredChoices]
              ]
      when getShowCoverage $ do
          putStrLn $
              unlines $
              ["templates never created:"] <> map printFullTemplateName (S.toList missingTemplates) <>
              ["choices never executed:"] <>
              [printFullTemplateName t <> ":" <> T.unpack c | (t, c) <- S.toList missingChoices]
  where
    modules :: [(Maybe LF.PackageId, LF.Module)]
    modules = concatMap loeGetModules allPackages

    pkgIdToPkgName :: T.Text -> T.Text
    pkgIdToPkgName targetPid =
        case mapMaybe isTargetPackage allPackages of
          [] -> targetPid
          [matchingPkg] -> maybe targetPid (LF.unPackageName . LF.packageName) $ LF.packageMetadata $ LF.extPackagePkg matchingPkg
          _ -> error ("pkgIdToPkgName: more than one package matching name " <> T.unpack targetPid)
        where
            isTargetPackage loe
                | External pkg <- loe
                , targetPid == LF.unPackageId (LF.extPackageId pkg)
                = Just pkg
                | otherwise
                = Nothing

    templates = [(pidM, m, t) | (pidM, m) <- modules, t <- NM.toList $ LF.moduleTemplates m]
    choices = [(pidM, m, t, n) | (pidM, m, t) <- templates, n <- NM.names $ LF.tplChoices t]
    percentage i j
      | j > 0 = show (round @Double $ 100.0 * (fromIntegral i / fromIntegral j) :: Int) <> "%"
      | otherwise = "100%"
    allScenarioNodes = [n | (_, results) <- results, (_vr, Right res) <- results, n <- V.toList $ SS.scenarioResultNodes res]
    coveredTemplatesAll =
        nubSort $
        [ templateId
        | n <- allScenarioNodes
        , Just (SS.NodeNodeCreate SS.Node_Create {SS.node_CreateContractInstance}) <-
              [SS.nodeNode n]
        , Just contractInstance <- [node_CreateContractInstance]
        , Just templateId <- [SS.contractInstanceTemplateId contractInstance]
        ]
    coveredTemplates =
        S.fromList [fullTemplateName pidM m t | (pidM, m, t) <- templates] `S.intersection`
        S.fromList
            [ fullTemplateNameProto tId
            | tId <- coveredTemplatesAll
            ]
    missingTemplates =
        S.fromList [fullTemplateName pidM m t | (pidM, m, t) <- templates] `S.difference`
        S.fromList
            [ fullTemplateNameProto tId
            | tId <- coveredTemplatesAll
            ]
    coveredChoicesAll =
        nubSort $
        [ (templateId, node_ExerciseChoiceId)
        | n <- allScenarioNodes
        , Just (SS.NodeNodeExercise SS.Node_Exercise { SS.node_ExerciseTemplateId
                                                     , SS.node_ExerciseChoiceId
                                                     }) <- [SS.nodeNode n]
        , Just templateId <- [node_ExerciseTemplateId]
        ]
    coveredChoices =
        S.fromList [(fullTemplateName pidM m t, LF.unChoiceName n) | (pidM, m, t, n) <- choices] `S.intersection`
        S.fromList
            [ (fullTemplateNameProto t, TL.toStrict c)
            | (t, c) <- coveredChoicesAll
            ]
    missingChoices =
        S.fromList [(fullTemplateName pidM m t, LF.unChoiceName n) | (pidM, m, t, n) <- choices] `S.difference`
        S.fromList
            [ (fullTemplateNameProto t, TL.toStrict c)
            | (t, c) <- coveredChoicesAll
            ]
    nrOfTemplates = length templates
    nrOfChoices = length choices
    coveredNrOfChoices = length coveredChoices
    coveredNrOfTemplates = length coveredTemplates
    printFullTemplateName (pIdM, name) =
        T.unpack $ maybe name (\pId -> pkgIdToPkgName pId <> ":" <> name) pIdM
    fullTemplateName pidM m t =
        ( fmap LF.unPackageId pidM
        , (LF.moduleNameString $ LF.moduleName m) <> ":" <>
          (T.concat $ LF.unTypeConName $ LF.tplTypeCon t))
    fullTemplateNameProto SS.Identifier {SS.identifierPackage, SS.identifierName} =
        ( do pIdSumM <- identifierPackage
             pIdSum <- SS.packageIdentifierSum pIdSumM
             case pIdSum of
                 SS.PackageIdentifierSumSelf _ -> Nothing
                 SS.PackageIdentifierSumPackageId pId -> Just $ TL.toStrict pId
        , TL.toStrict identifierName)

printScenarioResults :: UseColor -> [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> IO ()
printScenarioResults color results = do
    liftIO $ forM_ results $ \(VRScenario vrFile vrName, resultOrErr) -> do
      let name = DA.Pretty.string (fromNormalizedFilePath vrFile) <> ":" <> DA.Pretty.pretty vrName
      let stringStyleToRender = if getUseColor color then DA.Pretty.renderColored else DA.Pretty.renderPlain
      putStrLn $ stringStyleToRender $
        case resultOrErr of
          Left _err -> name <> ": " <> DA.Pretty.error_ "failed"
          Right result -> name <> ": " <> prettyResult result


prettyErr :: LF.Version -> SSC.Error -> DA.Pretty.Doc Pretty.SyntaxClass
prettyErr lfVersion err = case err of
    SSC.BackendError berr ->
        DA.Pretty.string (show berr)
    SSC.ScenarioError serr ->
        SS.prettyBriefScenarioError
          (LF.initWorld [] lfVersion)
          serr
    SSC.ExceptionError e -> DA.Pretty.string $ show e


prettyResult :: SS.ScenarioResult -> DA.Pretty.Doc Pretty.SyntaxClass
prettyResult result =
    let nTx = length (SS.scenarioResultScenarioSteps result)
        nActive = length $ filter (SS.isActive (SS.activeContractsFromScenarioResult result)) (V.toList (SS.scenarioResultNodes result))
    in DA.Pretty.typeDoc_ "ok, "
    <> DA.Pretty.int nActive <> DA.Pretty.typeDoc_ " active contracts, "
    <> DA.Pretty.int nTx <> DA.Pretty.typeDoc_ " transactions."


toJUnit :: [(NormalizedFilePath, [(VirtualResource, Maybe T.Text)])] -> XML.Element
toJUnit results =
    XML.node
        (XML.unqual "testsuites")
        ([ XML.Attr (XML.unqual "errors") "0"
           -- For now we only have successful tests and falures
         , XML.Attr (XML.unqual "failures") (show failures)
         , XML.Attr (XML.unqual "tests") (show tests)
         ],
         map handleFile results)
    where
        tests = length $ concatMap snd results
        failures = length $ concatMap (mapMaybe snd . snd) results
        handleFile :: (NormalizedFilePath, [(VirtualResource, Maybe T.Text)]) -> XML.Element
        handleFile (f, vrs) =
            XML.node
                (XML.unqual "testsuite")
                ([ XML.Attr (XML.unqual "name") (fromNormalizedFilePath f)
                 , XML.Attr (XML.unqual "tests") (show $ length vrs)
                 ],
                 map (handleVR f) vrs)
        handleVR :: NormalizedFilePath -> (VirtualResource, Maybe T.Text) -> XML.Element
        handleVR f (vr, mbErr) =
            XML.node
                (XML.unqual "testcase")
                ([ XML.Attr (XML.unqual "name") (T.unpack $ vrScenarioName vr)
                 , XML.Attr (XML.unqual "classname") (fromNormalizedFilePath f)
                 ],
                 maybe [] (\err -> [XML.node (XML.unqual "failure") (T.unpack err)]) mbErr
                )
