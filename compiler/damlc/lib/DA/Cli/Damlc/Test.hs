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
import qualified Data.Map.Strict as M
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

isLocal :: LocalOrExternal -> Bool
isLocal (Local _) = True
isLocal _ = False

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

data TemplateIdentifier = TemplateIdentifier
    { package :: Maybe T.Text -- `package == Nothing` means local package
    , qualifiedTemplate :: T.Text
    }
    deriving (Eq, Ord, Show)

data ChoiceIdentifier = ChoiceIdentifier
    { packageTemplate :: TemplateIdentifier
    , choice :: T.Text
    }
    deriving (Eq, Ord, Show)

type TemplateInfo = (Maybe LF.PackageId, LF.Module, LF.Template)

data Report = Report
    { groupName :: String
    , definedChoicesInside :: S.Set ChoiceIdentifier
    , internalExercisedAnywhere :: S.Set ChoiceIdentifier
    , internalExercisedInternal :: S.Set ChoiceIdentifier
    , externalExercisedInternal :: S.Set ChoiceIdentifier
    , definedTemplatesInside :: S.Set TemplateIdentifier
    , internalCreatedAnywhere :: S.Set TemplateIdentifier
    , internalCreatedInternal :: S.Set TemplateIdentifier
    , externalCreatedInternal :: S.Set TemplateIdentifier
    }
    deriving (Show, Eq, Ord)

printTestCoverage ::
    ShowCoverage
    -> [LocalOrExternal]
    -> [(LocalOrExternal, [(VirtualResource, Either SSC.Error SS.ScenarioResult)])]
    -> IO ()
printTestCoverage ShowCoverage {getShowCoverage} allPackages results
  | any (isLeft . snd) $ concatMap snd results = pure ()
  | otherwise = do
      printReport $ report "templates/choices defined in module" isLocal
      printReport $ report "templates/choices defined outside module" (not . isLocal)
      printReport $ report "all templates/choices available" (const True)
  where
    report :: String -> (LocalOrExternal -> Bool) -> Report
    report groupName pred =
        let filteredResults :: [(LocalOrExternal, [(VirtualResource, Either SSC.Error SS.ScenarioResult)])]
            filteredResults = filter (pred . fst) results
            definedTemplatesInside = M.keysSet $ templatesDefinedIn $ map fst filteredResults
            definedChoicesInside = M.keysSet $ choicesDefinedIn $ map fst filteredResults
            exercisedInside = foldMap (exercisedChoices . snd) filteredResults
            createdInside = foldMap (createdTemplates . snd) filteredResults
            internalExercisedAnywhere = allExercisedChoices `S.intersection` definedChoicesInside
            internalExercisedInternal = exercisedInside `S.intersection` definedChoicesInside
            externalExercisedInternal = exercisedInside `S.difference` definedChoicesInside
            internalCreatedAnywhere = allCreatedTemplates `S.intersection` definedTemplatesInside
            internalCreatedInternal = createdInside `S.intersection` definedTemplatesInside
            externalCreatedInternal = createdInside `S.difference` definedTemplatesInside
        in
        Report
            { groupName
            , definedChoicesInside
            , internalExercisedAnywhere
            , internalExercisedInternal
            , externalExercisedInternal
            , definedTemplatesInside
            , internalCreatedAnywhere
            , internalCreatedInternal
            , externalCreatedInternal
            }

    printReport :: Report -> IO ()
    printReport
        Report
            { groupName
            , definedChoicesInside
            , internalExercisedAnywhere
            , definedTemplatesInside
            , internalCreatedAnywhere
            } =
        let percentage i j
              | j > 0 = show (round @Double $ 100.0 * (fromIntegral i / fromIntegral j) :: Int) <> "%"
              | otherwise = "100%"
            frac msg a b = msg ++ ": " ++ show a ++ " / " ++ show b
            pct msg a b = frac msg a b ++ " (" ++ percentage a b ++ ")"
            indent = ("  " ++)
            header = "group: " ++ groupName
            body1 =
                [ pct "choices" (S.size internalExercisedAnywhere) (S.size definedChoicesInside)
                , pct "templates" (S.size internalCreatedAnywhere) (S.size definedTemplatesInside)
                ]
            body2
              | not getShowCoverage = []
              | otherwise =
                [ "templates never created:" ] <>
                map (indent . printTemplateIdentifier) (S.toList $ definedTemplatesInside `S.difference` internalCreatedAnywhere) <>
                ["choices never executed:"] <>
                map (indent . printChoiceIdentifier) (S.toList $ definedChoicesInside `S.difference` internalExercisedAnywhere)
            msg = unlines $ header : map indent (body1 ++ body2)
        in
        putStrLn msg

    lfTemplateIdentifier :: TemplateInfo -> TemplateIdentifier
    lfTemplateIdentifier (pkgIdMay, m, t) =
        let package = fmap LF.unPackageId pkgIdMay
            qualifiedTemplate =
                LF.moduleNameString (LF.moduleName m) <> ":" <> T.concat (LF.unTypeConName (LF.tplTypeCon t))
        in
        TemplateIdentifier { package, qualifiedTemplate }

    ssIdentifierToIdentifier :: SS.Identifier -> TemplateIdentifier
    ssIdentifierToIdentifier SS.Identifier {SS.identifierPackage, SS.identifierName} =
        let package = do
                pIdSumM <- identifierPackage
                pIdSum <- SS.packageIdentifierSum pIdSumM
                case pIdSum of
                    SS.PackageIdentifierSumSelf _ -> Nothing
                    SS.PackageIdentifierSumPackageId pId -> Just $ TL.toStrict pId
            qualifiedTemplate = TL.toStrict identifierName
        in
        TemplateIdentifier { package, qualifiedTemplate }

    templatesDefinedIn :: [LocalOrExternal] -> M.Map TemplateIdentifier TemplateInfo
    templatesDefinedIn localOrExternals = M.fromList
        [ (lfTemplateIdentifier templateInfo, templateInfo)
        | localOrExternal <- localOrExternals
        , (pkgIdMay, module_) <- loeGetModules localOrExternal
        , template <- NM.toList $ LF.moduleTemplates module_
        , let templateInfo = (pkgIdMay, module_, template)
        ]

    choicesDefinedIn :: [LocalOrExternal] -> M.Map ChoiceIdentifier (TemplateInfo, LF.TemplateChoice)
    choicesDefinedIn localOrExternals = M.fromList
        [ (ChoiceIdentifier templateIdentifier name, (templateInfo, choice))
        | (templateIdentifier, templateInfo) <- M.toList $ templatesDefinedIn localOrExternals
        , let (_, _, template) = templateInfo
        , choice <- NM.toList $ LF.tplChoices template
        , let name = LF.unChoiceName $ LF.chcName choice
        ]

    allCreatedTemplates :: S.Set TemplateIdentifier
    allCreatedTemplates = foldMap (createdTemplates . snd) results

    allExercisedChoices :: S.Set ChoiceIdentifier
    allExercisedChoices = foldMap (exercisedChoices . snd) results

    createdTemplates :: [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> S.Set TemplateIdentifier
    createdTemplates results =
        S.fromList $
        [ ssIdentifierToIdentifier identifier
        | n <- scenarioNodes results
        , Just (SS.NodeNodeCreate SS.Node_Create {SS.node_CreateContractInstance}) <-
              [SS.nodeNode n]
        , Just contractInstance <- [node_CreateContractInstance]
        , Just identifier <- [SS.contractInstanceTemplateId contractInstance]
        ]

    exercisedChoices :: [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> S.Set ChoiceIdentifier
    exercisedChoices results =
        S.fromList $
        [ ChoiceIdentifier (ssIdentifierToIdentifier identifier) (TL.toStrict node_ExerciseChoiceId)
        | n <- scenarioNodes results
        , Just (SS.NodeNodeExercise SS.Node_Exercise { SS.node_ExerciseTemplateId
                                                     , SS.node_ExerciseChoiceId
                                                     }) <- [SS.nodeNode n]
        , Just identifier <- [node_ExerciseTemplateId]
        ]

    scenarioNodes :: [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> [SS.Node]
    scenarioNodes results =
        [ node
        | (_virtualResource, Right result) <- results
        , node <- V.toList $ SS.scenarioResultNodes result
        ]

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

    printTemplateIdentifier :: TemplateIdentifier -> String
    printTemplateIdentifier TemplateIdentifier { package, qualifiedTemplate } =
        T.unpack $ maybe
            qualifiedTemplate
            (\pId -> pkgIdToPkgName pId <> ":" <> qualifiedTemplate)
            package

    printChoiceIdentifier :: ChoiceIdentifier -> String
    printChoiceIdentifier ChoiceIdentifier { packageTemplate, choice } =
        printTemplateIdentifier packageTemplate <> ":" <> T.unpack choice

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
