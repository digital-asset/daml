-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase #-}

-- | Main entry-point of the Daml compiler
module DA.Cli.Damlc.Test (
    execTest
    , UseColor(..)
    , ShowCoverage(..)
    , RunAllTests(..)
    -- , Summarize(..)
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
import Text.Printf


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

loeGetModules :: LocalOrExternal -> [(LF.Module, a -> LF.Qualified a)]
loeGetModules (Local mod) = pure (mod, LF.Qualified LF.PRSelf (LF.moduleName mod))
loeGetModules (External pkg) =
    [ (mod, qualifier)
    | mod <- NM.elems $ LF.packageModules $ LF.extPackagePkg pkg
    , let qualifier = LF.Qualified (LF.PRImport (LF.extPackageId pkg)) (LF.moduleName mod)
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

data ContractIdentifier = ContractIdentifier
    { package :: Maybe T.Text -- `package == Nothing` means local package
    , qualifiedName :: T.Text
    }
    deriving (Eq, Ord, Show)

data ChoiceIdentifier = ChoiceIdentifier
    { packageContract :: ContractIdentifier
    , choice :: T.Text
    }
    deriving (Eq, Ord, Show)

data Variety tpl iface = TemplateV tpl | InterfaceV iface
    deriving (Eq, Ord, Show)

data Report = Report
    { groupName :: String
    , definedChoicesInside ::
        M.Map
          ChoiceIdentifier
          (Variety
            (LF.Qualified LF.Template, LF.TemplateChoice)
            (LF.Qualified LF.DefInterface, LF.TemplateChoice))
    , internalExercisedAnywhere :: S.Set ChoiceIdentifier
    , internalExercisedInternal :: S.Set ChoiceIdentifier
    , externalExercisedInternal :: S.Set ChoiceIdentifier
    , definedContractsInside ::
        M.Map
          ContractIdentifier
          (Variety
            (LF.Qualified LF.Template)
            (LF.Qualified LF.DefInterface))
    , definedInterfaceImplementationsInside ::
        M.Map (ContractIdentifier, ContractIdentifier) LF.InterfaceInstanceBody
    , internalCreatedAnywhere :: S.Set ContractIdentifier
    , internalCreatedInternal :: S.Set ContractIdentifier
    , externalCreatedInternal :: S.Set ContractIdentifier
    }
    deriving (Show)

lfTemplateIdentifier :: LF.Qualified LF.Template -> ContractIdentifier
lfTemplateIdentifier = lfMkNameIdentifier . fmap LF.tplTypeCon

lfInterfaceIdentifier :: LF.Qualified LF.DefInterface -> ContractIdentifier
lfInterfaceIdentifier = lfMkNameIdentifier . fmap LF.intName

lfMkNameIdentifier :: LF.Qualified LF.TypeConName -> ContractIdentifier
lfMkNameIdentifier LF.Qualified { qualPackage, qualModule, qualObject } =
    let package =
            case qualPackage of
              LF.PRSelf -> Nothing
              LF.PRImport (LF.PackageId pid) -> Just pid
        qualifiedName =
            LF.moduleNameString qualModule
                <> ":"
                <> T.concat (LF.unTypeConName qualObject)
    in
    ContractIdentifier { package, qualifiedName }

ssIdentifierToIdentifier :: SS.Identifier -> ContractIdentifier
ssIdentifierToIdentifier SS.Identifier {SS.identifierPackage, SS.identifierName} =
    let package = do
            pIdSumM <- identifierPackage
            pIdSum <- SS.packageIdentifierSum pIdSumM
            case pIdSum of
                SS.PackageIdentifierSumSelf _ -> Nothing
                SS.PackageIdentifierSumPackageId pId -> Just $ TL.toStrict pId
        qualifiedName = TL.toStrict identifierName
    in
    ContractIdentifier { package, qualifiedName }

printTestCoverage ::
    ShowCoverage
    -> [LocalOrExternal]
    -> [(LocalOrExternal, [(VirtualResource, Either SSC.Error SS.ScenarioResult)])]
    -> IO ()
printTestCoverage _ allPackages results
  | any (isLeft . snd) $ concatMap snd results = pure ()
  | otherwise = printReport
  where
    printReport :: IO ()
    printReport =
        let countWhere pred = M.size . M.filter pred

            allContracts = contractsDefinedIn allPackages
            localTemplates = M.filterWithKey pred allContracts
              where
                pred (ContractIdentifier Nothing _) (TemplateV _) = True
                pred _ _ = False
            localTemplatesCreated = M.intersection allCreatedContracts localTemplates

            allChoices = choicesDefinedIn allPackages
            localTemplateChoices = M.filterWithKey pred allChoices
              where
                pred (ChoiceIdentifier (ContractIdentifier Nothing _) _) (TemplateV _) = True
                pred _ _ = False
            localTemplateChoicesExercised = M.intersection allExercisedChoices localTemplateChoices

            allImplementations = interfaceImplementationsDefinedIn allPackages
            fillInImplementation (ifaceId, _) (loe, instanceBody) = (loe, instanceBody, def)
              where
                def = case M.lookup ifaceId allContracts of
                        Just (InterfaceV def) -> Just def
                        _ -> Nothing

            allImplementationChoices = M.fromList $ do
                (k@(_, contractId), (loe, body, mdef)) <- M.toList $ M.mapWithKey fillInImplementation allImplementations
                def <- maybeToList mdef
                choice <- NM.toList $ LF.intChoices $ LF.qualObject def
                let name = LF.unChoiceName $ LF.chcName choice
                guard (name /= "Archive")
                pure (ChoiceIdentifier contractId name, (k, loe, body, def, choice))
            allExercisedImplementationChoices = M.intersection allExercisedChoices allImplementationChoices
        in
        putStrLn $
        unlines
        [ printf "Modules internal to this package:"
        -- Can't have any external tests that exercise internals, as that would
        -- require a circular dependency, so we only report local test results
        , printf "- Templates"
        , printf "  %d defined" (M.size localTemplates)
        , printf "  %d created" (M.size localTemplatesCreated)
        , printf "- Template choices"
        , printf "  %d defined" (M.size localTemplateChoices)
        , printf "  %d exercised" (M.size localTemplateChoicesExercised)
        , printf "- Interfaces"
        , printf "  %d implementations defined" (countWhere (isLocal . fst) allImplementations)
        , printf "    %d implementations of internal interfaces" (countWhere (isLocal . fst) allImplementations)
        , printf "    %d implementations of external interfaces" (countWhere (not . isLocal . fst) allImplementations)
        , printf "- Interface choices"
        , printf "  %d defined" (countWhere (\(_, loe, _, _, _) -> isLocal loe) allImplementationChoices)
        , printf "  %d exercised" (countWhere (any isLocal) allExercisedImplementationChoices)
        ]

    contractsDefinedIn :: [LocalOrExternal] -> M.Map ContractIdentifier (Variety (LF.Qualified LF.Template) (LF.Qualified LF.DefInterface))
    contractsDefinedIn = fmap TemplateV . templatesDefinedIn <> fmap InterfaceV . interfacesDefinedIn

    templatesDefinedIn :: [LocalOrExternal] -> M.Map ContractIdentifier (LF.Qualified LF.Template)
    templatesDefinedIn localOrExternals = M.fromList
        [ (lfTemplateIdentifier templateInfo, templateInfo)
        | localOrExternal <- localOrExternals
        , (module_, qualifier) <- loeGetModules localOrExternal
        , template <- NM.toList $ LF.moduleTemplates module_
        , let templateInfo = qualifier template
        ]

    interfacesDefinedIn :: [LocalOrExternal] -> M.Map ContractIdentifier (LF.Qualified LF.DefInterface)
    interfacesDefinedIn localOrExternals = M.fromList
        [ (lfInterfaceIdentifier interfaceInfo, interfaceInfo)
        | localOrExternal <- localOrExternals
        , (module_, qualifier) <- loeGetModules localOrExternal
        , interface <- NM.toList $ LF.moduleInterfaces module_
        , let interfaceInfo = qualifier interface
        ]

    choicesDefinedIn :: [LocalOrExternal] -> M.Map ChoiceIdentifier (Variety (LF.Qualified LF.Template, LF.TemplateChoice) (LF.Qualified LF.DefInterface, LF.TemplateChoice))
    choicesDefinedIn = fmap TemplateV . templateChoicesDefinedIn <> fmap InterfaceV . interfaceChoicesDefinedIn

    templateChoicesDefinedIn :: [LocalOrExternal] -> M.Map ChoiceIdentifier (LF.Qualified LF.Template, LF.TemplateChoice)
    templateChoicesDefinedIn localOrExternals = M.fromList
        [ (ChoiceIdentifier templateIdentifier name, (templateInfo, choice))
        | (templateIdentifier, templateInfo) <- M.toList $ templatesDefinedIn localOrExternals
        , choice <- NM.toList $ LF.tplChoices $ LF.qualObject templateInfo
        , let name = LF.unChoiceName $ LF.chcName choice
        ]

    interfaceChoicesDefinedIn :: [LocalOrExternal] -> M.Map ChoiceIdentifier (LF.Qualified LF.DefInterface, LF.TemplateChoice)
    interfaceChoicesDefinedIn localOrExternals = M.fromList
        [ (ChoiceIdentifier interfaceIdentifier name, (interfaceInfo, choice))
        | (interfaceIdentifier, interfaceInfo) <- M.toList $ interfacesDefinedIn localOrExternals
        , choice <- NM.toList $ LF.intChoices $ LF.qualObject interfaceInfo
        , let name = LF.unChoiceName $ LF.chcName choice
        ]

    interfaceImplementationsDefinedIn :: [LocalOrExternal] -> M.Map (ContractIdentifier, ContractIdentifier) (LocalOrExternal, LF.InterfaceInstanceBody)
    interfaceImplementationsDefinedIn localOrExternals = M.fromList $
        [ ((lfMkNameIdentifier tpiInterface, templateIdentifier), (loe, tpiBody))
        | loe <- localOrExternals
        , (templateIdentifier, templateInfo) <- M.toList $ templatesDefinedIn [loe]
        , LF.TemplateImplements { tpiInterface, tpiBody }
            <- NM.toList $ LF.tplImplements $ LF.qualObject templateInfo
        ] ++
        [ ((interfaceIdentifier, lfMkNameIdentifier iciTemplate), (loe, iciBody))
        | loe <- localOrExternals
        , (interfaceIdentifier, interfaceInfo) <- M.toList $ interfacesDefinedIn [loe]
        , LF.InterfaceCoImplements { iciTemplate, iciBody }
            <- NM.toList $ LF.intCoImplements $ LF.qualObject interfaceInfo
        ]

    allCreatedContracts :: M.Map ContractIdentifier [LocalOrExternal]
    allCreatedContracts = M.unionsWith (<>) $ map (uncurry createdContracts) results

    allExercisedChoices :: M.Map ChoiceIdentifier [LocalOrExternal]
    allExercisedChoices = M.unionsWith (<>) $ map (uncurry exercisedChoices) results

    createdContracts :: LocalOrExternal -> [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> M.Map ContractIdentifier [LocalOrExternal]
    createdContracts loe results =
        M.fromList $
        [ (ssIdentifierToIdentifier identifier, [loe])
        | n <- scenarioNodes results
        , Just (SS.NodeNodeCreate SS.Node_Create {SS.node_CreateContractInstance}) <-
              [SS.nodeNode n]
        , Just contractInstance <- [node_CreateContractInstance]
        , Just identifier <- [SS.contractInstanceTemplateId contractInstance]
        ]

    exercisedChoices :: LocalOrExternal -> [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> M.Map ChoiceIdentifier [LocalOrExternal]
    exercisedChoices loe results =
        M.fromList $
        [ (choiceIdentifier, [loe])
        | n <- scenarioNodes results
        , Just (SS.NodeNodeExercise SS.Node_Exercise { SS.node_ExerciseTemplateId
                                                     , SS.node_ExerciseChoiceId
                                                     }) <- [SS.nodeNode n]
        , Just identifier <- [node_ExerciseTemplateId]
        , let choiceIdentifier = ChoiceIdentifier (ssIdentifierToIdentifier identifier) (TL.toStrict node_ExerciseChoiceId)
        ]

    scenarioNodes :: [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> [SS.Node]
    scenarioNodes results =
        [ node
        | (_virtualResource, Right result) <- results
        , node <- V.toList $ SS.scenarioResultNodes result
        ]

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
