-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}

module DA.Cli.Damlc.Test.TestResults where

import qualified DA.Daml.LF.Ast as LF
import qualified Data.NameMap as NM
import qualified TestResults as TR
import qualified ScenarioService as SS
import qualified DA.Daml.LF.ScenarioServiceClient as SSC
import Development.IDE.Core.RuleTypes.Daml (VirtualResource (..))
import qualified Data.Vector as V
import qualified Proto3.Suite as Proto
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Set as S
import qualified Com.Daml.DamlLfDev.DamlLf1 as LF1
import qualified Data.Map.Strict as M
import Data.Maybe (mapMaybe, isJust)
import Text.Printf

class Protobuf a b | a -> b, b -> a where
    decode :: a -> Maybe b
    encode :: b -> a

data TestResults = TestResults
    -- What was defined
    { templates :: M.Map TemplateIdentifier Choices
    , interfaces :: M.Map InterfaceIdentifier Choices
    , interfaceInstances :: S.Set InterfaceInstanceIdentifier

    -- What was used
    , created :: M.Map TemplateIdentifier (S.Set PackageId)
    , exercised :: M.Map T.Text (M.Map TemplateIdentifier (S.Set PackageId))
    }
    deriving Show

vecToSet :: (Ord b, Protobuf a b) => V.Vector a -> Maybe (S.Set b)
vecToSet = fmap S.fromList . traverse decode . V.toList

setToVec :: Protobuf a b => S.Set b -> V.Vector a
setToVec = V.fromList . map encode . S.toList

    {-
instance Protobuf TR.TestResults TestResults where
    decode (TR.TestResults interfaces interfaceChoices templates templateChoices createdInternally exercisedInternally createdExternally exercisedExternally) =
        TestResults
            <$> vecToSet interfaces
            <*> vecToSet interfaceChoices
            <*> vecToSet templates
            <*> vecToSet templateChoices
            <*> vecToSet createdInternally
            <*> vecToSet exercisedInternally
            <*> vecToSet createdExternally
            <*> vecToSet exercisedExternally
    encode (TestResults interfaces interfaceChoices templates templateChoices createdInternally exercisedInternally createdExternally exercisedExternally) =
        TR.TestResults
            (setToVec interfaces)
            (setToVec interfaceChoices)
            (setToVec templates)
            (setToVec templateChoices)
            (setToVec createdInternally)
            (setToVec exercisedInternally)
            (setToVec createdExternally)
            (setToVec exercisedExternally)

instance Protobuf TR.ChoiceIdentifier ChoiceIdentifier where
    decode (TR.ChoiceIdentifier choice contract) = ChoiceIdentifier (TL.toStrict choice) <$> (decode =<< contract)
    encode (ChoiceIdentifier choice contract) = TR.ChoiceIdentifier (TL.fromStrict choice) (Just (encode contract))

instance Protobuf TR.ContractIdentifier ContractIdentifier where
    decode (TR.ContractIdentifier qualifiedName packageId) = ContractIdentifier (TL.toStrict qualifiedName) <$> (decode =<< packageId)
    encode (ContractIdentifier qualifiedName packageId) = TR.ContractIdentifier (TL.fromStrict qualifiedName) (Just (encode packageId))
    -}

instance Protobuf TR.TestResults TestResults where
    decode _ = undefined
    encode _ = undefined

    {-
instance Protobuf TR.ChoiceIdentifier ChoiceIdentifier where
    decode _ = undefined
    encode _ = undefined
    -}

    {-
instance Protobuf TR.ContractIdentifier ContractIdentifier where
    decode _ = undefined
    encode _ = undefined
    -}

instance Protobuf TR.PackageId PackageId where
    decode (TR.PackageId (Just (TR.PackageIdVarietyLocal LF1.Unit)) name) = Just (PackageId Nothing (TL.toStrict name))
    decode (TR.PackageId (Just (TR.PackageIdVarietyExternal t)) name) = Just (PackageId (Just (TL.toStrict t)) (TL.toStrict name))
    decode _ = Nothing
    encode (PackageId Nothing name) = TR.PackageId (Just (TR.PackageIdVarietyLocal LF1.Unit)) (TL.fromStrict name)
    encode (PackageId (Just ext) name) = TR.PackageId (Just (TR.PackageIdVarietyExternal (TL.fromStrict ext))) (TL.fromStrict name)

saveTestResults :: FilePath -> TestResults -> IO ()
saveTestResults file testResults = do
    let bs = Proto.toLazyByteString (encode testResults)
    BSL.writeFile file bs

loadTestResults :: FilePath -> IO (Maybe TestResults)
loadTestResults file = do
    bs <- BS.readFile file
    pure $ case Proto.fromByteString bs of
      Right result -> decode result
      _ -> Nothing

data LocalOrExternal
    = Local LF.Module
    | External LF.ExternalPackage
    deriving (Show, Eq)

isLocal :: LocalOrExternal -> Bool
isLocal (Local _) = True
isLocal _ = False

loeGetModules :: LocalOrExternal -> [LF.Module]
loeGetModules (Local mod) = [mod]
loeGetModules (External pkg) = NM.elems $ LF.packageModules $ LF.extPackagePkg pkg

loeToPackageId :: (T.Text -> T.Text) -> LocalOrExternal -> PackageId
loeToPackageId pkgIdToPkgName loe =
    let variety = case loe of
          Local _ -> Nothing
          External pkg -> Just (LF.unPackageId (LF.extPackageId pkg))
        name = maybe "" pkgIdToPkgName variety
    in
    PackageId { variety, name }

-- Contracts
data TemplateIdentifier = TemplateIdentifier
    { qualifiedName :: T.Text
    , package :: PackageId
    }
    deriving (Eq, Ord, Show)

newtype InterfaceIdentifier = InterfaceIdentifier { unInterfaceIdentifier :: TemplateIdentifier }
    deriving (Eq, Ord, Show)

data InterfaceInstanceIdentifier = InterfaceInstanceIdentifier
    { instanceTemplate :: TemplateIdentifier
    , instanceInterface :: InterfaceIdentifier
    , instancePackage :: PackageId
      -- ^ package where the instance was defined, as opposed to where the template/interface was defined
    }
    deriving (Eq, Ord, Show)

    {-
data ContractIdentifier
    = TemplateContract TemplateIdentifier
    | InterfaceContract InterfaceIdentifier
    | InterfaceInstanceContract InterfaceInstanceIdentifier
    deriving (Eq, Ord, Show)
    -}

-- Choices
type Choices = S.Set T.Text

data PackageId = PackageId
    { variety :: Maybe T.Text -- `variety == Nothing` means local package
    , name :: T.Text
    }
    deriving (Eq, Ord, Show)

isLocalPkgId :: PackageId -> Bool
isLocalPkgId PackageId { variety = Nothing } = True
isLocalPkgId _ = False

lfTemplateIdentifier :: PackageId -> LF.Module -> LF.Template -> TemplateIdentifier
lfTemplateIdentifier packageId mod tpl =
    lfMkNameIdentifier packageId mod $ LF.tplTypeCon tpl

lfInterfaceIdentifier :: PackageId -> LF.Module -> LF.DefInterface -> InterfaceIdentifier
lfInterfaceIdentifier packageId mod iface =
    InterfaceIdentifier $ lfMkNameIdentifier packageId mod $ LF.intName iface

lfMkNameIdentifier :: PackageId -> LF.Module -> LF.TypeConName -> TemplateIdentifier
lfMkNameIdentifier packageId mod typeConName =
    let qualifiedName =
            LF.moduleNameString (LF.moduleName mod)
                <> ":"
                <> T.concat (LF.unTypeConName typeConName)
    in
    TemplateIdentifier { package = packageId, qualifiedName }

ssIdentifierToPackage :: (T.Text -> T.Text) -> SS.Identifier -> PackageId
ssIdentifierToPackage pkgIdToPkgName SS.Identifier {SS.identifierPackage} =
    let variety = do
            pIdSumM <- identifierPackage
            pIdSum <- SS.packageIdentifierSum pIdSumM
            case pIdSum of
                SS.PackageIdentifierSumSelf _ -> Nothing
                SS.PackageIdentifierSumPackageId pId -> Just $ TL.toStrict pId
        name = maybe "" pkgIdToPkgName variety
    in
    PackageId { variety, name }

ssIdentifierToIdentifier :: (T.Text -> T.Text) -> SS.Identifier -> TemplateIdentifier
ssIdentifierToIdentifier pkgIdToPkgName identifier@SS.Identifier {SS.identifierName} =
    TemplateIdentifier
        { package = ssIdentifierToPackage pkgIdToPkgName identifier
        , qualifiedName = TL.toStrict identifierName
        }

allTemplateChoices :: TestResults -> M.Map T.Text TemplateIdentifier
allTemplateChoices testResults = M.fromList
    [ (choice, templateIdentifier)
    | (templateIdentifier, choices) <- M.toList (templates testResults)
    , choice <- S.toList choices
    ]

allInterfaceChoices :: TestResults -> M.Map T.Text InterfaceIdentifier
allInterfaceChoices testResults = M.fromList
    [ (choice, interfaceIdentifier)
    | (interfaceIdentifier, choices) <- M.toList (interfaces testResults)
    , choice <- S.toList choices
    ]

scenarioResultsToTestResults
    :: [LocalOrExternal]
    -> [(LocalOrExternal, [(VirtualResource, Either SSC.Error SS.ScenarioResult)])]
    -> TestResults
scenarioResultsToTestResults allPackages results =
    TestResults
        { templates = fmap (extractChoicesFromTemplate . thd) $ templatesDefinedIn allPackages
        , interfaces = fmap (extractChoicesFromInterface . thd) $ interfacesDefinedIn allPackages
        , interfaceInstances = interfaceImplementationsDefinedIn allPackages
        , created = allCreatedTemplates
        , exercised = allExercisedChoices
        }
    where
    templatesDefinedIn :: [LocalOrExternal] -> M.Map TemplateIdentifier (PackageId, LF.Module, LF.Template)
    templatesDefinedIn localOrExternals = M.fromList
        [ ( lfTemplateIdentifier pkgId module_ template
          , (pkgId, module_, template)
          )
        | localOrExternal <- localOrExternals
        , module_ <- loeGetModules localOrExternal
        , template <- NM.toList $ LF.moduleTemplates module_
        , let pkgId = loeToPackageId pkgIdToPkgName localOrExternal
        ]

    thd :: (a, b, c) -> c
    thd (_, _, c) = c

    extractChoicesFromTemplate :: LF.Template -> Choices
    extractChoicesFromTemplate template
        = S.fromList
        $ map (LF.unChoiceName . LF.chcName)
        $ NM.toList
        $ LF.tplChoices template

    interfacesDefinedIn :: [LocalOrExternal] -> M.Map InterfaceIdentifier (PackageId, LF.Module, LF.DefInterface)
    interfacesDefinedIn localOrExternals = M.fromList
        [ ( lfInterfaceIdentifier pkgId module_ interface
          , (pkgId, module_, interface)
          )
        | localOrExternal <- localOrExternals
        , module_ <- loeGetModules localOrExternal
        , interface <- NM.toList $ LF.moduleInterfaces module_
        , let pkgId = loeToPackageId pkgIdToPkgName localOrExternal
        ]

    extractChoicesFromInterface :: LF.DefInterface -> Choices
    extractChoicesFromInterface interface
        = S.fromList
        $ filter (/= "Archive")
        $ map (LF.unChoiceName . LF.chcName)
        $ NM.toList
        $ LF.intChoices interface

    interfaceImplementationsDefinedIn :: [LocalOrExternal] -> S.Set InterfaceInstanceIdentifier
    interfaceImplementationsDefinedIn localOrExternals = S.fromList $
        [ InterfaceInstanceIdentifier
            { instanceTemplate = templateIdentifier
            , instanceInterface = InterfaceIdentifier $ lfMkNameIdentifier pkgId module_ (LF.qualObject tpiInterface)
            , instancePackage = package templateIdentifier
            }
        | loe <- localOrExternals
        , (templateIdentifier, (pkgId, module_, templateInfo)) <- M.toList $ templatesDefinedIn [loe]
        , LF.TemplateImplements { tpiInterface }
            <- NM.toList $ LF.tplImplements templateInfo
        ] ++
        [ InterfaceInstanceIdentifier
            { instanceTemplate = lfMkNameIdentifier pkgId module_ (LF.qualObject iciTemplate)
            , instanceInterface = interfaceIdentifier
            , instancePackage = package (unInterfaceIdentifier interfaceIdentifier)
            }
        | loe <- localOrExternals
        , (interfaceIdentifier, (pkgId, module_, interfaceInfo)) <- M.toList $ interfacesDefinedIn [loe]
        , LF.InterfaceCoImplements { iciTemplate }
            <- NM.toList $ LF.intCoImplements interfaceInfo
        ]

    allCreatedTemplates :: M.Map TemplateIdentifier (S.Set PackageId)
    allCreatedTemplates = M.unionsWith (<>) $ map (uncurry templatesCreatedIn) results

    allExercisedChoices :: M.Map T.Text (M.Map TemplateIdentifier (S.Set PackageId))
    allExercisedChoices = M.unionsWith (M.unionWith (<>)) $ map (uncurry choicesExercisedIn) results

    templatesCreatedIn :: LocalOrExternal -> [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> M.Map TemplateIdentifier (S.Set PackageId)
    templatesCreatedIn loe results = M.fromListWith (<>)
        [ ( ssIdentifierToIdentifier pkgIdToPkgName identifier
          , S.singleton (loeToPackageId pkgIdToPkgName loe)
          )
        | n <- scenarioNodes results
        , Just (SS.NodeNodeCreate SS.Node_Create {SS.node_CreateContractInstance}) <-
              [SS.nodeNode n]
        , Just contractInstance <- [node_CreateContractInstance]
        , Just identifier <- [SS.contractInstanceTemplateId contractInstance]
        ]

    choicesExercisedIn :: LocalOrExternal -> [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> M.Map T.Text (M.Map TemplateIdentifier (S.Set PackageId))
    choicesExercisedIn loe results = M.fromListWith (<>)
        [ ( TL.toStrict node_ExerciseChoiceId
          , M.singleton
              (ssIdentifierToIdentifier pkgIdToPkgName identifier)
              (S.singleton (loeToPackageId pkgIdToPkgName loe))
          )
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

templateChoiceExercised :: TestResults -> T.Text -> TemplateIdentifier -> Maybe (S.Set PackageId)
templateChoiceExercised testResults name templateIdentifier = do
    exercise <- name `M.lookup` exercised testResults
    occurrences <- templateIdentifier `M.lookup` exercise
    pure occurrences

interfaceInstanceChoiceExercised :: TestResults -> T.Text -> InterfaceInstanceIdentifier -> Maybe (S.Set PackageId)
interfaceInstanceChoiceExercised testResults name interfaceInstanceIdentifier = do
    exercise <- name `M.lookup` exercised testResults
    occurrences <- instanceTemplate interfaceInstanceIdentifier `M.lookup` exercise
    pure occurrences

interfaceInstanceChoices :: TestResults -> InterfaceInstanceIdentifier -> Maybe Choices
interfaceInstanceChoices testResults interfaceInstanceIdentifier =
    instanceInterface interfaceInstanceIdentifier `M.lookup` interfaces testResults

printTestCoverage ::
    Bool
    -> TestResults
    -> IO ()
printTestCoverage showCoverage testResults@TestResults { templates, interfaceInstances, created } =
    let countWhere pred = M.size . M.filter pred
        pctage :: Int -> Int -> Double
        pctage _ 0 = 100
        pctage n d = max 0 $ min 100 $ 100 * fromIntegral n / fromIntegral d

        localTemplates = M.filterWithKey pred templates
          where
            pred templateId _ = isLocalPkgId (package templateId)
        localTemplatesCreated = M.intersection created localTemplates

        (localTemplateChoices, externalTemplateChoices) = M.partition (isLocalPkgId . package) (allTemplateChoices testResults)

        (localImplementations, externalImplementations) = S.partition pred interfaceInstances
          where
            pred ifaceInst = isLocalPkgId (instancePackage ifaceInst)
        getImplementationChoices implementations = M.fromList
            [ ( (choice, implementation)
              , interfaceInstanceChoiceExercised testResults choice implementation
              )
            | implementation <- S.toList implementations
            , Just choices <- pure (interfaceInstanceChoices testResults implementation)
            , choice <- S.toList choices
            ]
        localImplementationChoices = getImplementationChoices localImplementations
        externalImplementationChoices = getImplementationChoices externalImplementations

        externalTemplates = M.filterWithKey (\k _ -> isLocalPkgId (package k)) templates
        externalTemplatesCreated = M.intersection created externalTemplates

        showCoverageReport :: (k -> String) -> String -> M.Map k a -> [String]
        showCoverageReport printer variety names
          | not showCoverage = []
          | otherwise =
            [ printf "  %s: %d" variety (M.size names)
            ] ++ [ "    " ++ printer id | id <- M.keys names ]
    in
    putStrLn $
    unlines $
    concat
    [ [ printf "Modules internal to this package:" ]
    -- Can't have any external tests that exercise internals, as that would
    -- require a circular dependency, so we only report local test results
    , let defined = M.size localTemplates
          created = M.size localTemplatesCreated
          neverCreated = M.difference localTemplates localTemplatesCreated
      in
      [ printf "- Internal templates"
      , printf "  %d defined" defined
      , printf "  %d (%5.1f%%) created" created (pctage created defined)
      ] ++ showCoverageReport printContractIdentifier "internal templates never created" neverCreated
    , let defined = M.size localTemplateChoices
          (exercised, neverExercised) =
              M.partitionWithKey pred localTemplateChoices
            where
              pred choice templateId = isJust (templateChoiceExercised testResults choice templateId)
      in
      [ printf "- Internal template choices"
      , printf "  %d defined" defined
      , printf "  %d (%5.1f%%) exercised" (M.size exercised) (pctage (M.size exercised) defined)
      ] ++ showCoverageReport printChoiceIdentifier "internal template choices never exercised" neverExercised
    , let defined = S.size localImplementations
          (internal, external) = S.partition (isLocalPkgId . package . unInterfaceIdentifier . instanceInterface) localImplementations
      in
      [ printf "- Internal interface implementations"
      , printf "  %d defined" defined
      , printf "    %d internal interfaces" (S.size internal)
      , printf "    %d external interfaces" (S.size external)
      ]
    , let defined = M.size localImplementationChoices
          (exercised, neverExercised) = M.partition isJust localImplementationChoices
      in
      [ printf "- Internal interface choices"
      , printf "  %d defined" defined
      , printf "  %d (%5.1f%%) exercised" (M.size exercised) (pctage (M.size exercised) defined)
      ] ++ showCoverageReport printChoiceIdentifier "internal interface choices never exercised" neverExercised
    , [ printf "Modules external to this package:" ]
    , let defined = M.size externalTemplates
          createdAny = M.size externalTemplatesCreated
          createdInternal = countWhere (any isLocalPkgId) externalTemplatesCreated
          createdExternal = countWhere (any (not . isLocalPkgId)) externalTemplatesCreated
          neverCreated = M.difference externalTemplates externalTemplatesCreated
      in
      [ printf "- External templates"
      , printf "  %d defined" defined
      , printf "  %d (%5.1f%%) created in any tests" createdAny (pctage createdAny defined)
      , printf "  %d (%5.1f%%) created in internal tests" createdInternal (pctage createdInternal defined)
      , printf "  %d (%5.1f%%) created in external tests" createdExternal (pctage createdExternal defined)
      ] ++ showCoverageReport printContractIdentifier "external templates never created" neverCreated
    , let defined = M.size externalTemplateChoices
          (exercisedAny, neverExercised) = M.mapEitherWithKey f externalTemplateChoices
              where
                  f choice templateId =
                      case templateChoiceExercised testResults choice templateId of
                        Nothing -> Right templateId
                        Just locations -> Left locations
          exercisedInternal = countWhere (any isLocalPkgId) exercisedAny
          exercisedExternal = countWhere (any (not . isLocalPkgId)) exercisedAny
      in
      [ printf "- External template choices"
      , printf "  %d defined" defined
      , printf "  %d (%5.1f%%) exercised in any tests" (M.size exercisedAny) (pctage (M.size exercisedAny) defined)
      , printf "  %d (%5.1f%%) exercised in internal tests" exercisedInternal (pctage exercisedInternal defined)
      , printf "  %d (%5.1f%%) exercised in external tests" exercisedExternal (pctage exercisedExternal defined)
      ] ++ showCoverageReport printChoiceIdentifier "external template choices never exercised" neverExercised
    , let defined = S.size $ S.filter (not . isLocalPkgId . instancePackage) interfaceInstances
    -- Here, interface instances can only refer to external templates and
    -- interfaces, so we only report external interface instances
      in
      [ printf "- External interface implementations"
      , printf "  %d defined" defined
      ]
    , let defined = M.size externalImplementationChoices
          (exercisedAny, neverExercised) = M.mapEitherWithKey f externalImplementationChoices
              where
                  f (_, instanceId) locations =
                      case locations of
                        Nothing -> Right instanceId
                        Just locations -> Left locations
          exercisedInternal = countWhere (any isLocalPkgId) exercisedAny
          exercisedExternal = countWhere (any (not . isLocalPkgId)) exercisedAny
      in
      [ printf "- External interface choices"
      , printf "  %d defined" defined
      , printf "  %d (%5.1f%%) exercised in any tests" (M.size exercisedAny) (pctage (M.size exercisedAny) defined)
      , printf "  %d (%5.1f%%) exercised in internal tests" exercisedInternal (pctage exercisedInternal defined)
      , printf "  %d (%5.1f%%) exercised in external tests" exercisedExternal (pctage exercisedExternal defined)
      ] ++ showCoverageReport printChoiceIdentifier "external interface choices never exercised" neverExercised
    ]

    where

        {-
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
        -}

    printContractIdentifier = undefined
    printChoiceIdentifier = undefined

        {-
    printContractIdentifier :: ContractIdentifier -> String
    printContractIdentifier ContractIdentifier { package, qualifiedName } =
        T.unpack $ maybe
            qualifiedName
            (\pId -> pkgIdToPkgName pId <> ":" <> qualifiedName)
            package

    printChoiceIdentifier :: ChoiceIdentifier -> String
    printChoiceIdentifier ChoiceIdentifier { contract, choice } =
        printContractIdentifier contract <> ":" <> T.unpack choice
        -}
