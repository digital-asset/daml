-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}

module DA.Cli.Damlc.Test.TestResults (
        module DA.Cli.Damlc.Test.TestResults
    ) where

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
import qualified Com.Daml.Lf2.Archive.DamlLfDev.DamlLf2 as LF2
import qualified Data.Map.Strict as M
import Data.Maybe (mapMaybe)
import Data.Foldable (fold)
import Text.Printf
import Text.Regex.TDFA
import Data.Monoid (Endo(..))

class Protobuf a b | a -> b where
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

instance Monoid TestResults where
    mempty = TestResults mempty mempty mempty mempty mempty

instance Semigroup TestResults where
    (<>) tr1 tr2 =
        TestResults
            { templates = templates `combineUsing` M.unionWith S.union
            , interfaces = interfaces `combineUsing` M.unionWith S.union
            , interfaceInstances = interfaceInstances `combineUsing` S.union
            , created = created `combineUsing` M.unionWith S.union
            , exercised = exercised `combineUsing` M.unionWith (M.unionWith S.union)
            }
      where
        combineUsing field combine = field tr1 `combine` field tr2

vecToSet :: (Ord b, Protobuf a b) => V.Vector a -> Maybe (S.Set b)
vecToSet = fmap S.fromList . traverse decode . V.toList

setToVec :: Protobuf a b => S.Set b -> V.Vector a
setToVec = V.fromList . map encode . S.toList

instance Protobuf TL.Text T.Text where
    decode = Just . TL.toStrict
    encode = TL.fromStrict

instance Protobuf TR.Choices Choices where
    decode (TR.Choices choices) = vecToSet choices
    encode choices = TR.Choices (setToVec choices)

vecToMap :: (Ord k, Protobuf a (k, b)) => V.Vector a -> Maybe (M.Map k b)
vecToMap = fmap M.fromList . traverse decode . V.toList

mapToVec :: (Ord k, Protobuf a (k, b)) => M.Map k b -> V.Vector a
mapToVec = V.fromList . map encode . M.toList

instance Protobuf TR.Template (TemplateIdentifier, Choices) where
    decode (TR.Template mTId mChoices) = (,) <$> (decode =<< mTId) <*> (decode =<< mChoices)
    encode (tid, choices) = TR.Template (Just (encode tid)) (Just (encode choices))

instance Protobuf TR.Interface (InterfaceIdentifier, Choices) where
    decode (TR.Interface mTId mChoices) = (,) <$> (decode =<< mTId) <*> (decode =<< mChoices)
    encode (tid, choices) = TR.Interface (Just (encode tid)) (Just (encode choices))

instance Protobuf TR.TemplateIdentifier TemplateIdentifier where
    decode (TR.TemplateIdentifier qualifiedName mPackageId) =
        TemplateIdentifier <$> decode qualifiedName <*> (decode =<< mPackageId)
    encode (TemplateIdentifier qualifiedName packageId) =
        TR.TemplateIdentifier (encode qualifiedName) (Just (encode packageId))

instance Protobuf TR.InterfaceIdentifier InterfaceIdentifier where
    decode (TR.InterfaceIdentifier qualifiedName mPackageId) =
        InterfaceIdentifier <$> (TemplateIdentifier <$> decode qualifiedName <*> (decode =<< mPackageId))
    encode (InterfaceIdentifier (TemplateIdentifier qualifiedName packageId)) =
        TR.InterfaceIdentifier (encode qualifiedName) (Just (encode packageId))

instance Protobuf TR.InterfaceInstanceIdentifier InterfaceInstanceIdentifier where
    decode (TR.InterfaceInstanceIdentifier mTemplate mInterface mPackageId) =
        InterfaceInstanceIdentifier <$> (decode =<< mTemplate) <*> (decode =<< mInterface) <*> (decode =<< mPackageId)
    encode (InterfaceInstanceIdentifier template interface packageId) =
        TR.InterfaceInstanceIdentifier (Just $ encode template) (Just $ encode interface) (Just $ encode packageId)

instance Protobuf TR.Created (TemplateIdentifier, S.Set PackageId) where
    decode (TR.Created mTemplateId locations) =
        (,) <$> (decode =<< mTemplateId) <*> vecToSet locations
    encode (templateId, locations) =
        TR.Created (Just (encode templateId)) (setToVec locations)

instance Protobuf TR.Exercise (T.Text, M.Map TemplateIdentifier (S.Set PackageId)) where
    decode (TR.Exercise choice exerciseOnTemplates) =
        (,) <$> decode choice <*> vecToMap exerciseOnTemplates
    encode (choice, exerciseOnTemplates) =
        TR.Exercise (encode choice) (mapToVec exerciseOnTemplates)

instance Protobuf TR.ExerciseOnTemplate (TemplateIdentifier, S.Set PackageId) where
    decode (TR.ExerciseOnTemplate mTemplate locations) =
        (,) <$> (decode =<< mTemplate) <*> vecToSet locations
    encode (template, locations) =
        TR.ExerciseOnTemplate (Just (encode template)) (setToVec locations)

instance Protobuf TR.TestResults TestResults where
    decode (TR.TestResults templates interfaces interfaceInstances created exercised) =
        TestResults
            <$> vecToMap templates
            <*> vecToMap interfaces
            <*> vecToSet interfaceInstances
            <*> vecToMap created
            <*> vecToMap exercised
    encode (TestResults templates interfaces interfaceInstances created exercised) =
        TR.TestResults
            (mapToVec templates)
            (mapToVec interfaces)
            (setToVec interfaceInstances)
            (mapToVec created)
            (mapToVec exercised)

instance Protobuf TR.PackageId PackageId where
    decode (TR.PackageId (Just (TR.PackageIdVarietyLocal LF2.Unit))) = Just LocalPackageId
    decode (TR.PackageId (Just (TR.PackageIdVarietyExternal (TR.PackageId_ExternalPackageId id name)))) = Just (ExternalPackageId (TL.toStrict id) (TL.toStrict name))
    decode _ = Nothing
    encode LocalPackageId = TR.PackageId (Just (TR.PackageIdVarietyLocal LF2.Unit))
    encode (ExternalPackageId ext name) = TR.PackageId (Just (TR.PackageIdVarietyExternal (TR.PackageId_ExternalPackageId (TL.fromStrict ext) (TL.fromStrict name))))

saveTestResults :: FilePath -> TestResults -> IO ()
saveTestResults file testResults = do
    let bs = Proto.toLazyByteString ((encode :: TestResults -> TR.TestResults) testResults)
    BSL.writeFile file bs

loadTestResults :: FilePath -> IO (Maybe TestResults)
loadTestResults file = do
    bs <- BS.readFile file
    pure $ case Proto.fromByteString bs of
      Right (result :: TR.TestResults) -> decode result
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
loeToPackageId _ (Local _) = LocalPackageId
loeToPackageId pkgIdToPkgName (External pkg) =
    let pid = LF.unPackageId (LF.extPackageId pkg)
    in
    ExternalPackageId { pid, name = pkgIdToPkgName pid }

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

-- Choices
type Choices = S.Set T.Text

data PackageId
    = LocalPackageId
    | ExternalPackageId
        { pid :: T.Text
        , name :: T.Text
        }
    deriving (Eq, Ord, Show)

isLocalPkgId :: PackageId -> Bool
isLocalPkgId LocalPackageId = True
isLocalPkgId ExternalPackageId {} = False

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
    let pid = do
            pIdSumM <- identifierPackage
            pIdSum <- SS.packageIdentifierSum pIdSumM
            case pIdSum of
                SS.PackageIdentifierSumSelf _ -> Nothing
                SS.PackageIdentifierSumPackageId pId -> Just $ TL.toStrict pId
    in
    case pid of
      Nothing -> LocalPackageId
      Just pid -> ExternalPackageId { pid, name = pkgIdToPkgName pid }

ssIdentifierToIdentifier :: (T.Text -> T.Text) -> SS.Identifier -> TemplateIdentifier
ssIdentifierToIdentifier pkgIdToPkgName identifier@SS.Identifier {SS.identifierName} =
    TemplateIdentifier
        { package = ssIdentifierToPackage pkgIdToPkgName identifier
        , qualifiedName = TL.toStrict identifierName
        }

allTemplateChoices :: TestResults -> M.Map (T.Text, TemplateIdentifier) (S.Set PackageId)
allTemplateChoices testResults = M.fromList
    [ ((choice, templateIdentifier), exerciseOccurence)
    | (templateIdentifier, choices) <- M.toList (templates testResults)
    , choice <- S.toList choices
    , let exerciseOccurence = fold $ do
            exercise <- choice `M.lookup` exercised testResults
            templateIdentifier `M.lookup` exercise
    ]

interfaceInstanceChoices :: TestResults -> InterfaceInstanceIdentifier -> Maybe Choices
interfaceInstanceChoices testResults interfaceInstanceIdentifier =
    instanceInterface interfaceInstanceIdentifier `M.lookup` interfaces testResults

allInterfaceInstanceChoices :: TestResults -> M.Map (T.Text, InterfaceInstanceIdentifier) (S.Set PackageId)
allInterfaceInstanceChoices testResults = M.fromList
    [ ( (choice, implementation)
      , exerciseOccurence
      )
    | implementation <- S.toList (interfaceInstances testResults)
    , Just choices <- pure (interfaceInstanceChoices testResults implementation)
    , choice <- S.toList choices
    , let exerciseOccurence = fold $ do
            exercise <- choice `M.lookup` exercised testResults
            instanceTemplate implementation `M.lookup` exercise
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
        , let pkgId = loeToPackageId pkgIdToPkgName localOrExternal
        , module_ <- loeGetModules localOrExternal
        , template <- NM.toList $ LF.moduleTemplates module_
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
        , let pkgId = loeToPackageId pkgIdToPkgName localOrExternal
        , module_ <- loeGetModules localOrExternal
        , interface <- NM.toList $ LF.moduleInterfaces module_
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
          [matchingPkg] -> LF.unPackageName $ LF.packageName $ LF.packageMetadata $ LF.extPackagePkg matchingPkg
          _ -> error ("pkgIdToPkgName: more than one package matching name " <> T.unpack targetPid)
        where
            isTargetPackage loe
                | External pkg <- loe
                , targetPid == LF.unPackageId (LF.extPackageId pkg)
                = Just pkg
                | otherwise
                = Nothing

printTestCoverageWithFilters :: Bool -> [Regex] -> TestResults -> IO ()
printTestCoverageWithFilters showCoverage filters testResults =
    let filtered = foldMap (Endo . skipMatchingChoices . matchTest) filters `appEndo` testResults
    in
    printTestCoverage showCoverage filtered

printTestCoverage
    :: Bool
    -> TestResults
    -> IO ()
printTestCoverage showCoverage testResults@TestResults { templates, interfaceInstances, created } =
    let -- Utilities
        countWhere :: (a -> Bool) -> M.Map k a -> Int
        countWhere pred = M.size . M.filter pred

        pctage :: Int -> Int -> Double
        pctage _ 0 = 100
        pctage n d = max 0 $ min 100 $ 100 * fromIntegral n / fromIntegral d

        partitionByKey :: (k -> Bool) -> M.Map k a -> (M.Map k a, M.Map k a)
        partitionByKey pred = M.partitionWithKey (\k _ -> pred k)

        -- Generate template creation reports
        (localTemplates, externalTemplates) =
            partitionByKey (isLocalPkgId . package) templates

        localTemplatesReport =
            let defined = M.size localTemplates
                createdAny = M.intersection created localTemplates
                neverCreated = M.difference localTemplates createdAny
            in
            [ printf "- Internal templates"
            , printf "  %d defined" defined
            , printf "  %d (%5.1f%%) created" (M.size createdAny) (pctage (M.size createdAny) defined)
            ] ++ showCoverageReport printTemplateIdentifier "internal templates never created" neverCreated

        externalTemplatesReport =
           let defined = externalTemplates
               createdAny = M.intersection created defined
               createdInternal = countWhere (any isLocalPkgId) createdAny
               createdExternal = countWhere (not . all isLocalPkgId) createdAny
               neverCreated = M.difference defined createdAny
           in
           [ printf "- External templates"
           , printf "  %d defined" (M.size defined)
           , printf "  %d (%5.1f%%) created in any tests" (M.size createdAny) (pctage (M.size createdAny) (M.size defined))
           , printf "  %d (%5.1f%%) created in internal tests" createdInternal (pctage createdInternal (M.size defined))
           , printf "  %d (%5.1f%%) created in external tests" createdExternal (pctage createdExternal (M.size defined))
           ] ++ showCoverageReport printTemplateIdentifier "external templates never created" neverCreated

        -- Generate template choices reports
        (localTemplateChoices, externalTemplateChoices) =
            partitionByKey (isLocalPkgId . package . snd) (allTemplateChoices testResults)

        localTemplateChoicesReport =
            let defined = M.size localTemplateChoices
                (exercised, neverExercised) = M.partition (\locs -> S.size locs > 0) localTemplateChoices
            in
            [ printf "- Internal template choices"
            , printf "  %d defined" defined
            , printf "  %d (%5.1f%%) exercised" (M.size exercised) (pctage (M.size exercised) defined)
            ] ++ showCoverageReport printTemplateChoiceIdentifier "internal template choices never exercised" neverExercised

        externalTemplateChoicesReport =
            let defined = M.size externalTemplateChoices
                (exercisedAny, neverExercised) = M.partition (\locs -> S.size locs > 0) externalTemplateChoices
                exercisedInternal = countWhere (any isLocalPkgId) exercisedAny
                exercisedExternal = countWhere (not . all isLocalPkgId) exercisedAny
            in
            [ printf "- External template choices"
            , printf "  %d defined" defined
            , printf "  %d (%5.1f%%) exercised in any tests" (M.size exercisedAny) (pctage (M.size exercisedAny) defined)
            , printf "  %d (%5.1f%%) exercised in internal tests" exercisedInternal (pctage exercisedInternal defined)
            , printf "  %d (%5.1f%%) exercised in external tests" exercisedExternal (pctage exercisedExternal defined)
            ] ++ showCoverageReport printTemplateChoiceIdentifier "external template choices never exercised" neverExercised

        -- Generate implementation reports
        (localImplementations, externalImplementations) =
            S.partition (isLocalPkgId . instancePackage) interfaceInstances

        localImplementationsReport =
            let defined = S.size localImplementations
                (internal, external) = S.partition (isLocalPkgId . package . unInterfaceIdentifier . instanceInterface) localImplementations
            in
            [ printf "- Internal interface implementations"
            , printf "  %d defined" defined
            , printf "    %d internal interfaces" (S.size internal)
            , printf "    %d external interfaces" (S.size external)
            ]

        externalImplementationsReport =
            let defined = S.size externalImplementations
            -- Here, interface instances can only refer to external templates and
            -- interfaces, so we only report external interface instances
            in
            [ printf "- External interface implementations"
            , printf "  %d defined" defined
            ]

        -- Generate implementation choices reports
        (localImplementationChoices, externalImplementationChoices) =
            partitionByKey (isLocalPkgId . instancePackage . snd) (allInterfaceInstanceChoices testResults)

        localImplementationChoicesReport =
           let defined = M.size localImplementationChoices
               (exercised, neverExercised) = M.partition (\locs -> S.size locs > 0) localImplementationChoices
           in
           [ printf "- Internal interface choices"
           , printf "  %d defined" defined
           , printf "  %d (%5.1f%%) exercised" (M.size exercised) (pctage (M.size exercised) defined)
           ] ++ showCoverageReport printInstanceChoiceIdentifier "internal interface choices never exercised" neverExercised

        externalImplementationChoicesReport =
            let defined = M.size externalImplementationChoices
                (exercisedAny, neverExercised) = M.partition (\locs -> S.size locs > 0) externalImplementationChoices
                exercisedInternal = countWhere (any isLocalPkgId) exercisedAny
                exercisedExternal = countWhere (not . all isLocalPkgId) exercisedAny
            in
            [ printf "- External interface choices"
            , printf "  %d defined" defined
            , printf "  %d (%5.1f%%) exercised in any tests" (M.size exercisedAny) (pctage (M.size exercisedAny) defined)
            , printf "  %d (%5.1f%%) exercised in internal tests" exercisedInternal (pctage exercisedInternal defined)
            , printf "  %d (%5.1f%%) exercised in external tests" exercisedExternal (pctage exercisedExternal defined)
            ] ++ showCoverageReport printInstanceChoiceIdentifier "external interface choices never exercised" neverExercised

        showCoverageReport :: (k -> String) -> String -> M.Map k a -> [String]
        showCoverageReport printer variety names
          | not showCoverage = []
          | otherwise =
            [ printf "  %s: %d" variety (M.size names)
            ] ++ [ "    " ++ printer id | (id, _value) <- M.toList names ]
    in
    putStrLn $
    unlines $
    concat
    [ [ printf "Modules internal to this package:" ]
    -- Can't have any external tests that exercise internals, as that would
    -- require a circular dependency, so we only report local test results
    , localTemplatesReport
    , localTemplateChoicesReport
    , localImplementationsReport
    , localImplementationChoicesReport
    , [ printf "Modules external to this package:" ]
    , externalTemplatesReport
    , externalTemplateChoicesReport
    , externalImplementationsReport
    , externalImplementationChoicesReport
    ]

printTemplateChoiceIdentifier :: (T.Text, TemplateIdentifier) -> String
printTemplateChoiceIdentifier (choice, templateId) =
    printTemplateIdentifier templateId <> ":" <> T.unpack choice

printInstanceChoiceIdentifier :: (T.Text, InterfaceInstanceIdentifier) -> String
printInstanceChoiceIdentifier (choice, InterfaceInstanceIdentifier { instanceTemplate }) =
    printTemplateIdentifier instanceTemplate <> ":" <> T.unpack choice

printTemplateIdentifier :: TemplateIdentifier -> String
printTemplateIdentifier TemplateIdentifier { package, qualifiedName } =
    let prefix =
            case package of
              LocalPackageId -> ""
              ExternalPackageId { name } -> name <> ":"
    in
    T.unpack $ prefix <> qualifiedName

skipMatchingChoices :: (String -> Bool) -> TestResults -> TestResults
skipMatchingChoices namePred TestResults{..} =
  TestResults
    { templates = M.mapWithKey (S.filter . shouldKeep) templates
    , interfaces = M.mapWithKey (S.filter . shouldKeep . unInterfaceIdentifier) interfaces
    , interfaceInstances
    , created
    , exercised = M.mapWithKey (\choiceName -> M.filterWithKey (\tid _ -> shouldKeep tid choiceName)) exercised
    }
  where
    shouldKeep :: TemplateIdentifier -> T.Text -> Bool
    shouldKeep tid choiceName = not (namePred (printTemplateChoiceIdentifier (choiceName, tid)))
