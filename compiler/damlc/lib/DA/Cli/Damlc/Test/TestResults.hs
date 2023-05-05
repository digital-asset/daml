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
import Data.Maybe (mapMaybe, isNothing, maybeToList)
import Data.Either (isLeft)
import Control.Monad (guard)
import Text.Printf (printf)

class Protobuf a b | a -> b, b -> a where
    decode :: a -> Maybe b
    encode :: b -> a

data TestResults = TestResults
    -- What was defined
    { interfaces :: S.Set ContractIdentifier
    , interfaceChoices :: S.Set ChoiceIdentifier
    , templates :: S.Set ContractIdentifier
    , templateChoices :: S.Set ChoiceIdentifier
    -- What was used
    , createdInternally :: S.Set ContractIdentifier
    , createdExternally :: S.Set ContractIdentifier
    , exercisedInternally :: S.Set ChoiceIdentifier
    , exercisedExternally :: S.Set ChoiceIdentifier
    }
    deriving Show

vecToSet :: (Ord b, Protobuf a b) => V.Vector a -> Maybe (S.Set b)
vecToSet = fmap S.fromList . traverse decode . V.toList

setToVec :: Protobuf a b => S.Set b -> V.Vector a
setToVec = V.fromList . map encode . S.toList

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

instance Protobuf TR.PackageId (Maybe T.Text) where
    decode (TR.PackageId (Just (TR.PackageIdVarietyLocal LF1.Unit))) = Just Nothing
    decode (TR.PackageId (Just (TR.PackageIdVarietyExternal t))) = Just (Just (TL.toStrict t))
    decode _ = Nothing
    encode Nothing = TR.PackageId (Just (TR.PackageIdVarietyLocal LF1.Unit))
    encode (Just t) = TR.PackageId (Just (TR.PackageIdVarietyExternal (TL.fromStrict t)))

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

loeGetModules :: LocalOrExternal -> [(LF.Module, a -> LF.Qualified a)]
loeGetModules (Local mod) =
    pure (mod, LF.Qualified LF.PRSelf (LF.moduleName mod))
loeGetModules (External pkg) =
    [ (mod, qualifier)
    | mod <- NM.elems $ LF.packageModules $ LF.extPackagePkg pkg
    , let qualifier = LF.Qualified (LF.PRImport (LF.extPackageId pkg)) (LF.moduleName mod)
    ]

data ContractIdentifier = ContractIdentifier
    { qualifiedName :: T.Text
    , package :: Maybe T.Text -- `package == Nothing` means local package
    }
    deriving (Eq, Ord, Show)

data ChoiceIdentifier = ChoiceIdentifier
    { choice :: T.Text
    , packageContract :: ContractIdentifier
    }
    deriving (Eq, Ord, Show)

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
    Bool
    -> [LocalOrExternal]
    -> [(LocalOrExternal, [(VirtualResource, Either SSC.Error SS.ScenarioResult)])]
    -> IO ()
printTestCoverage showCoverage allPackages results
  | any (isLeft . snd) $ concatMap snd results = pure ()
  | otherwise = printReport
  where
    printReport :: IO ()
    printReport =
        let countWhere pred = M.size . M.filter pred
            pctage :: Int -> Int -> Double
            pctage _ 0 = 100
            pctage n d = max 0 $ min 100 $ 100 * fromIntegral n / fromIntegral d

            allTemplates = templatesDefinedIn allPackages
            localTemplates = M.filterWithKey pred allTemplates
              where
                pred (ContractIdentifier { package = Nothing }) _ = True
                pred _ _ = False
            localTemplatesCreated = M.intersection allCreatedContracts localTemplates

            allTemplateChoices = templateChoicesDefinedIn allPackages
            localTemplateChoices = M.filterWithKey pred allTemplateChoices
              where
                pred (ChoiceIdentifier { packageContract = ContractIdentifier { package = Nothing } }) _ = True
                pred _ _ = False
            localTemplateChoicesExercised = M.intersection allExercisedChoices localTemplateChoices

            allInterfaces = interfacesDefinedIn allPackages
            allImplementations = interfaceImplementationsDefinedIn allPackages
            fillInImplementation (ifaceId, _) (loe, instanceBody) = (loe, instanceBody, M.lookup ifaceId allInterfaces)

            allImplementationChoices = M.fromList $ do
                (k@(_, contractId), (loe, body, mdef)) <- M.toList $ M.mapWithKey fillInImplementation allImplementations
                def <- maybeToList mdef
                choice <- NM.toList $ LF.intChoices $ LF.qualObject def
                let name = LF.unChoiceName $ LF.chcName choice
                guard (name /= "Archive")
                pure (ChoiceIdentifier { choice = name, packageContract = contractId }, (k, loe, body, def, choice))

            localImplementationChoices = M.filter pred allImplementationChoices
              where
                pred (_, loe, _, _, _) = isLocal loe
            localImplementationChoicesExercised = M.intersection allExercisedChoices localImplementationChoices
            externalImplementationChoices = M.filter pred allImplementationChoices
              where
                pred (_, loe, _, _, _) = not (isLocal loe)
            externalImplementationChoicesExercised = M.intersection allExercisedChoices externalImplementationChoices

            externalTemplates = M.filterWithKey pred allTemplates
              where
                pred (ContractIdentifier { package = Just _ }) _ = True
                pred _ _ = False
            externalTemplatesCreated = M.intersection allCreatedContracts externalTemplates

            externalTemplateChoices = M.filterWithKey pred allTemplateChoices
              where
                pred (ChoiceIdentifier { packageContract = ContractIdentifier { package = Just _ } }) _ = True
                pred _ _ = False
            externalTemplateChoicesExercised = M.intersection allExercisedChoices externalTemplateChoices

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
              exercised = M.size localTemplateChoicesExercised
              neverExercised = M.difference localTemplateChoices localTemplateChoicesExercised
          in
          [ printf "- Internal template choices"
          , printf "  %d defined" defined
          , printf "  %d (%5.1f%%) exercised" exercised (pctage exercised defined)
          ] ++ showCoverageReport printChoiceIdentifier "internal template choices never exercised" neverExercised
        , let localImplementations = M.filter (isLocal . fst) allImplementations
              defined = M.size localImplementations
              (internal, external) = M.partitionWithKey (\(ifaceId, _) _ -> isNothing (package ifaceId)) localImplementations
          in
          [ printf "- Internal interface implementations"
          , printf "  %d defined" defined
          , printf "    %d internal interfaces" (M.size internal)
          , printf "    %d external interfaces" (M.size external)
          ]
        , let defined = M.size localImplementationChoices
              exercised = M.size localImplementationChoicesExercised
              neverExercised = M.difference localImplementationChoices localImplementationChoicesExercised
          in
          [ printf "- Internal interface choices"
          , printf "  %d defined" defined
          , printf "  %d (%5.1f%%) exercised" exercised (pctage exercised defined)
          ] ++ showCoverageReport printChoiceIdentifier "internal interface choices never exercised" neverExercised
        , [ printf "Modules external to this package:" ]
        -- Here, interface instances can only refer to external templates and
        -- interfaces, so we only report external interface instances
        , let defined = M.size externalTemplates
              createdAny = M.size externalTemplatesCreated
              createdInternal = countWhere (any isLocal) externalTemplatesCreated
              createdExternal = countWhere (not . all isLocal) externalTemplatesCreated
              neverCreated = M.difference externalTemplates externalTemplatesCreated
          in
          [ printf "- External templates"
          , printf "  %d defined" defined
          , printf "  %d (%5.1f%%) created in any tests" createdAny (pctage createdAny defined)
          , printf "  %d (%5.1f%%) created in internal tests" createdInternal (pctage createdInternal defined)
          , printf "  %d (%5.1f%%) created in external tests" createdExternal (pctage createdExternal defined)
          ] ++ showCoverageReport printContractIdentifier "external templates never created" neverCreated
        , let defined = M.size externalTemplateChoices
              exercisedAny = M.size externalTemplateChoicesExercised
              exercisedInternal = countWhere (any isLocal) externalTemplateChoicesExercised
              exercisedExternal = countWhere (not . all isLocal) externalTemplateChoicesExercised
              neverExercised = M.difference externalTemplateChoices externalTemplateChoicesExercised
          in
          [ printf "- External template choices"
          , printf "  %d defined" defined
          , printf "  %d (%5.1f%%) exercised in any tests" exercisedAny (pctage exercisedAny defined)
          , printf "  %d (%5.1f%%) exercised in internal tests" exercisedInternal (pctage exercisedInternal defined)
          , printf "  %d (%5.1f%%) exercised in external tests" exercisedExternal (pctage exercisedExternal defined)
          ] ++ showCoverageReport printChoiceIdentifier "external template choices never exercised" neverExercised
        , let defined = countWhere (not . isLocal . fst) allImplementations
          in
          [ printf "- External interface implementations"
          , printf "  %d defined" defined
          ]
        , let defined = M.size externalImplementationChoices
              exercisedAny = M.size externalImplementationChoicesExercised
              exercisedInternal = countWhere (any isLocal) externalImplementationChoicesExercised
              exercisedExternal = countWhere (not . all isLocal) externalImplementationChoicesExercised
              neverExercised = M.difference externalImplementationChoices externalImplementationChoicesExercised
          in
          [ printf "- External interface choices"
          , printf "  %d defined" defined
          , printf "  %d (%5.1f%%) exercised in any tests" exercisedAny (pctage exercisedAny defined)
          , printf "  %d (%5.1f%%) exercised in internal tests" exercisedInternal (pctage exercisedInternal defined)
          , printf "  %d (%5.1f%%) exercised in external tests" exercisedExternal (pctage exercisedExternal defined)
          ] ++ showCoverageReport printChoiceIdentifier "external interface choices never exercised" neverExercised
        ]

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

    templateChoicesDefinedIn :: [LocalOrExternal] -> M.Map ChoiceIdentifier (LF.Qualified LF.Template, LF.TemplateChoice)
    templateChoicesDefinedIn localOrExternals = M.fromList
        [ ( ChoiceIdentifier
              { choice = name
              , packageContract = templateIdentifier
              }
          , (templateInfo, choice)
          )
        | (templateIdentifier, templateInfo) <- M.toList $ templatesDefinedIn localOrExternals
        , choice <- NM.toList $ LF.tplChoices $ LF.qualObject templateInfo
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
    allCreatedContracts = M.unionsWith (<>) $ map (fmap (:[]) . uncurry createdContracts) results

    allExercisedChoices :: M.Map ChoiceIdentifier [LocalOrExternal]
    allExercisedChoices = M.unionsWith (<>) $ map (fmap (:[]) . uncurry exercisedChoices) results

    createdContracts :: LocalOrExternal -> [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> M.Map ContractIdentifier LocalOrExternal
    createdContracts loe results =
        M.fromList $
        [ (ssIdentifierToIdentifier identifier, loe)
        | n <- scenarioNodes results
        , Just (SS.NodeNodeCreate SS.Node_Create {SS.node_CreateContractInstance}) <-
              [SS.nodeNode n]
        , Just contractInstance <- [node_CreateContractInstance]
        , Just identifier <- [SS.contractInstanceTemplateId contractInstance]
        ]

    exercisedChoices :: LocalOrExternal -> [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> M.Map ChoiceIdentifier LocalOrExternal
    exercisedChoices loe results =
        M.fromList $
        [ (choiceIdentifier, loe)
        | n <- scenarioNodes results
        , Just (SS.NodeNodeExercise SS.Node_Exercise { SS.node_ExerciseTemplateId
                                                     , SS.node_ExerciseChoiceId
                                                     }) <- [SS.nodeNode n]
        , Just identifier <- [node_ExerciseTemplateId]
        , let choiceIdentifier =
                ChoiceIdentifier
                    { choice = TL.toStrict node_ExerciseChoiceId
                    , packageContract = ssIdentifierToIdentifier identifier
                    }
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

    printContractIdentifier :: ContractIdentifier -> String
    printContractIdentifier ContractIdentifier { package, qualifiedName } =
        T.unpack $ maybe
            qualifiedName
            (\pId -> pkgIdToPkgName pId <> ":" <> qualifiedName)
            package

    printChoiceIdentifier :: ChoiceIdentifier -> String
    printChoiceIdentifier ChoiceIdentifier { packageContract, choice } =
        printContractIdentifier packageContract <> ":" <> T.unpack choice

