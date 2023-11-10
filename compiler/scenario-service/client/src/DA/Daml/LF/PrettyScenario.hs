-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE PatternSynonyms #-}

-- | Pretty-printing of scenario results
module DA.Daml.LF.PrettyScenario
  ( activeContractsFromScenarioResult
  , prettyScenarioResult
  , prettyScenarioError
  , prettyBriefScenarioError
  , prettyWarningMessage
  , renderScenarioResult
  , renderScenarioError
  , renderTableView
  , renderTransactionView
  , lookupDefLocation
  , lookupLocationModule
  , scenarioNotInFileNote
  , fileWScenarioNoLongerCompilesNote
  , isActive
  , ModuleRef
  , PrettyLevel
  -- Exposed for testing
  , ptxExerciseContext
  , ExerciseContext(..)
  ) where

import           Control.Monad.Extra
import           Control.Monad.Reader
import           Control.Monad.Trans.Except
import DA.Daml.LF.Ast qualified             as LF
import DA.Daml.LF.Ast.Pretty qualified      as LF
import DA.Daml.LF.Mangling
import Control.Applicative
import Text.Read hiding (parens)
import           DA.Pretty as Pretty
import           Data.Either.Extra
import           Data.Int
import Data.List
import Data.List.Extra (unsnoc)
import Data.Maybe
import Data.NameMap qualified               as NM
import Data.Map.Strict qualified            as MS
import Data.Ratio qualified                 as Ratio
import Data.Set qualified                   as S
import Data.Text qualified                  as T
import Data.Text.Extended qualified         as TE
import Data.Text.Lazy qualified             as TL
import Data.Time.Clock.POSIX qualified      as CP
import Data.Time.Format qualified           as TF
import Data.Vector qualified                as V
import Network.URI.Encode qualified
import           ScenarioService
import Text.Blaze.Html5 qualified as H
import Text.Blaze.Html5.Attributes qualified as A
import Text.Blaze.Html.Renderer.Text qualified as Blaze

data Error = ErrorMissingNode NodeId
type M = ExceptT Error (Reader (MS.Map NodeId Node, LF.World))

type ModuleRef = LF.Qualified ()

unmangleQualifiedName :: T.Text -> (LF.ModuleName, T.Text)
unmangleQualifiedName t = case T.splitOn ":" t of
    [modName, defName] -> (unmangleModuleName modName, unmangleDotted defName)
    _ -> error "Bad definition"

unmangleModuleName :: T.Text -> LF.ModuleName
unmangleModuleName t = LF.ModuleName (map (unwrapUnmangle . unmangleIdentifier) $ T.splitOn "." t)

-- | Partial helper to handle the result
-- of `unmangleIdentifier` by crashing if it failed.
unwrapUnmangle :: Either String UnmangledIdentifier -> T.Text
unwrapUnmangle (Left err) = error err
unwrapUnmangle (Right (UnmangledIdentifier s)) = s

unmangleDotted :: T.Text -> T.Text
unmangleDotted s = unwrapUnmangle unmangled
  where unmangled =
              fmap (UnmangledIdentifier . T.intercalate "." . map getUnmangledIdentifier) $
              traverse unmangleIdentifier $
              T.splitOn "." s

-- This assumes the name is dotted which is the case for all type
-- constructors which is the only thing we use it for.
{-# COMPLETE UnmangledQualifiedName #-}
pattern UnmangledQualifiedName :: LF.ModuleName -> T.Text -> TL.Text
pattern UnmangledQualifiedName mod def <-
    (unmangleQualifiedName . TL.toStrict -> (mod, def))

runM :: V.Vector Node -> LF.World -> M (Doc SyntaxClass) -> Doc SyntaxClass
runM nodes world =
  ppError . flip runReader (nodeMap, world) . runExceptT
  where
    nodeMap = MS.fromList
        [ (nodeId, node)
        | node <- V.toList nodes
        , Just nodeId <- [nodeNodeId node]
        ]
    ppError :: Either Error (Doc SyntaxClass) -> Doc SyntaxClass
    ppError (Right x) = x
    ppError (Left (ErrorMissingNode nodeId)) =
      angledList [ text "missing node",  prettyNodeId nodeId ]

lookupNode :: NodeId -> M Node
lookupNode nodeId = asks (MS.lookup nodeId . fst) >>= \case
  Nothing -> throwE (ErrorMissingNode nodeId)
  Just node -> pure node

askWorld :: M LF.World
askWorld = asks snd

lookupDefLocation :: LF.Module -> T.Text -> Maybe LF.SourceLoc
lookupDefLocation mod0 defName =
  join $
    LF.dvalLocation <$> NM.lookup (LF.ExprValName defName) (LF.moduleValues mod0)
    <|>
    LF.tplLocation <$> NM.lookup (LF.TypeConName [defName]) (LF.moduleTemplates mod0)

lookupModule :: LF.World -> Maybe PackageIdentifier -> LF.ModuleName -> Maybe LF.Module
lookupModule world mbPkgId modName = do
  let pkgRef = case mbPkgId of
       Just (PackageIdentifier (Just (PackageIdentifierSumPackageId pkgId))) ->
         LF.PRImport $ LF.PackageId $ TL.toStrict pkgId
       _ -> LF.PRSelf
  eitherToMaybe (LF.lookupModule (LF.Qualified pkgRef modName ()) world)

lookupLocationModule :: LF.World -> Location -> Maybe LF.Module
lookupLocationModule world Location{..} =
    lookupModule world locationPackage $
        unmangleModuleName (TL.toStrict locationModule)

parseNodeId :: NodeId -> [Integer]
parseNodeId =
    fmap (fromMaybe 0 . readMaybe . dropHash . TL.unpack)
  . TL.splitOn ":"
  . nodeIdId
  where
    dropHash s = fromMaybe s $ stripPrefix "#" s

activeContractsFromScenarioResult :: ScenarioResult -> S.Set TL.Text
activeContractsFromScenarioResult result =
    S.fromList (V.toList (scenarioResultActiveContracts result))

activeContractsFromScenarioError :: ScenarioError -> S.Set TL.Text
activeContractsFromScenarioError err =
    S.fromList (V.toList (scenarioErrorActiveContracts err))

prettyScenarioResult
  :: PrettyLevel -> LF.World -> S.Set TL.Text -> ScenarioResult -> Doc SyntaxClass
prettyScenarioResult lvl world activeContracts (ScenarioResult steps nodes retValue _finaltime traceLog warnings _) =
  let ppSteps = runM nodes world (vsep <$> mapM (prettyScenarioStep lvl) (V.toList steps))
      sortNodeIds = sortOn parseNodeId
      ppActive =
          fcommasep
        $ map prettyNodeIdLink
        $ sortNodeIds $ mapMaybe nodeNodeId
        $ filter (isActive activeContracts) (V.toList nodes)

      ppTrace = vcat $ map prettyTraceMessage (V.toList traceLog)
      ppWarnings = vcat $ map prettyWarningMessage (V.toList warnings)
  in vsep $ concat
    [ [label_ "Transactions: " ppSteps]
    , [label_ "Active contracts: " ppActive]
    , [label_ "Return value:" (prettyValue' lvl True 0 world v) | Just v <- [retValue]]
    , [text "Trace: " $$ nest 2 ppTrace | not (V.null traceLog)]
    , [text "Warnings: " $$ nest 2 ppWarnings | not (V.null warnings)]
    ]

prettyBriefScenarioError
  :: PrettyLevel -> LF.World -> ScenarioError -> Doc SyntaxClass
prettyBriefScenarioError lvl world ScenarioError{..} = runM scenarioErrorNodes world $ do
  ppError <- prettyScenarioErrorError lvl scenarioErrorError
  pure $
    annotateSC ErrorSC
      (text "Script execution" <->
        (if isNothing scenarioErrorCommitLoc
         then "failed:"
         else "failed on commit at"
           <-> prettyMayLocation world scenarioErrorCommitLoc <> char ':')
      )
    $$ nest 2 ppError

prettyScenarioError
  :: PrettyLevel -> LF.World -> ScenarioError -> Doc SyntaxClass
prettyScenarioError lvl world ScenarioError{..} = runM scenarioErrorNodes world $ do
  ppError <- prettyScenarioErrorError lvl scenarioErrorError
  ppSteps <- vsep <$> mapM (prettyScenarioStep lvl) (V.toList scenarioErrorScenarioSteps)
  ppPtx <- forM scenarioErrorPartialTransaction $ \ptx -> do
      p <- prettyPartialTransaction lvl ptx
      pure $ text "Partial transaction:" $$ nest 2 p
  let ppTrace = vcat $ map prettyTraceMessage (V.toList scenarioErrorTraceLog)
      ppWarnings = vcat $ map prettyWarningMessage (V.toList scenarioErrorWarnings)
      ppStackTraceEntry loc =
         "-" <-> ltext (locationDefinition loc) <-> parens (prettyLocation world loc)
  pure $
    vsep $ catMaybes
    [ Just $ error_ (text "Script execution" <->
      (if isNothing scenarioErrorCommitLoc
       then "failed:"
       else "failed on commit at"
         <-> prettyMayLocation world scenarioErrorCommitLoc <> char ':'))
      $$ nest 2 ppError

    , if V.null scenarioErrorStackTrace
      then Nothing
      else Just $ vcat $ "Stack trace:" : map ppStackTraceEntry (reverse $ V.toList scenarioErrorStackTrace)

    , Just $ "Ledger time:" <-> prettyTimestamp scenarioErrorLedgerTime

    , ppPtx

    , if V.null scenarioErrorScenarioSteps
      then Nothing
      else Just $ text "Committed transactions: " $$ nest 2 ppSteps

    , if V.null scenarioErrorTraceLog
      then Nothing
      else Just $ text "Trace: " $$ nest 2 ppTrace

    , if V.null scenarioErrorWarnings
      then Nothing
      else Just $ text "Warnings: " $$ nest 2 ppWarnings
    ]

prettyTraceMessage :: TraceMessage -> Doc SyntaxClass
prettyTraceMessage msg =
  ltext (traceMessageMessage msg)

prettyWarningMessage :: WarningMessage -> Doc SyntaxClass
prettyWarningMessage msg =
  ltext (warningMessageMessage msg)

data ExerciseContext = ExerciseContext
  { targetId :: Maybe ContractRef
  , choiceId :: TL.Text
  , exerciseLocation :: Maybe Location
  , chosenValue :: Maybe Value
  , exerciseKey :: Maybe KeyWithMaintainers
  } deriving (Eq, Show)

ptxExerciseContext :: PartialTransaction -> Maybe ExerciseContext
ptxExerciseContext PartialTransaction{..} = go Nothing partialTransactionRoots
  where go :: Maybe ExerciseContext -> V.Vector NodeId -> Maybe ExerciseContext
        go acc children
            | V.null children = acc
            | otherwise = do
                  n <- nodeNode =<< MS.lookup (V.last children) nodeMap
                  case n of
                    NodeNodeCreate _ -> acc
                    NodeNodeFetch _ -> acc
                    NodeNodeLookupByKey _ -> acc
                    NodeNodeExercise Node_Exercise{..}
                      | Nothing <- node_ExerciseExerciseResult ->
                        let ctx = ExerciseContext
                              { targetId = Just ContractRef
                                  { contractRefContractId = node_ExerciseTargetContractId
                                  , contractRefTemplateId = node_ExerciseTemplateId
                                  }
                              , choiceId = node_ExerciseChoiceId
                              , exerciseLocation = Nothing
                              , chosenValue = node_ExerciseChosenValue
                              , exerciseKey = node_ExerciseExerciseByKey
                              }
                        in go (Just ctx) node_ExerciseChildren
                      | otherwise -> acc
                    NodeNodeRollback _ ->
                        -- do not decend in rollback. If we aborted within a try, this will not produce
                        -- a rollback node.
                        acc
        nodeMap = MS.fromList [ (nodeId, node) | node <- V.toList partialTransactionNodes, Just nodeId <- [nodeNodeId node] ]

prettyScenarioErrorError :: PrettyLevel -> Maybe ScenarioErrorError -> M (Doc SyntaxClass)
prettyScenarioErrorError _ Nothing = pure $ text "<missing error details>"
prettyScenarioErrorError lvl (Just err) =  do
  world <- askWorld
  case err of
    ScenarioErrorErrorCrash reason -> pure $ text "CRASH:" <-> ltext reason
    ScenarioErrorErrorUserError reason -> pure $ text "Aborted: " <-> ltext reason
    ScenarioErrorErrorUnhandledException exc -> pure $ text "Unhandled exception: " <-> prettyValue' lvl True 0 world exc
    ScenarioErrorErrorTemplatePrecondViolated ScenarioError_TemplatePreconditionViolated{..} -> do
      pure $
        "Template precondition violated in:"
          $$ nest 2
          (   "create"
          <-> prettyMay "<missing template id>" (prettyDefName lvl world) scenarioError_TemplatePreconditionViolatedTemplateId
          $$ (   keyword_ "with"
              $$ nest 2 (prettyMay "<missing argument>" (prettyValue' lvl False 0 world) scenarioError_TemplatePreconditionViolatedArg)
             )
          )
    ScenarioErrorErrorUpdateLocalContractNotActive ScenarioError_ContractNotActive{..} ->
      pure $ vcat
        [ "Attempt to exercise a contract that was consumed in same transaction."
        , "Contract:"
            <-> prettyMay "<missing contract>"
                  (prettyContractRef lvl world)
                  scenarioError_ContractNotActiveContractRef
        ]
    ScenarioErrorErrorDisclosedContractKeyHashingError(ScenarioError_DisclosedContractKeyHashingError contractId key computedHash declaredHash) ->
      pure $ vcat
        [ "Mismatched disclosed contract key hash for contract"
        , label_ "Contract:" $ prettyMay "<missing contract>" (prettyContractRef lvl world) contractId
        , label_ "key:" $ prettyMay "<missing key>" (prettyValue' lvl False 0 world) key
        , label_ "computed hash:" $ ltext computedHash
        , label_ "declared hash:" $ ltext declaredHash
        ]
    ScenarioErrorErrorCreateEmptyContractKeyMaintainers ScenarioError_CreateEmptyContractKeyMaintainers{..} ->
      pure $ vcat
        [ "Attempt to create a contract key with an empty set of maintainers:"
        , nest 2
          (   "create"
          <-> prettyMay "<missing template id>" (prettyDefName lvl world) scenarioError_CreateEmptyContractKeyMaintainersTemplateId
          $$ (   keyword_ "with"
              $$ nest 2 (prettyMay "<missing argument>" (prettyValue' lvl False 0 world) scenarioError_CreateEmptyContractKeyMaintainersArg)
             )
          )
        , label_ "Key: "
          $ prettyMay "<missing key>"
              (prettyValue' lvl False 0 world)
              scenarioError_CreateEmptyContractKeyMaintainersKey
        ]
    ScenarioErrorErrorFetchEmptyContractKeyMaintainers ScenarioError_FetchEmptyContractKeyMaintainers{..} ->
      pure $ vcat
        [ "Attempt to fetch, lookup or exercise a contract key with an empty set of maintainers"
        , label_ "Template:"
            $ prettyMay "<missing template id>"
                (prettyDefName lvl world)
                scenarioError_FetchEmptyContractKeyMaintainersTemplateId
        , label_ "Key: "
            $ prettyMay "<missing key>"
                (prettyValue' lvl False 0 world)
                scenarioError_FetchEmptyContractKeyMaintainersKey
        ]
    ScenarioErrorErrorScenarioContractNotActive ScenarioError_ContractNotActive{..} -> do
      pure $ vcat
        [ "Attempt to exercise a consumed contract"
            <-> prettyMay "<missing contract>"
                  (prettyContractRef lvl world)
                  scenarioError_ContractNotActiveContractRef
        , "Consumed by:"
            <-> prettyMay "<missing node id>" prettyNodeIdLink scenarioError_ContractNotActiveConsumedBy
        ]
    ScenarioErrorErrorScenarioCommitError (CommitError Nothing) -> do
      pure "Unknown commit error"

    ScenarioErrorErrorScenarioCommitError (CommitError (Just (CommitErrorSumFailedAuthorizations fas))) -> do
      pure $ vcat $ mapV (prettyFailedAuthorization lvl world) (failedAuthorizationsFailedAuthorizations fas)

    ScenarioErrorErrorScenarioCommitError (CommitError (Just (CommitErrorSumUniqueContractKeyViolation gk))) -> do
      pure $ vcat
        [ "Commit error due to unique key violation for key"
        , nest 2 (prettyGlobalKey lvl world gk)
        ]

    ScenarioErrorErrorScenarioCommitError (CommitError (Just (CommitErrorSumInconsistentContractKey gk))) -> do
      pure $ vcat
        [ "Commit error due to inconsistent key"
        , nest 2 (prettyGlobalKey lvl world gk)
        ]

    ScenarioErrorErrorUnknownContext ctxId ->
      pure $ "Unknown script interpretation context:" <-> string (show ctxId)
    ScenarioErrorErrorUnknownScenario name ->
      pure $ "Unknown script:" <-> prettyDefName lvl world name
    ScenarioErrorErrorScenarioContractNotEffective ScenarioError_ContractNotEffective{..} ->
      pure $ vcat
        [ "Attempt to fetch or exercise a contract not yet effective."
        , "Contract:"
        <-> prettyMay "<missing contract>" (prettyContractRef lvl world)
              scenarioError_ContractNotEffectiveContractRef
        , "Effective at:"
        <-> prettyTimestamp scenarioError_ContractNotEffectiveEffectiveAt
        ]

    ScenarioErrorErrorScenarioMustfailSucceeded _ ->
      pure "A must-fail commit succeeded."
    ScenarioErrorErrorScenarioInvalidPartyName name ->
      pure $ "Invalid party name:" <-> ltext name
    ScenarioErrorErrorScenarioPartyAlreadyExists name ->
      pure $ "Tried to allocate a party that already exists:" <-> ltext name

    ScenarioErrorErrorScenarioContractNotVisible ScenarioError_ContractNotVisible{..} ->
      pure $ vcat
        [ "Attempt to fetch or exercise a contract not visible to the reading parties."
        , label_ "Contract: "
            $ prettyMay "<missing contract>"
                (prettyContractRef lvl world)
                scenarioError_ContractNotVisibleContractRef
        , label_ "actAs:" $ prettyParties scenarioError_ContractNotVisibleActAs
        , label_ "readAs:" $ prettyParties scenarioError_ContractNotVisibleReadAs
        , label_ "Disclosed to:"
            $ prettyParties scenarioError_ContractNotVisibleObservers
        ]
    ScenarioErrorErrorScenarioContractKeyNotVisible ScenarioError_ContractKeyNotVisible{..} ->
      pure $ vcat
        [ "Attempt to fetch, lookup or exercise a key associated with a contract not visible to the committer."
        , label_ "Contract: "
            $ prettyMay "<missing contract>"
                (prettyContractRef lvl world)
                scenarioError_ContractKeyNotVisibleContractRef
        , label_ "Key: "
            $ prettyMay "<missing key>"
                (prettyValue' lvl False 0 world)
                scenarioError_ContractKeyNotVisibleKey
        , label_ "actAs:" $ prettyParties scenarioError_ContractKeyNotVisibleActAs
        , label_ "readAs:" $ prettyParties scenarioError_ContractKeyNotVisibleReadAs
        , label_ "Stakeholders:"
            $ prettyParties scenarioError_ContractKeyNotVisibleStakeholders
        ]
    ScenarioErrorErrorScenarioContractKeyNotFound ScenarioError_ContractKeyNotFound{..} ->
      pure $ vcat
        [ "Attempt to fetch or exercise by key but no contract with that key was found."
        , label_ "Key: "
          $ prettyMay "<missing key>"
              (prettyGlobalKey lvl world)
              scenarioError_ContractKeyNotFoundKey
        ]
    ScenarioErrorErrorWronglyTypedContract ScenarioError_WronglyTypedContract{..} ->
      pure $ vcat
        [ "Attempt to fetch or exercise a wrongly typed contract."
        , label_ "Contract: "
            $ prettyMay "<missing contract>"
                (prettyContractRef lvl world)
                scenarioError_WronglyTypedContractContractRef
        , label_ "Expected type: "
            $ prettyMay "<missing template id>" (prettyDefName lvl world) scenarioError_WronglyTypedContractExpected
        ]
    ScenarioErrorErrorWronglyTypedContractSoft ScenarioError_WronglyTypedContractSoft{..} ->
      pure $ vcat
        [ "Attempt to fetch or exercise a wrongly typed contract."
        , label_ "Contract: "
            $ prettyMay "<missing contract>"
                (prettyContractRef lvl world)
                scenarioError_WronglyTypedContractSoftContractRef
        , label_ "Expected type: "
            $ prettyMay "<missing template id>" (prettyDefName lvl world) scenarioError_WronglyTypedContractSoftExpected
        , label_ "Accepted types (ancestors): "
            $ vcat $ mapV (prettyDefName lvl world) scenarioError_WronglyTypedContractSoftAccepted
        ]
    ScenarioErrorErrorContractIdInContractKey ScenarioError_ContractIdInContractKey{..} ->
      pure $ "Contract IDs are not supported in contract key:" <->
        prettyMay "<missing contract key>"
          (prettyValue' lvl False 0 world)
          scenarioError_ContractIdInContractKeyKey
    ScenarioErrorErrorComparableValueError _ ->
      pure "Attend to compare incomparable values"
    ScenarioErrorErrorValueExceedsMaxNesting _ ->
          pure "Value exceeds maximum nesting value of 100"
    ScenarioErrorErrorScenarioPartiesNotAllocated ScenarioError_PartiesNotAllocated{..} ->
      pure $ vcat
        [ "Tried to submit a command for parties that have not ben allocated:"
        , prettyParties scenarioError_PartiesNotAllocatedParties
        ]
    ScenarioErrorErrorChoiceGuardFailed ScenarioError_ChoiceGuardFailed {..} ->
      pure $ vcat
        [ "Attempt to exercise a choice with a failing guard"
        , label_ "Contract: " $
            prettyMay "<missing contract>"
              (prettyContractRef lvl world)
              scenarioError_ChoiceGuardFailedContractRef
        , label_ "Choice: " $
            prettyChoiceId world
              (contractRefTemplateId =<< scenarioError_ChoiceGuardFailedContractRef)
              scenarioError_ChoiceGuardFailedChoiceId
        , maybe
            mempty
            (\iid -> label_ "Interface: " $ prettyDefName lvl world iid)
            scenarioError_ChoiceGuardFailedByInterface
        ]
    ScenarioErrorErrorContractDoesNotImplementInterface ScenarioError_ContractDoesNotImplementInterface {..} ->
      pure $ vcat
        [ "Attempt to use a contract via an interface that the contract does not implement"
        , label_ "Contract: " $
            prettyMay "<missing contract>"
              (prettyContractRef lvl world)
              scenarioError_ContractDoesNotImplementInterfaceContractRef
        , label_ "Interface: " $
            prettyMay "<missing interface>"
              (prettyDefName lvl world)
              scenarioError_ContractDoesNotImplementInterfaceInterfaceId
        ]
    ScenarioErrorErrorContractDoesNotImplementRequiringInterface ScenarioError_ContractDoesNotImplementRequiringInterface {..} ->
      pure $ vcat
        [ "Attempt to use a contract via a required interface, but the contract does not implement the requiring interface"
        , label_ "Contract: " $
            prettyMay "<missing contract>"
              (prettyContractRef lvl world)
              scenarioError_ContractDoesNotImplementRequiringInterfaceContractRef
        , label_ "Required interface: " $
            prettyMay "<missing interface>"
              (prettyDefName lvl world)
              scenarioError_ContractDoesNotImplementRequiringInterfaceRequiredInterfaceId
        , label_ "Requiring interface: " $
            prettyMay "<missing interface>"
              (prettyDefName lvl world)
              scenarioError_ContractDoesNotImplementRequiringInterfaceRequiringInterfaceId
        ]
    ScenarioErrorErrorEvaluationTimeout timeout ->
      pure $ text $ T.pack $ "Evaluation timed out after " <> show timeout <> " seconds"
    ScenarioErrorErrorCancelledByRequest _ ->
      pure $ text $ T.pack "Evaluation was cancelled because the test was changed and rerun in a new thread."

    ScenarioErrorErrorLookupError ScenarioError_LookupError {..} -> do
      let
        errMsg =
          case scenarioError_LookupErrorError of
            Just (ScenarioError_LookupErrorErrorNotFound ScenarioError_LookupError_NotFound {..}) ->
              "Failed to find " <> scenarioError_LookupError_NotFoundNotFound <>
                if scenarioError_LookupError_NotFoundNotFound == scenarioError_LookupError_NotFoundContext
                  then ""
                  else " when looking for " <> scenarioError_LookupError_NotFoundContext
            Just (ScenarioError_LookupErrorErrorAmbiguousInterfaceInstance ScenarioError_LookupError_AmbiguousInterfaceInstance {..}) ->
              "Multiple possible instances of " <> scenarioError_LookupError_AmbiguousInterfaceInstanceInstance <>
                if scenarioError_LookupError_AmbiguousInterfaceInstanceInstance == scenarioError_LookupError_AmbiguousInterfaceInstanceContext
                  then ""
                  else " in the context of " <> scenarioError_LookupError_AmbiguousInterfaceInstanceContext
            Nothing -> "Unknown Lookup error"
      pure $ vcat
        [ text $ TL.toStrict errMsg
        , label_ "Package name:" $
            prettyMay "<missing package name>"
              prettyPackageMetadata
              scenarioError_LookupErrorPackageMetadata
        , label_ "Package id:" $ text $ TL.toStrict scenarioError_LookupErrorPackageId
        ]
    ScenarioErrorErrorUpgradeError ScenarioError_UpgradeError {..} -> do
       pure $ text $ TL.toStrict scenarioError_UpgradeErrorMessage
partyDifference :: V.Vector Party -> V.Vector Party -> Doc SyntaxClass
partyDifference with without =
  fcommasep $ map prettyParty $ S.toList $
  S.fromList (V.toList with) `S.difference`
  S.fromList (V.toList without)

prettyParties :: V.Vector Party -> Doc SyntaxClass
prettyParties = fcommasep . mapV prettyParty

prettyFailedAuthorization :: PrettyLevel -> LF.World -> FailedAuthorization -> Doc SyntaxClass
prettyFailedAuthorization lvl world (FailedAuthorization mbNodeId mbFa) =
  hcat
    [ prettyMay "<missing node id>" prettyNodeIdLink mbNodeId
    , text ": "
    , vcat $ case mbFa of
        Just (FailedAuthorizationSumCreateMissingAuthorization
          (FailedAuthorization_CreateMissingAuthorization templateId mbLoc authParties reqParties)) ->
              [ "create of" <-> prettyMay "<missing template id>" (prettyDefName lvl world) templateId
                <-> "at" <-> prettyMayLocation world mbLoc
              , "failed due to a missing authorization from"
                <-> reqParties `partyDifference` authParties
              ]

        Just (FailedAuthorizationSumMaintainersNotSubsetOfSignatories
          (FailedAuthorization_MaintainersNotSubsetOfSignatories templateId mbLoc signatories maintainers)) ->
              [ "create of" <-> prettyMay "<missing template id>" (prettyDefName lvl world) templateId
                <-> "at" <-> prettyMayLocation world mbLoc
              , "failed due to that some parties are maintainers but not signatories: "
                <-> maintainers `partyDifference` signatories
              ]

        Just (FailedAuthorizationSumFetchMissingAuthorization
          (FailedAuthorization_FetchMissingAuthorization templateId mbLoc authParties stakeholders)) ->
              [ "fetch of" <-> prettyMay "<missing template id>" (prettyDefName lvl world) templateId
                <-> "at" <-> prettyMayLocation world mbLoc
              , "failed since none of the stakeholders"
                <-> prettyParties stakeholders
              , "is in the authorizing set"
                <-> prettyParties authParties
              ]

        Just (FailedAuthorizationSumExerciseMissingAuthorization
          (FailedAuthorization_ExerciseMissingAuthorization templateId choiceId mbLoc authParties reqParties)) ->
              [ "exercise of" <-> prettyChoiceId world templateId choiceId
                <-> "in" <-> prettyMay "<missing template id>" (prettyDefName lvl world) templateId
                <-> "at" <-> prettyMayLocation world mbLoc
              , "failed due to a missing authorization from"
                <-> reqParties `partyDifference` authParties
              ]

        Just (FailedAuthorizationSumNoControllers
          (FailedAuthorization_NoControllers templateId choiceId mbLoc)) ->
              [ "exercise of" <-> prettyChoiceId world templateId choiceId
                <-> "in" <-> prettyMay "<missing template id>" (prettyDefName lvl world) templateId
                <-> "at" <-> prettyMayLocation world mbLoc
              , "failed due missing controllers"
              ]

        Just (FailedAuthorizationSumNoSignatories
          (FailedAuthorization_NoSignatories templateId mbLoc)) ->
              [ "create of"
                <-> prettyMay "<missing template id>" (prettyDefName lvl world) templateId
                <-> "at" <-> prettyMayLocation world mbLoc
              , "failed due missing signatories"
              ]

        Just (FailedAuthorizationSumLookupByKeyMissingAuthorization
          (FailedAuthorization_LookupByKeyMissingAuthorization templateId mbLoc authParties maintainers)) ->
              [ "lookup by key of" <-> prettyMay "<missing template id>" (prettyDefName lvl world) templateId
                <-> "at" <-> prettyMayLocation world mbLoc
              , "failed due to a missing authorization from"
                <-> maintainers `partyDifference` authParties
              ]

        Nothing -> [text "<missing failed_authorization>"]
    ]


prettyScenarioStep :: PrettyLevel -> ScenarioStep -> M (Doc SyntaxClass)
prettyScenarioStep _ (ScenarioStep _stepId Nothing) =
  pure $ text "<missing script step>"
prettyScenarioStep lvl (ScenarioStep stepId (Just step)) = do
  world <- askWorld
  case step of
    ScenarioStepStepCommit (ScenarioStep_Commit txId (Just tx) mbLoc) ->
      prettyCommit lvl txId mbLoc tx

    ScenarioStepStepAssertMustFail (ScenarioStep_AssertMustFail actAs readAs time txId mbLoc) ->
      pure
          $ idSC ("n" <> TE.show txId) (keyword_ "TX")
        <-> prettyTxId txId
        <-> prettyTimestamp time
         $$ (nest 3
             $   keyword_ "mustFailAt"
             <-> label_ "actAs:" (braces $ prettyParties actAs)
             <-> label_ "readAs:" (braces $ prettyParties readAs)
             <-> parens (prettyMayLocation world mbLoc)
            )

    ScenarioStepStepPassTime dtMicros ->
      pure
          $ idSC ("n" <> TE.show stepId) (keyword_ "pass")
          <-> prettyTxId stepId
          <-> text (relTimeToText dtMicros)

    bad ->
      pure $ text "INVALID STEP:" <-> string (show bad)
  where
    relTimeToText :: Int64 -> T.Text
    relTimeToText micros
      | micros == 0 = "0s"
      | n <= 6 = fixup $ '0' : '.' : trim (replicate (6 - n) '0' ++ dt)
      | otherwise = fixup $
          let (prefix, trim -> suffix) = splitAt (n - 6) dt
          in if null suffix then prefix else prefix ++ '.' : suffix
      where
        trim = dropWhileEnd ('0' ==)
        fixup cs
          | micros < 0 = T.pack ('-' : cs) <> "s"
          | otherwise = T.pack cs <> "s"
        dt = show (abs micros)
        n  = length dt

prettyTimestamp :: Int64 -> Doc SyntaxClass
prettyTimestamp = prettyUtcTime . toUtcTime . fromIntegral
  where
    prettyUtcTime =
      string . TF.formatTime TF.defaultTimeLocale "%FT%T%QZ"
    toUtcTime t =
        CP.posixSecondsToUTCTime
      $ fromRational $ t Ratio.% (10 ^ (6 :: Integer))

prettyCommit :: PrettyLevel -> Int32 -> Maybe Location -> Transaction -> M (Doc SyntaxClass)
prettyCommit lvl txid mbLoc Transaction{..} = do
  world <- askWorld
  children <- vsep <$> mapM (lookupNode >=> prettyNode lvl) (V.toList transactionRoots)
  return
      $ idSC ("n" <> TE.show txid) (keyword_ "TX")
    <-> prettyTxId txid
    <-> prettyTimestamp transactionEffectiveAt
    <-> parens (prettyMayLocation world mbLoc)
     $$ children

prettyMayLocation :: LF.World -> Maybe Location -> Doc SyntaxClass
prettyMayLocation world = maybe (text "unknown source") (prettyLocation world)

prettyLocation :: LF.World -> Location -> Doc SyntaxClass
prettyLocation world (Location mbPkgId (unmangleModuleName . TL.toStrict -> modName) sline scol eline _ecol _definition) =
      maybe id (\path -> linkSC (url path) title)
        (lookupModule world mbPkgId modName >>= LF.moduleSource)
    $ text title
  where
    modName' = LF.moduleNameString modName
    encodeURI = Network.URI.Encode.encodeText
    title = modName' <> lineNum
    url fp = "command:daml.revealLocation?"
      <> encodeURI ("[\"file://" <> T.pack fp <> "\", "
      <> TE.show sline <> ", " <> TE.show eline <> "]")

    lineNum = ":" <> pint32 sline <> ":" <> pint32 scol
    pint32 :: Int32 -> T.Text
    pint32 = TE.show . (1+) -- locations are 0-indexed.



prettyTxId :: Int32 -> Doc SyntaxClass
prettyTxId txid =
  linkToIdSC ("n" <> TE.show txid) $ string (show txid)

linkToIdSC :: T.Text -> Doc SyntaxClass -> Doc SyntaxClass
linkToIdSC targetId =
  -- This seems to be the only way to easily scroll to a anchor within
  -- the iframe inside VSCode. One downside is that it's a no-op on further
  -- clicks.
    annotateSC ConstructorSC
  . annotateSC (OnClickSC $ "window.location.hash='" <> targetId <> "';")

prettyNodeId :: NodeId -> Doc SyntaxClass
prettyNodeId (NodeId nodeId) =
    idSC ("n" <> TL.toStrict nodeId)
  $ annotateSC ConstructorSC
  $ text (TL.toStrict nodeId)

prettyNodeIdLink :: NodeId -> Doc SyntaxClass
prettyNodeIdLink (NodeId nodeId) =
  linkToIdSC ("n" <> TL.toStrict nodeId) $ text (TL.toStrict nodeId)

prettyContractId :: TL.Text -> Doc SyntaxClass
prettyContractId coid =
  linkToIdSC ("n" <> TL.toStrict coid) $ ltext coid

linkSC :: T.Text -> T.Text -> Doc SyntaxClass -> Doc SyntaxClass
linkSC url title = annotateSC (LinkSC url title)

idSC :: T.Text -> Doc SyntaxClass -> Doc SyntaxClass
idSC id_ = annotateSC (IdSC id_)

prettyMay :: T.Text -> (x -> Doc SyntaxClass) -> Maybe x -> Doc SyntaxClass
prettyMay miss _ Nothing = text miss
prettyMay _ p (Just x)   = p x

prettyMayParty :: Maybe Party -> Doc SyntaxClass
prettyMayParty Nothing  = text "<missing party>"
prettyMayParty (Just p) = prettyParty p

prettyParty :: Party -> Doc SyntaxClass
prettyParty (Party p) = text ("'" <> TL.toStrict p <> "'")

ltext :: TL.Text -> Doc a
ltext = text . TL.toStrict

mapV :: (a -> b) -> V.Vector a -> [b]
mapV f = map f . V.toList

prettyChildren :: PrettyLevel -> V.Vector NodeId -> M (Doc SyntaxClass)
prettyChildren lvl cs
  | V.null cs = pure mempty
  | otherwise = do
        children <- mapM (lookupNode >=> prettyNode lvl) (V.toList cs)
        pure $ keyword_ "children:" $$ vsep children

prettyNodeNode :: PrettyLevel -> NodeNode -> M (Doc SyntaxClass)
prettyNodeNode lvl nn = do
  world <- askWorld
  case nn of
    NodeNodeCreate Node_Create{..} ->
      pure $
        case node_CreateContractInstance of
          Nothing -> text "<missing contract instance>"
          Just ContractInstance{..} ->
            let (parties, kw) = partiesAction node_CreateSignatories "creates" "create" in
            parties
            <-> ( -- group to align "create" and "with"
              (kw <-> prettyMay "<TEMPLATE?>" (prettyDefName lvl world) contractInstanceTemplateId)
              $$ maybe
                    mempty
                    (\v ->
                      keyword_ "with" $$
                        nest 2 (prettyValue' lvl False 0 world v))
                      contractInstanceValue
              )

    NodeNodeFetch Node_Fetch{..} -> do
      let (parties, kw) = partiesAction node_FetchActingParties "fetches" "fetch"
      pure
        $   parties
        <-> (
              kw
          <-> prettyContractId node_FetchContractId
          <-> maybe mempty
                  (\tid -> parens (prettyDefName lvl world tid))
                  node_FetchTemplateId
           $$ foldMap
              (\key ->
                  let prettyKey = prettyMay "<KEY?>" (prettyValue' lvl False 0 world) $ keyWithMaintainersKey key
                  in
                  hsep [ keyword_ "by key", prettyKey ]
              )
              node_FetchFetchByKey
        )

    NodeNodeExercise Node_Exercise{..} -> do
      ppChildren <- prettyChildren lvl node_ExerciseChildren
      let (parties, kw) = partiesAction node_ExerciseActingParties "exercises" "exercise"
      pure
        $   parties
        <-> ( -- group to align "exercises" and "with"
              kw
          <-> hsep
              [ prettyChoiceId world node_ExerciseTemplateId node_ExerciseChoiceId
              , keyword_ "on"
              , prettyContractId node_ExerciseTargetContractId
              , parens (prettyMay "<missing TemplateId>" (prettyDefName lvl world) node_ExerciseTemplateId)
              ]
           $$ foldMap
              (\key ->
                let prettyKey = prettyMay "<KEY?>" (prettyValue' lvl False 0 world) $ keyWithMaintainersKey key
                in
                hsep [ keyword_ "by key", prettyKey ]
              )
              node_ExerciseExerciseByKey
           $$ if isUnitValue node_ExerciseChosenValue
              then mempty
              else keyword_ "with"
                $$ nest 2
                     (prettyMay "<missing value>"
                       (prettyValue' lvl False 0 world)
                       node_ExerciseChosenValue)
        )
        $$ ppChildren

    NodeNodeLookupByKey Node_LookupByKey{..} -> do
      pure $
        keyword_ "lookupByKey"
          <-> prettyMay "<TEMPLATE?>" (prettyDefName lvl world) node_LookupByKeyTemplateId
          $$ text "with key"
          $$ nest 2
            (prettyMay "<KEY?>"
              (prettyMay "<KEY?>" (prettyValue' lvl False 0 world) . keyWithMaintainersKey)
              node_LookupByKeyKeyWithMaintainers)
          $$ if TL.null node_LookupByKeyContractId
            then text "not found"
            else text "found:" <-> text (TL.toStrict node_LookupByKeyContractId)

    NodeNodeRollback Node_Rollback{..} -> do
        ppChildren <- prettyChildren lvl node_RollbackChildren
        pure $ keyword_ "rollback" $$ ppChildren

-- | Take a list of parties and the singular and multiple present tense verbs
-- Depending on the count of parties, returns a prettified list of these elements, using @,@ and @and@, as well as the correct verb
-- e.g. @[a, b, c] -> a, b and c@
partiesAction :: V.Vector Party -> String -> String -> (Doc SyntaxClass, Doc SyntaxClass)
partiesAction pv singular multiple =
  case unsnoc $ mapV prettyParty pv of
    Just (init@(_:_), last) -> (fcommasep init <-> keyword_ "and" <-> last, keyword_ multiple)
    Just (_, p) ->             (p, keyword_ singular)
    Nothing ->                 (text "No-one/unknown", keyword_ singular)

isUnitValue :: Maybe Value -> Bool
isUnitValue (Just (Value (Just ValueSumUnit{}))) = True
isUnitValue (Just (Value (Just (ValueSumRecord Record{recordFields})))) = V.null recordFields
isUnitValue _ = False

prettyNode :: PrettyLevel -> Node -> M (Doc SyntaxClass)
prettyNode lvl Node{..}
  | Nothing <- nodeNode =
      pure "<missing node>"

  | Just node <- nodeNode = do
      ppNode <- prettyNodeNode lvl node
      let ppConsumedBy =
              maybe mempty
                (\nodeId -> meta $ archivedSC $ text "consumed by:" <-> prettyNodeIdLink nodeId)
                nodeConsumedBy

      let ppReferencedBy =
            if V.null nodeReferencedBy
            then mempty
            else meta $ keyword_ "referenced by"
                   <-> fcommasep (mapV prettyNodeIdLink nodeReferencedBy)

      let mkPpDisclosures kw disclosures =
            if null disclosures
            then mempty
            else
              meta $ keyword_ kw
                <-> fcommasep
                  (map
                    (\(Disclosure p txId _explicit) -> prettyMayParty p <-> parens (prettyTxId txId))
                    disclosures)

      let (nodeWitnesses, nodeDivulgences) = partition disclosureExplicit $ V.toList nodeDisclosures
      let ppDisclosedTo = mkPpDisclosures "disclosed to (since):" nodeWitnesses
      let ppDivulgedTo = mkPpDisclosures "divulged to (since):" nodeDivulgences

      pure
         $ prettyMay "<missing node id>" prettyNodeId nodeNodeId
        $$ vcat
             [ ppConsumedBy, ppReferencedBy, ppDisclosedTo, ppDivulgedTo
             , arrowright ppNode
             ]

  where
    arrowright p = text "└─>" <-> p
    meta p       = text "│  " <-> p
    archivedSC = annotateSC PredicateSC -- Magenta

prettyPartialTransaction :: PrettyLevel -> PartialTransaction -> M (Doc SyntaxClass)
prettyPartialTransaction lvl ptx@PartialTransaction{..} = do
  world <- askWorld
  let ppNodes =
           runM partialTransactionNodes world
         $ fmap vsep
         $ mapM (lookupNode >=> prettyNode lvl)
                (V.toList partialTransactionRoots)
  pure $ vcat
    [ case ptxExerciseContext ptx of
        Nothing -> mempty
        Just ExerciseContext{..} ->
          text "Failed exercise"
            <-> parens (prettyMayLocation world exerciseLocation) <> ":"
            $$ nest 2 (
                keyword_ "exercises"
            <-> prettyMay "<missing template id>"
                  (\tid ->
                      prettyChoiceId world tid choiceId)
                  (contractRefTemplateId <$> targetId)
            <-> keyword_ "on"
            <-> prettyMay "<missing>"
                  (prettyContractRef lvl world)
                  targetId
             $$ keyword_ "with"
             $$ ( nest 2
                $ prettyMay "<missing>"
                    (prettyValue' lvl False 0 world)
                    chosenValue)
            )

   , if V.null partialTransactionRoots
     then mempty
     else text "Sub-transactions:" $$ nest 3 ppNodes
   ]


prettyValue' :: PrettyLevel -> Bool -> Int -> LF.World -> Value -> Doc SyntaxClass
prettyValue' _ _ _ _ (Value Nothing) = text "<missing value>"
prettyValue' lvl showRecordType prec world (Value (Just vsum)) = case vsum of
  ValueSumRecord (Record mbRecordId fields) ->
    maybeParens (prec > precWith) $
      (if showRecordType
       then \fs -> prettyMay "" (prettyDefName lvl world) mbRecordId <-> keyword_ "with" $$ nest 2 fs
       else id)
      (sep (punctuate ";" (mapV prettyField fields)))
  ValueSumVariant (Variant mbVariantId ctor mbValue) ->
        prettyMay "" (\v -> prettyDefName lvl world v <> ":") mbVariantId <> ltext ctor
    <-> prettyMay "<missing value>" (prettyValue' lvl True precHighest world) mbValue
  ValueSumEnum (Enum mbEnumId constructor) ->
        prettyMay "" (\x -> prettyDefName lvl world x <> ":") mbEnumId <> ltext constructor
  ValueSumList (List elems) ->
    brackets (fcommasep (mapV (prettyValue' lvl True prec world) elems))
  ValueSumContractId coid -> prettyContractId coid
  ValueSumInt64 i -> string (show i)
  ValueSumDecimal ds -> ltext ds
  ValueSumText t -> char '"' <> ltext t <> char '"'
  ValueSumTimestamp ts -> prettyTimestamp ts
  ValueSumParty p -> char '\'' <> ltext p <> char '\''
  ValueSumBool True -> text "true"
  ValueSumBool False -> text "false"
  ValueSumUnit{} -> text "{}"
  ValueSumDate d -> prettyDate d
  ValueSumOptional (Optional Nothing) -> text "none"
  ValueSumOptional (Optional (Just v)) -> "some " <> prettyValue' lvl True precHighest world v
  ValueSumMap (Map entries) -> "Map" <> brackets (fcommasep (mapV (prettyEntry lvl prec world) entries))
  ValueSumGenMap (GenMap entries) -> "GenMap" <> brackets (fcommasep (mapV (prettyGenMapEntry lvl prec world) entries))
  ValueSumUnserializable what -> ltext what
  where
    prettyField (Field label mbValue) =
      hang (ltext label <-> "=") 2
        (prettyMay "<missing value>" (prettyValue' lvl True precHighest world) mbValue)
    precWith = 1
    precHighest = 9

prettyGenMapEntry :: PrettyLevel -> Int -> LF.World -> GenMap_Entry -> Doc SyntaxClass
prettyGenMapEntry lvl prec world (GenMap_Entry keyM valueM) =
    prettyMay "<missing key>" (prettyValue' lvl True prec world) keyM <> "->" <>
    prettyMay "<missing value>" (prettyValue' lvl True prec world) valueM

prettyEntry :: PrettyLevel -> Int -> LF.World ->  Map_Entry -> Doc SyntaxClass
prettyEntry lvl prec world (Map_Entry key (Just value)) =
   ltext key <> "->" <> prettyValue' lvl True prec world value
prettyEntry _ _ _ (Map_Entry key _) =
   ltext key <> "-> <missing value>"

prettyDate :: Int32 -> Doc a
prettyDate =
    string
  . TF.formatTime TF.defaultTimeLocale "%FT"
  . CP.posixSecondsToUTCTime
  . (24*60*60*)
  . fromIntegral

prettyPackageIdentifier :: PrettyLevel -> PackageIdentifier -> Doc SyntaxClass
prettyPackageIdentifier lvl (PackageIdentifier psum) = case psum of
  Nothing                                    -> mempty
  (Just (PackageIdentifierSumSelf _))        -> mempty
  (Just (PackageIdentifierSumPackageId pid))
    | LF.levelHasPackageIds lvl -> char '@' <> ltext pid
    | otherwise -> mempty

-- | Note that this should only be called with dotted identifiers.
prettyDefName :: PrettyLevel -> LF.World -> Identifier -> Doc SyntaxClass
prettyDefName lvl world (Identifier mbPkgId (UnmangledQualifiedName modName defName))
  | Just mod0 <- lookupModule world mbPkgId modName
  , Just fp <- LF.moduleSource mod0
  , Just (LF.SourceLoc _mref sline _scol eline _ecol) <- lookupDefLocation mod0 defName =
      linkSC (revealLocationUri fp sline eline) name ppName
  | otherwise =
      ppName
  where
    name = LF.moduleNameString modName <> ":" <> defName
    ppName = text name <> ppPkgId
    ppPkgId = maybe mempty (prettyPackageIdentifier lvl) mbPkgId

prettyQualifiedName :: TL.Text -> Doc SyntaxClass
prettyQualifiedName (UnmangledQualifiedName modName defName) = text $ LF.moduleNameString modName <> ":" <> defName

prettyGlobalKey :: PrettyLevel -> LF.World -> GlobalKey -> Doc SyntaxClass
prettyGlobalKey lvl world gk = vcat [
    prettyMay "<no value>" (prettyValue' lvl False 0 world) (globalKeyKey gk),
    "for template",
    prettyQualifiedName $ globalKeyName gk,
    maybe mempty (prettyPackageIdentifier lvl) (globalKeyPackage gk)
  ]

prettyPackageMetadata :: PackageMetadata -> Doc SyntaxClass
prettyPackageMetadata (PackageMetadata name version) = text $ TL.toStrict $ name <> "-" <> version

prettyChoiceId
  :: LF.World -> Maybe Identifier -> TL.Text
  -> Doc SyntaxClass
prettyChoiceId _ Nothing choiceId = ltext choiceId
prettyChoiceId world (Just (Identifier mbPkgId (UnmangledQualifiedName modName defName))) (TL.toStrict -> choiceId)
  | Just mod0 <- lookupModule world mbPkgId modName
  , Just fp <- LF.moduleSource mod0
  , Just tpl <- NM.lookup (LF.TypeConName [defName]) (LF.moduleTemplates mod0)
  , Just chc <- NM.lookup (LF.ChoiceName choiceId) (LF.tplChoices tpl)
  , Just (LF.SourceLoc _mref sline _scol eline _ecol) <- LF.chcLocation chc =
      linkSC (revealLocationUri fp sline eline) choiceId $ text choiceId
  | otherwise =
      text choiceId

revealLocationUri :: FilePath -> Int -> Int -> T.Text
revealLocationUri fp sline eline =
    "command:daml.revealLocation?"
  <> encodeURI ("[\"file://" <> T.pack fp <> "\", "
  <> TE.show sline <> ", " <> TE.show eline <> "]")
  where
    encodeURI = Network.URI.Encode.encodeText

prettyContractRef :: PrettyLevel -> LF.World -> ContractRef -> Doc SyntaxClass
prettyContractRef lvl world (ContractRef coid tid) =
  hsep
  [ prettyContractId coid
  , parens (prettyMay "<missing template id>" (prettyDefName lvl world) tid)
  ]


-- TABLE VIEW

data NodeInfo = NodeInfo
    { niTemplateId :: Identifier
    , niNodeId :: NodeId
    , niValue :: Value
    , niActive :: Bool
    , niSignatories :: S.Set T.Text
    , niStakeholders :: S.Set T.Text  -- Is a superset of `niSignatories`.
    , niWitnesses :: S.Set T.Text  -- Is a superset of `niStakeholders`.
    , niDivulgences :: S.Set T.Text
    }

data Table = Table
    { tTemplateId :: Identifier
    , tRows :: [NodeInfo]
    }

isActive :: S.Set TL.Text -> Node -> Bool
isActive activeContracts Node{..} = case nodeNode of
    Just(NodeNodeCreate Node_Create{node_CreateContractId}) -> node_CreateContractId `S.member` activeContracts
    _ -> False

nodeInfo :: S.Set TL.Text -> Node -> Maybe NodeInfo
nodeInfo activeContracts node@Node{..} = do
    NodeNodeCreate create <- nodeNode
    niNodeId <- nodeNodeId
    inst <- node_CreateContractInstance create
    niTemplateId <- contractInstanceTemplateId inst
    niValue <- contractInstanceValue inst
    let niActive = isActive activeContracts node
    let niSignatories = S.fromList $ map (TL.toStrict . partyParty) $ V.toList (node_CreateSignatories create)
    let niStakeholders = S.fromList $ map (TL.toStrict . partyParty) $ V.toList (node_CreateStakeholders create)
    let (nodeWitnesses, nodeDivulgences) = partition disclosureExplicit $ V.toList nodeDisclosures
    let niWitnesses = S.fromList $ mapMaybe party nodeWitnesses
    let niDivulgences = S.fromList $ mapMaybe party nodeDivulgences
    pure NodeInfo{..}
    where
        party :: Disclosure -> Maybe T.Text
        party Disclosure{..} = do
            Party{..} <- disclosureParty
            pure (TL.toStrict partyParty)


groupTables :: [NodeInfo] -> [Table]
groupTables =
    map (uncurry Table)
    . MS.toList
    . MS.map (sortOn (parseNodeId . niNodeId))
    . MS.fromListWith (++)
    . map (\node -> (niTemplateId node, [node]))

renderValue :: PrettyLevel -> LF.World -> [T.Text] -> Value -> (H.Html, H.Html)
renderValue lvl world name = \case
    Value (Just (ValueSumRecord (Record _ fields))) ->
        let (ths, tds) = unzip $ map renderField (V.toList fields)
        in (mconcat ths, mconcat tds)
    value ->
        let th = H.th $ H.text $ T.intercalate "." name
            td = H.td $ H.text $ renderPlain $ prettyValue' lvl True 0 world value
        in (th, td)
    where
        renderField (Field label mbValue) =
            renderValue lvl world (name ++ [TL.toStrict label]) (fromJust mbValue)

renderRow :: PrettyLevel -> LF.World -> S.Set T.Text -> NodeInfo -> (H.Html, H.Html)
renderRow lvl world parties NodeInfo{..} =
    let (ths, tds) = renderValue lvl world [] niValue
        header = H.tr $ mconcat
            [ H.th "id"
            , H.th "status"
            , ths
            , foldMap (H.th . (H.div H.! A.class_ "observer") . H.text) parties
            ]
        viewStatus party =
            let (label, mbHint)
                    | party `S.member` niSignatories = ("S", Just "Signatory")
                    | party `S.member` niStakeholders = ("O", Just "Observer")
                    | party `S.member` niWitnesses = ("W", Just "Witness")
                    | party `S.member` niDivulgences = ("D", Just "Divulged")
                    | otherwise = ("-", Nothing)
            in
            H.td H.! A.class_ (H.textValue $ T.unwords $ "disclosure" : ["disclosed" | isJust mbHint]) $ H.div H.! A.class_ "tooltip" $ do
                H.span $ H.text label
                whenJust mbHint $ \hint -> H.span H.! A.class_ "tooltiptext" $ H.text hint
        active = if niActive then "active" else "archived"
        row = H.tr H.! A.class_ (H.textValue active) $ mconcat
            [ H.td (H.text $ renderPlain $ prettyNodeId niNodeId)
            , H.td (H.text active)
            , tds
            , foldMap viewStatus parties
            ]
    in (header, row)

-- TODO(MH): The header should be rendered from the type rather than from the
-- first value.
renderTable :: PrettyLevel -> LF.World -> Table -> H.Html
renderTable lvl world Table{..} = H.div H.! A.class_ active $ do
    let parties = S.unions $ map (\row -> niWitnesses row `S.union` niDivulgences row) tRows
    H.h1 $ renderPlain $ prettyDefName lvl world tTemplateId
    let (headers, rows) = unzip $ map (renderRow lvl world parties) tRows
    H.table $ head headers <> mconcat rows
    where
        active = if any niActive tRows then "active" else "archived"

renderTableView :: PrettyLevel -> LF.World -> S.Set TL.Text -> V.Vector Node -> Maybe H.Html
renderTableView lvl world activeContracts nodes =
    let nodeInfos = mapMaybe (nodeInfo activeContracts) (V.toList nodes)
        tables = groupTables nodeInfos
    in if null nodeInfos then Nothing else Just $ H.div H.! A.class_ "table" $ foldMap (renderTable lvl world) tables

renderTransactionView :: PrettyLevel -> LF.World -> S.Set TL.Text -> ScenarioResult -> H.Html
renderTransactionView lvl world activeContracts res =
    let doc = prettyScenarioResult lvl world activeContracts res
    in H.div H.! A.class_ "da-code transaction" $ Pretty.renderHtml 128 doc

renderScenarioResult :: PrettyLevel -> LF.World -> ScenarioResult -> T.Text
renderScenarioResult lvl world res = TL.toStrict $ Blaze.renderHtml $ do
    H.docTypeHtml $ do
        H.head $ do
            H.style $ H.text Pretty.highlightStylesheet
            H.script "" H.! A.src "$webviewSrc"
            H.link H.! A.rel "stylesheet" H.! A.href "$webviewCss"
        let activeContracts = S.fromList (V.toList (scenarioResultActiveContracts res))
        let tableView = renderTableView lvl world activeContracts (scenarioResultNodes res)
        let transView = renderTransactionView lvl world activeContracts res
        renderViews SuccessView tableView transView

renderScenarioError :: PrettyLevel -> LF.World -> ScenarioError -> T.Text
renderScenarioError lvl world err = TL.toStrict $ Blaze.renderHtml $ do
    H.docTypeHtml $ do
        H.head $ do
            H.style $ H.text Pretty.highlightStylesheet
            H.script "" H.! A.src "$webviewSrc"
            H.link H.! A.rel "stylesheet" H.! A.href "$webviewCss"
        let tableView = do
                table <- renderTableView lvl world (activeContractsFromScenarioError err) (scenarioErrorNodes err)
                pure $ H.div H.! A.class_ "table" $ do
                  Pretty.renderHtml 128 $ annotateSC ErrorSC "Script execution failed, displaying state before failing transaction"
                  table
        let transView =
                let doc = prettyScenarioError lvl world err
                in H.div H.! A.class_ "da-code transaction" $ Pretty.renderHtml 128 doc
        renderViews ErrorView tableView transView

data ViewType = SuccessView | ErrorView

renderViews :: ViewType -> Maybe H.Html -> H.Html -> H.Html
renderViews viewType tableView transView =
    case tableView of
        Nothing -> H.body H.! A.class_ "hide_note" $ do
            noteView
            transView
        Just tbl -> H.body H.! A.class_ ("hide_archived hide_note hidden_disclosure" <> extraClasses) $ do
            H.div $ do
                H.button H.! A.onclick "toggle_view();" $ do
                    H.span H.! A.class_ "table" $ H.text "Show transaction view"
                    H.span H.! A.class_ "transaction" $ H.text "Show table view"
                H.text " "
                H.span H.! A.class_ "table" $ do
                    H.input H.! A.type_ "checkbox" H.! A.id "show_archived" H.! A.onchange "show_archived_changed();"
                    H.label H.! A.for "show_archived" $ "Show archived"
                H.span H.! A.class_ "table" $ do
                    H.input H.! A.type_ "checkbox" H.! A.id "show_detailed_disclosure" H.! A.onchange "toggle_detailed_disclosure();"
                    H.label H.! A.for "show_detailed_disclosure" $ "Show detailed disclosure"
            noteView
            tbl
            transView
  where
    noteView = H.div H.! A.class_ "note" H.! A.id "note" $ H.toHtml $ T.pack " "
    extraClasses = case viewType of
        SuccessView -> " hide_transaction" -- default to table view
        ErrorView -> " hide_table" -- default to transaction view

scenarioNotInFileNote :: T.Text -> T.Text
scenarioNotInFileNote file = htmlNote $ T.pack $
    "This script no longer exists in the source file: " ++ T.unpack file

fileWScenarioNoLongerCompilesNote :: T.Text -> T.Text
fileWScenarioNoLongerCompilesNote file = htmlNote $ T.pack $
    "The source file containing this script no longer compiles: " ++ T.unpack file

htmlNote :: T.Text -> T.Text
htmlNote t = TL.toStrict $ Blaze.renderHtml $ H.docTypeHtml $ H.span H.! A.class_ "da-hl-warning" $ H.toHtml t
