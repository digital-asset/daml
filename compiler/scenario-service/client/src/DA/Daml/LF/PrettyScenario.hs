-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Pretty-printing of scenario results
module DA.Daml.LF.PrettyScenario
  ( prettyScenarioResult
  , prettyScenarioError
  , prettyBriefScenarioError
  , renderScenarioResult
  , lookupDefLocation
  , ModuleRef
  ) where

import           Control.Lens               (preview, review)
import           Control.Monad              ((>=>))
import           Control.Monad.Reader       (Reader, asks, runReader)
import           Control.Monad.Trans.Except (ExceptT (..), runExceptT, throwE)
import           DA.Daml.LF.Decimal  (stringToDecimal)
import qualified DA.Daml.LF.Ast             as LF
import           DA.Prelude
import           DA.Pretty as Pretty
import           Data.Either.Extra          (eitherToMaybe)
import           Data.Int                   (Int32)
import qualified Data.NameMap               as NM
import qualified Data.Map.Strict            as MS
import qualified Data.Ratio                 as Ratio
import qualified Data.Set                   as S
import qualified Data.Text                  as T
import qualified Data.Text.Extended         as TE
import qualified Data.Text.Lazy             as TL
import qualified Data.Time.Clock.POSIX      as CP
import qualified Data.Time.Format           as TF
import qualified Data.Vector                as V
import qualified Network.URI.Encode
import           ScenarioService
import qualified Text.Blaze.Html5 as H
import qualified Text.Blaze.Html5.Attributes as A
import qualified Text.Blaze.Html.Renderer.Text as Blaze

data Error = ErrorMissingNode NodeId
type M = ExceptT Error (Reader (MS.Map NodeId Node, LF.World))

type ModuleRef = LF.Qualified ()

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
    LF.dvalLocation <$> NM.lookup (Tagged defName) (LF.moduleValues mod0)
    <|>
    LF.tplLocation <$> NM.lookup (Tagged [defName]) (LF.moduleTemplates mod0)

lookupModule :: LF.World -> Maybe PackageIdentifier -> LF.ModuleName -> Maybe LF.Module
lookupModule world mbPkgId modName = do
  let pkgRef = case mbPkgId of
       Just (PackageIdentifier (Just (PackageIdentifierSumPackageId pkgId))) ->
         LF.PRImport $ Tagged $ TL.toStrict pkgId
       _ -> LF.PRSelf
  eitherToMaybe (LF.lookupModule (LF.Qualified pkgRef modName ()) world)

lookupModuleFromQualifiedName ::
     LF.World -> Maybe PackageIdentifier -> T.Text
  -> Maybe (LF.ModuleName, T.Text, LF.Module)
lookupModuleFromQualifiedName world mbPkgId qualName = do
  let (modName, defName) = case T.splitOn ":" qualName of
        [modNm, defNm] -> (Tagged (T.splitOn "." modNm), defNm)
        _ -> error "Bad definition"
  modu <- lookupModule world mbPkgId modName
  return (modName, defName, modu)

parseNodeId :: NodeId -> [Integer]
parseNodeId =
    fmap (fromMaybe 0 . readMay . TL.unpack)
  . TL.splitOn ":"
  . nodeIdId

prettyScenarioResult
  :: LF.World -> ScenarioResult -> Doc SyntaxClass
prettyScenarioResult world (ScenarioResult steps nodes retValue _finaltime traceLog) =
  let ppSteps = runM nodes world (vsep <$> mapM prettyScenarioStep (V.toList steps))
      isActive Node{..} = case nodeNode of
        Just NodeNodeCreate{} -> isNothing nodeConsumedBy
        _ -> False
      sortNodeIds = sortBy (\a b -> compare (parseNodeId a) (parseNodeId b))
      ppActive =
          fcommasep
        $ map prettyNodeIdLink
        $ sortNodeIds $ mapMaybe nodeNodeId
        $ filter isActive (V.toList nodes)

      ppTrace = vcat $ map (prettyTraceMessage world) (V.toList traceLog)
  in vsep
    [ label_ "Transactions: " ppSteps
    , label_ "Active contracts: " ppActive
    , maybe mempty (\v -> label_ "Return value:" (prettyValue' True 0 world v)) retValue
    , if V.null traceLog
      then text ""
      else text "Trace: " $$ nest 2 ppTrace
    ]

prettyBriefScenarioError
  :: LF.World -> ScenarioError -> Doc SyntaxClass
prettyBriefScenarioError world ScenarioError{..} = runM scenarioErrorNodes world $ do
  ppError <- prettyScenarioErrorError scenarioErrorError
  pure $
    annotateSC ErrorSC
      (text "Scenario execution" <->
        (if isNothing scenarioErrorCommitLoc
         then "failed:"
         else "failed on commit at"
           <-> prettyMayLocation world scenarioErrorCommitLoc <> char ':')
      )
    $$ nest 2 ppError

prettyScenarioError
  :: LF.World -> ScenarioError -> Doc SyntaxClass
prettyScenarioError world ScenarioError{..} = runM scenarioErrorNodes world $ do
  ppError <- prettyScenarioErrorError scenarioErrorError
  ppSteps <- vsep <$> mapM prettyScenarioStep (V.toList scenarioErrorScenarioSteps)
  ppPtx <- forM scenarioErrorPartialTransaction $ \ptx -> do
      p <- prettyPartialTransaction ptx
      pure $ text "Partial transaction:" $$ nest 2 p
  let ppTrace =
        vcat
          (map (prettyTraceMessage world)
               (V.toList scenarioErrorTraceLog))
  pure $
    vsep $ catMaybes
    [ Just $ error_ (text "Scenario execution" <->
      (if isNothing scenarioErrorCommitLoc
       then "failed:"
       else "failed on commit at"
         <-> prettyMayLocation world scenarioErrorCommitLoc <> char ':'))
      $$ nest 2 ppError

    , if isNothing scenarioErrorLastLoc
      then Nothing
      else Just $ label_ "Last source location:"
                $ prettyMayLocation world scenarioErrorLastLoc

    , Just $ "Ledger time:" <-> prettyTimestamp scenarioErrorLedgerTime

    , ppPtx

    , if V.null scenarioErrorScenarioSteps
      then Nothing
      else Just $ text "Committed transactions: " $$ nest 2 ppSteps

    , if V.null scenarioErrorTraceLog
      then Nothing
      else Just $ text "Trace: " $$ nest 2 ppTrace
    ]

prettyTraceMessage
  :: LF.World -> TraceMessage -> Doc SyntaxClass
prettyTraceMessage _world msg =
  -- TODO(JM): Locations are still missing in DAML 1.2, and
  -- that's the only place where we get traces.
  --  prettyMayLocation world (traceMessageLocation msg)
  ltext (traceMessageMessage msg)

prettyScenarioErrorError :: Maybe ScenarioErrorError -> M (Doc SyntaxClass)
prettyScenarioErrorError Nothing = pure $ text "<missing error details>"
prettyScenarioErrorError (Just err) =  do
  world <- askWorld
  case err of
    ScenarioErrorErrorCrash reason -> pure $ text "CRASH:" <-> ltext reason
    ScenarioErrorErrorUserError reason -> pure $ text "Aborted: " <-> ltext reason
    ScenarioErrorErrorTemplatePrecondViolated ScenarioError_TemplatePreconditionViolated{..} -> do
      pure $
        "Template pre-condition violated in:"
          $$ nest 2
          (   "create"
          <-> prettyMay "<missing template id>" (prettyDefName world) scenarioError_TemplatePreconditionViolatedTemplateId
          $$ (   keyword_ "with"
              $$ nest 2 (prettyMay "<missing argument>" (prettyValue' False 0 world) scenarioError_TemplatePreconditionViolatedArg)
             )
          )
    ScenarioErrorErrorUpdateLocalContractNotActive ScenarioError_ContractNotActive{..} ->
      pure $ vcat
        [ "Attempt to exercise a contract that was consumed in same transaction."
        , "Contract:"
            <-> prettyMay "<missing contract>"
                  (prettyContractRef world)
                  scenarioError_ContractNotActiveContractRef
        ]
    ScenarioErrorErrorScenarioContractNotActive ScenarioError_ContractNotActive{..} -> do
      pure $ vcat
        [ "Attempt to exercise a consumed contract"
            <-> prettyMay "<missing contract>"
                  (prettyContractRef world)
                  scenarioError_ContractNotActiveContractRef
        , "Consumed by:"
            <-> prettyMay "<missing node id>" prettyNodeIdLink scenarioError_ContractNotActiveConsumedBy
        ]
    ScenarioErrorErrorScenarioCommitError (CommitError Nothing) -> do
      pure "Unknown commit error"

    ScenarioErrorErrorScenarioCommitError (CommitError (Just (CommitErrorSumFailedAuthorizations fas))) -> do
      pure $ vcat $ mapV (prettyFailedAuthorization world) (failedAuthorizationsFailedAuthorizations fas)

    ScenarioErrorErrorScenarioCommitError (CommitError (Just (CommitErrorSumUniqueKeyViolation gk))) -> do
      pure $ vcat
        [ "Commit error due to unique key violation for key"
        , nest 2 (prettyMay "<missing key>" (prettyValue' False 0 world) (globalKeyKey gk))
        , "for template"
        , nest 2 (prettyMay "<missing template id>" (prettyDefName world) (globalKeyTemplateId gk))
        ]

    ScenarioErrorErrorUnknownContext ctxId ->
      pure $ "Unknown scenario interpretation context:" <-> string (show ctxId)
    ScenarioErrorErrorUnknownScenario name ->
      pure $ "Unknown scenario:" <-> prettyDefName world name
    ScenarioErrorErrorScenarioContractNotEffective ScenarioError_ContractNotEffective{..} ->
      pure $ vcat
        [ "Attempt to fetch or exercise a contract not yet effective."
        , "Contract:"
        <-> prettyMay "<missing contract>" (prettyContractRef world)
              scenarioError_ContractNotEffectiveContractRef
        , "Effective at:"
        <-> prettyTimestamp scenarioError_ContractNotEffectiveEffectiveAt
        ]

    ScenarioErrorErrorScenarioMustfailSucceeded _ ->
      pure "A must-fail commit succeeded."
    ScenarioErrorErrorScenarioInvalidPartyName name ->
      pure $ "Invalid party name: " <-> ltext name
    ScenarioErrorErrorScenarioContractNotVisible ScenarioError_ContractNotVisible{..} ->
      pure $ vcat
        [ "Attempt to fetch or exercise a contract not visible to the committer."
        , label_ "Contract: "
            $ prettyMay "<missing contract>"
                (prettyContractRef world)
                scenarioError_ContractNotVisibleContractRef
        , label_ "Committer:" $ prettyMay "<missing party>" prettyParty scenarioError_ContractNotVisibleCommitter
        , label_ "Disclosed to:"
            $ fcommasep
            $ mapV prettyParty scenarioError_ContractNotVisibleObservers
        ]

prettyFailedAuthorization :: LF.World -> FailedAuthorization -> Doc SyntaxClass
prettyFailedAuthorization world (FailedAuthorization mbNodeId mbFa) =
  hcat
    [ prettyMay "<missing node id>" prettyNodeIdLink mbNodeId
    , text ": "
    , case mbFa of
        Just (FailedAuthorizationSumCreateMissingAuthorization
          (FailedAuthorization_CreateMissingAuthorization templateId mbLoc authParties reqParties)) ->
          "create of" <-> prettyMay "<missing template id>" (prettyDefName world) templateId
          <-> "at" <-> prettyMayLocation world mbLoc
          $$
            ("failed due to a missing authorization from"
             <->
             ( fcommasep
             $ map (prettyParty . Party)
             $ S.toList
             $ S.fromList (mapV partyParty reqParties)
               `S.difference`
               S.fromList (mapV partyParty authParties)
             )
            )

        Just (FailedAuthorizationSumFetchMissingAuthorization
          (FailedAuthorization_FetchMissingAuthorization templateId mbLoc authParties stakeholders)) ->
          "fetch of" <-> prettyMay "<missing template id>" (prettyDefName world) templateId
          <-> "at" <-> prettyMayLocation world mbLoc
          $$
            ("failed since none of the stakeholders"
             <->
             ( fcommasep
             $ map (prettyParty . Party)
             $ mapV partyParty stakeholders
            ))
          $$
            ("is in the authorizing set"
             <->
             ( fcommasep
             $ map (prettyParty . Party)
             $ mapV partyParty authParties
            ))

        Just (FailedAuthorizationSumExerciseMissingAuthorization
          (FailedAuthorization_ExerciseMissingAuthorization templateId choiceId mbLoc authParties reqParties)) ->
          "exercise of" <-> prettyChoiceId world templateId choiceId
          <-> "in" <-> prettyMay "<missing template id>" (prettyDefName world) templateId
          <-> "at" <-> prettyMayLocation world mbLoc
          $$
            ("failed due to a missing authorization from"
             <->
             ( fcommasep
             $ map (prettyParty . Party)
             $ S.toList
             $ S.fromList (mapV partyParty reqParties)
               `S.difference`
               S.fromList (mapV partyParty authParties)
             )
            )

        Just (FailedAuthorizationSumActorMismatch
          (FailedAuthorization_ActorMismatch templateId choiceId mbLoc ctrls givenActors)) ->
          "exercise of" <-> prettyChoiceId world templateId choiceId
          <-> "in" <-> prettyMay "<missing template id>" (prettyDefName world) templateId
          <-> "at" <-> prettyMayLocation world mbLoc
          $$
            ("failed due to authorization error:"
            $$ "the choice's controlling parties"
            <-> brackets (fcommasep (mapV prettyParty ctrls))
            $$ "is not a subset of the authorizing parties"
            <-> brackets (fcommasep (mapV prettyParty givenActors))
            )

        Just (FailedAuthorizationSumNoControllers
          (FailedAuthorization_NoControllers templateId choiceId mbLoc)) ->
          "exercise of" <-> prettyChoiceId world templateId choiceId
          <-> "in" <-> prettyMay "<missing template id>" (prettyDefName world) templateId
          <-> "at" <-> prettyMayLocation world mbLoc
          $$ "failed due missing controllers"

        Just (FailedAuthorizationSumNoSignatories
          (FailedAuthorization_NoSignatories templateId mbLoc)) ->
          "create of"
          <-> prettyMay "<missing template id>" (prettyDefName world) templateId
          <-> "at" <-> prettyMayLocation world mbLoc
          $$ "failed due missing signatories"

        Just (FailedAuthorizationSumLookupByKeyMissingAuthorization
          (FailedAuthorization_LookupByKeyMissingAuthorization templateId mbLoc authParties maintainers)) ->
         "lookup by key of" <-> prettyMay "<missing template id>" (prettyDefName world) templateId
         <-> "at" <-> prettyMayLocation world mbLoc
         $$
            ("failed due to a missing authorization from"
             <->
             ( fcommasep
             $ map (prettyParty . Party)
             $ S.toList
             $ S.fromList (mapV partyParty maintainers)
               `S.difference`
               S.fromList (mapV partyParty authParties)
             )
            )


        Nothing ->
          text "<missing failed_authorization>"
    ]


prettyScenarioStep :: ScenarioStep -> M (Doc SyntaxClass)
prettyScenarioStep (ScenarioStep _stepId Nothing) =
  pure $ text "<missing scenario step>"
prettyScenarioStep (ScenarioStep stepId (Just step)) = do
  world <- askWorld
  case step of
    ScenarioStepStepCommit (ScenarioStep_Commit txId (Just tx) mbLoc) ->
      prettyCommit txId mbLoc tx

    ScenarioStepStepAssertMustFail (ScenarioStep_AssertMustFail (Just actor) time txId mbLoc) ->
      pure
          $ idSC ("n" <> TE.show txId) (keyword_ "TX")
        <-> prettyTxId txId
        <-> prettyTimestamp time
         $$ (nest 3
             $   keyword_ "mustFailAt"
             <-> prettyParty actor
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

prettyCommit :: Int32 -> Maybe Location -> Transaction -> M (Doc SyntaxClass)
prettyCommit txid mbLoc Transaction{..} = do
  world <- askWorld
  children <- vsep <$> mapM (lookupNode >=> prettyNode) (V.toList transactionRoots)
  return
      $ idSC ("n" <> TE.show txid) (keyword_ "TX")
    <-> prettyTxId txid
    <-> prettyTimestamp transactionEffectiveAt
    <-> parens (prettyMayLocation world mbLoc)
     $$ children

prettyMayLocation :: LF.World -> Maybe Location -> Doc SyntaxClass
prettyMayLocation _ Nothing = text "unknown source"
prettyMayLocation world (Just (Location mbPkgId modName sline scol eline _ecol)) =
      maybe id (\path -> linkSC (url path) title)
        (lookupModule world mbPkgId (Tagged (T.splitOn "." (TL.toStrict modName))) >>= LF.moduleSource)
    $ text title
  where
    modName' = TL.toStrict modName
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
  linkToIdSC ("n" <> TE.show txid) $ char '#' <> string (show txid)

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
  $ char '#' <> text (TL.toStrict nodeId)

prettyNodeIdLink :: NodeId -> Doc SyntaxClass
prettyNodeIdLink (NodeId nodeId) =
  linkToIdSC ("n" <> TL.toStrict nodeId) $ char '#' <> text (TL.toStrict nodeId)

prettyContractId :: TL.Text -> Doc SyntaxClass
prettyContractId coid =
  linkToIdSC ("n" <> TL.toStrict coid) $ char '#' <> ltext coid

-- commentSC :: EmbedsSyntaxClass a => Doc SyntaxClass -> Doc SyntaxClass
-- commentSC = annotateSC CommentSC -- White

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

prettyNodeNode :: NodeNode -> M (Doc SyntaxClass)
prettyNodeNode nn = do
  world <- askWorld
  case nn of
    NodeNodeCreate Node_Create{..} ->
      pure $
        case node_CreateContractInstance of
          Nothing -> text "<missing contract instance>"
          Just ContractInstance{..} ->
             (keyword_ "create" <-> prettyMay "<TEMPLATE?>" (prettyDefName world) contractInstanceTemplateId)
           $$ maybe
                mempty
                (\v ->
                  keyword_ "with" $$
                    nest 2 (prettyValue' False 0 world v))
                  contractInstanceValue

    NodeNodeFetch Node_Fetch{..} ->
      pure $
        keyword_ "fetch"
          <-> prettyContractId node_FetchContractId
          <-> maybe mempty
                  (\tid -> parens (prettyDefName world tid))
                  node_FetchTemplateId

    NodeNodeExercise Node_Exercise{..} -> do
      ppChildren <-
        if V.null node_ExerciseChildren
        then pure mempty
        else
              (keyword_ "children:" $$) . vsep
          <$> mapM (lookupNode >=> prettyNode) (V.toList node_ExerciseChildren)

      pure
        $   fcommasep (mapV prettyParty node_ExerciseActingParties)
        <-> ( -- group to align "exercises" and "with"
              keyword_ "exercises"
          <-> hsep
              [ prettyChoiceId world node_ExerciseTemplateId node_ExerciseChoiceId
              , keyword_ "on"
              , prettyContractId node_ExerciseTargetContractId
              , parens (prettyMay "<missing TemplateId>" (prettyDefName world) node_ExerciseTemplateId)
              ]
           $$ if isUnitValue node_ExerciseChosenValue
              then mempty
              else keyword_ "with"
                $$ nest 2
                     (prettyMay "<missing value>"
                       (prettyValue' False 0 world)
                       node_ExerciseChosenValue)
        )
        $$ ppChildren

    NodeNodeLookupByKey Node_LookupByKey{..} -> do
      pure $
        keyword_ "lookupByKey"
          <-> prettyMay "<TEMPLATE?>" (prettyDefName world) node_LookupByKeyTemplateId
          $$ text "with key"
          $$ nest 2
            (prettyMay "<KEY?>"
              (prettyMay "<KEY?>" (prettyValue' False 0 world) . keyWithMaintainersKey)
              node_LookupByKeyKeyWithMaintainers)
          $$ if TL.null node_LookupByKeyContractId
            then text "not found"
            else text "found:" <-> text (TL.toStrict node_LookupByKeyContractId)

isUnitValue :: Maybe Value -> Bool
isUnitValue (Just (Value (Just ValueSumUnit{}))) = True
isUnitValue _ = False

prettyNode :: Node -> M (Doc SyntaxClass)
prettyNode Node{..}
  | Nothing <- nodeNode =
      pure "<missing node>"

  | Just node <- nodeNode = do
      ppNode <- prettyNodeNode node
      let ppConsumedBy =
              maybe mempty
                (\nodeId -> meta $ archivedSC $ text "consumed by:" <-> prettyNodeIdLink nodeId)
                nodeConsumedBy

      let ppReferencedBy =
            if V.null nodeReferencedBy
            then mempty
            else meta $ keyword_ "referenced by"
                   <-> fcommasep (mapV prettyNodeIdLink nodeReferencedBy)

      let ppDisclosedTo =
            if V.null nodeObservingSince
            then mempty
            else
              meta $ keyword_ "known to (since):"
                <-> fcommasep
                  (mapV
                    (\(PartyAndTransactionId p txId) -> prettyMayParty p <-> parens (prettyTxId txId))
                    nodeObservingSince)

      pure
         $ prettyMay "<missing node id>" prettyNodeId nodeNodeId
        $$ vcat
             [ ppConsumedBy, ppReferencedBy, ppDisclosedTo
             , arrowright ppNode
             ]

  where
    arrowright p = text "└─>" <-> p
    meta p       = text "│  " <-> p
    archivedSC = annotateSC PredicateSC -- Magenta


prettyPartialTransaction :: PartialTransaction -> M (Doc SyntaxClass)
prettyPartialTransaction PartialTransaction{..} = do
  world <- askWorld
  let ppNodes =
           runM partialTransactionNodes world
         $ fmap vsep
         $ mapM (lookupNode >=> prettyNode)
                (V.toList partialTransactionRoots)
  pure $ vcat
    [ case partialTransactionExerciseContext of
        Nothing -> mempty
        Just ExerciseContext{..} ->
          text "Failed exercise"
            <-> parens (prettyMayLocation world exerciseContextExerciseLocation) <> ":"
            $$ nest 2 (
                keyword_ "exercise"
            <-> prettyMay "<missing template id>"
                  (\tid ->
                      prettyChoiceId world tid exerciseContextChoiceId)
                  (contractRefTemplateId <$> exerciseContextTargetId)
            <-> keyword_ "on"
            <-> prettyMay "<missing>"
                  (prettyContractRef world)
                  exerciseContextTargetId
             $$ keyword_ "with"
             $$ ( nest 2
                $ prettyMay "<missing>"
                    (prettyValue' False 0 world)
                    exerciseContextChosenValue)
            )

   , if V.null partialTransactionRoots
     then mempty
     else text "Sub-transactions:" $$ nest 3 ppNodes
   ]


prettyValue' :: Bool -> Int -> LF.World -> Value -> Doc SyntaxClass
prettyValue' _ _ _ (Value Nothing) = text "<missing value>"
prettyValue' showRecordType prec world (Value (Just vsum)) = case vsum of
  ValueSumRecord (Record mbRecordId fields) ->
    maybeParens (prec > precWith) $
      (if showRecordType
       then \fs -> prettyMay "" (prettyDefName world) mbRecordId <-> keyword_ "with" $$ nest 2 fs
       else id)
      (sep (punctuate ";" (mapV prettyField fields)))
  ValueSumTuple (Tuple fields) ->
    braces (sep (punctuate ";" (mapV prettyField fields)))
  ValueSumVariant (Variant mbVariantId ctor mbValue) ->
        prettyMay "" (\v -> prettyDefName world v <> ":") mbVariantId <> ltext ctor
    <-> prettyMay "<missing value>" (prettyValue' True precHighest world) mbValue
  ValueSumList (List elems) ->
    brackets (fcommasep (mapV (prettyValue' True prec world) elems))
  ValueSumContractId coid -> prettyContractId coid
  ValueSumInt64 i -> string (show i)
  ValueSumDecimal ds ->
    case preview stringToDecimal (TL.unpack ds) of
      Nothing -> ltext ds
      Just d  -> string $ review stringToDecimal d
  ValueSumText t -> char '"' <> ltext t <> char '"'
  ValueSumTimestamp ts -> prettyTimestamp ts
  ValueSumParty p -> char '\'' <> ltext p <> char '\''
  ValueSumBool True -> text "true"
  ValueSumBool False -> text "false"
  ValueSumUnit{} -> text "{}"
  ValueSumDate d -> prettyDate d
  ValueSumOptional (Optional Nothing) -> text "none"
  ValueSumOptional (Optional (Just v)) -> "some " <> prettyValue' True precHighest world v
  ValueSumMap (Map entries) -> "Map" <> brackets (fcommasep (mapV (prettyEntry prec world) entries))
  ValueSumUnserializable what -> ltext what
  where
    prettyField (Field label mbValue) =
      hang (ltext label <-> "=") 2
        (prettyMay "<missing value>" (prettyValue' True precHighest world) mbValue)
    precWith = 1
    precHighest = 9

prettyEntry :: Int -> LF.World ->  Map_Entry -> Doc SyntaxClass
prettyEntry prec world (Map_Entry key (Just value)) =
   ltext key <> "->" <> prettyValue' True prec world value
prettyEntry  _ _ (Map_Entry key _) =
   ltext key <> "-> <missing value>"

prettyDate :: Int32 -> Doc a
prettyDate =
    string
  . TF.formatTime TF.defaultTimeLocale "%FT"
  . CP.posixSecondsToUTCTime
  . (24*60*60*)
  . fromIntegral


prettyPackageIdentifier :: PackageIdentifier -> Doc SyntaxClass
prettyPackageIdentifier (PackageIdentifier psum) = case psum of
  Nothing                                    -> mempty
  (Just (PackageIdentifierSumSelf _))        -> mempty
  (Just (PackageIdentifierSumPackageId pid)) -> char '@' <> ltext pid

prettyDefName :: LF.World -> Identifier -> Doc SyntaxClass
prettyDefName world (Identifier mbPkgId (TL.toStrict -> qualName))
  | Just (_modName, defName, mod0) <- lookupModuleFromQualifiedName world mbPkgId qualName
  , Just fp <- LF.moduleSource mod0
  , Just (LF.SourceLoc _mref sline _scol eline _ecol) <- lookupDefLocation mod0 defName =
      linkSC (revealLocationUri fp sline eline) name ppName
  | otherwise =
      ppName
  where
    name = qualName
    ppName = text name <> ppPkgId
    ppPkgId = maybe mempty prettyPackageIdentifier mbPkgId

prettyChoiceId
  :: LF.World -> Maybe Identifier -> TL.Text
  -> Doc SyntaxClass
prettyChoiceId _ Nothing choiceId = ltext choiceId
prettyChoiceId world (Just (Identifier mbPkgId (TL.toStrict -> qualName))) (TL.toStrict -> choiceId)
  | Just (_modName, defName, mod0) <- lookupModuleFromQualifiedName world mbPkgId qualName
  , Just fp <- LF.moduleSource mod0
  , Just tpl <- NM.lookup (Tagged [defName]) (LF.moduleTemplates mod0)
  , Just chc <- NM.lookup (Tagged choiceId) (LF.tplChoices tpl)
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

prettyContractRef :: LF.World -> ContractRef -> Doc SyntaxClass
prettyContractRef world (ContractRef _relative coid tid) =
  hsep
  [ prettyContractId coid
  , parens (prettyMay "<missing template id>" (prettyDefName world) tid)
  ]


-- TABLE VIEW

data NodeInfo = NodeInfo
    { niTemplateId :: Identifier
    , niNodeId :: NodeId
    , niValue :: Value
    , niActive :: Bool
    , niObservers :: S.Set T.Text
    }

data Table = Table
    { tTemplateId :: Identifier
    , tRows :: [NodeInfo]
    }

nodeInfo :: Node -> Maybe NodeInfo
nodeInfo Node{..} = do
    NodeNodeCreate create <- nodeNode
    niNodeId <- nodeNodeId
    inst <- node_CreateContractInstance create
    niTemplateId <- contractInstanceTemplateId inst
    niValue <- contractInstanceValue inst
    let niActive = isNothing nodeConsumedBy
    let niObservers = S.fromList $ mapMaybe party $ V.toList nodeObservingSince
    pure NodeInfo{..}
    where
        party :: PartyAndTransactionId -> Maybe T.Text
        party PartyAndTransactionId{..} = do
            Party{..} <- partyAndTransactionIdParty
            pure (TL.toStrict partyParty)


groupTables :: [NodeInfo] -> [Table]
groupTables =
    map (uncurry Table)
    . MS.toList
    . MS.map (sortOn (parseNodeId . niNodeId))
    . MS.fromListWith (++)
    . map (\node -> (niTemplateId node, [node]))

renderValue :: LF.World -> [T.Text] -> Value -> (H.Html, H.Html)
renderValue world name = \case
    Value (Just (ValueSumRecord (Record _ fields))) ->
        let (ths, tds) = unzip $ map renderField (V.toList fields)
        in (mconcat ths, mconcat tds)
    value ->
        let th = H.th $ H.text $ T.intercalate "." name
            td = H.td $ H.text $ renderPlain $ prettyValue' True 0 world value
        in (th, td)
    where
        renderField (Field label mbValue) =
            renderValue world (name ++ [TL.toStrict label]) (fromJust mbValue)

renderRow :: LF.World -> S.Set T.Text -> NodeInfo -> (H.Html, H.Html)
renderRow world parties NodeInfo{..} =
    let (ths, tds) = renderValue world [] niValue
        header = H.tr $ mconcat
            [ foldMap (H.th . (H.div H.! A.class_ "observer") . H.text) parties
            , H.th "id"
            , H.th "status"
            , ths
            ]
        observed party = if party `S.member` niObservers then "X" else "-"
        active = if niActive then "active" else "archived"
        row = H.tr H.! A.class_ (H.textValue active) $ mconcat
            [ foldMap ((H.td H.! A.class_ "disclosure") . H.text . observed) parties
            , H.td (H.text $ renderPlain $ prettyNodeId niNodeId)
            , H.td (H.text active)
            , tds
            ]
    in (header, row)

-- TODO(MH): The header should be rendered from the type rather than from the
-- first value.
renderTable :: LF.World -> Table -> H.Html
renderTable world Table{..} = H.div H.! A.class_ active $ do
    let parties = S.unions $ map niObservers tRows
    H.h1 $ renderPlain $ prettyDefName world tTemplateId
    let (headers, rows) = unzip $ map (renderRow world parties) tRows
    H.table $ head headers <> mconcat rows
    where
        active = if any niActive tRows then "active" else "archived"

renderTableView :: LF.World -> ScenarioResult -> Maybe H.Html
renderTableView world ScenarioResult{..} =
    let nodes = mapMaybe nodeInfo (V.toList scenarioResultNodes)
        tables = groupTables nodes
    in if null nodes then Nothing else Just $ H.div H.! A.class_ "table" $ foldMap (renderTable world) tables

renderTransactionView :: LF.World -> ScenarioResult -> H.Html
renderTransactionView world res =
    let doc = prettyScenarioResult world res
    in H.div H.! A.class_ "da-code transaction" $ Pretty.renderHtml 128 doc

renderScenarioResult :: LF.World -> ScenarioResult -> T.Text
renderScenarioResult world res = TL.toStrict $ Blaze.renderHtml $ do
    H.docTypeHtml $ do
        H.head $ do
            H.style $ H.text Pretty.highlightStylesheet
            H.style $ H.text stylesheet
            H.script $ H.text javascript
        let tableView = renderTableView world res
        let transView = renderTransactionView world res
        case tableView of
            Nothing -> H.body transView
            Just tbl -> H.body H.! A.class_ "hide_archived hide_transaction" $ do
                H.div $ do
                    H.button H.! A.onclick "toggle_view();" $ do
                        H.span H.! A.class_ "table" $ H.text "Show transaction view"
                        H.span H.! A.class_ "transaction" $ H.text "Show table view"
                    H.text " "
                    H.span H.! A.class_ "table" $ do
                        H.input H.! A.type_ "checkbox" H.! A.id "show_archived" H.! A.onchange "show_archived_changed();"
                        H.label H.! A.for "show_archived" $ "Show archived"
                tbl
                transView

javascript :: T.Text
javascript = T.unlines
  [ "function show_archived_changed() {"
  , "  document.body.classList.toggle('hide_archived', !document.getElementById('show_archived').checked);"
  , "}"
  , "function toggle_view() {"
  , "  document.body.classList.toggle('hide_transaction');"
  , "  document.body.classList.toggle('hide_table');"
  , "}"
  ]

stylesheet :: T.Text
stylesheet = T.unlines
  [ "table, th, td {"
  , "  border: 1px solid;"
  , "  border-collapse: collapse;"
  , "}"
  , "th, td {"
  , "  padding-left: 2px;"
  , "  padding-right: 2px;"
  , "}"
  , "body.hide_archived .archived {"
  , "  display: none;"
  , "}"
  , "body.hide_table .table {"
  , "  display: none;"
  , "}"
  , "body.hide_transaction .transaction {"
  , "  display: none;"
  , "}"
  , "tr.archived td {"
  , "  text-decoration: line-through;"
  , "}"
  , "td.disclosure {"
  , "  max-width: 1em;"
  , "  text-align: center;"
  , "}"
  , "tr.archived td.disclosure {"
  , "  text-decoration: none;"
  , "}"
  , "th {"
  , "  vertical-align: bottom;"
  , "}"
  , "div.observer {"
  , "  max-width: 1em;"
  , "  writing-mode: vertical-rl;"
  , "  transform: rotate(180deg);"
  , "}"
  ]
