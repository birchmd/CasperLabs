package io.casperlabs.casper

import cats.{Applicative, Monad}
import cats.data.EitherT
import cats.effect.Sync
import cats.effect.concurrent.{Ref, Semaphore}
import cats.implicits._
import cats.mtl.FunctorRaise
import com.google.protobuf.ByteString
import io.casperlabs.blockstorage.{BlockDagRepresentation, BlockDagStorage, BlockStore}
import io.casperlabs.casper.Estimator.BlockHash
import io.casperlabs.casper.Validate.ValidateErrorWrapper
import io.casperlabs.casper.protocol._
import io.casperlabs.casper.util.ProtoUtil._
import io.casperlabs.casper.util._
import io.casperlabs.casper.util.comm.CommUtil
import io.casperlabs.casper.util.execengine.ExecEngineUtil.StateHash
import io.casperlabs.casper.util.execengine.{DeploysCheckpoint, ExecEngineUtil}
import io.casperlabs.catscontrib._
import io.casperlabs.comm.CommError.ErrorHandler
import io.casperlabs.comm.rp.Connect.{ConnectionsCell, RPConfAsk}
import io.casperlabs.comm.transport.TransportLayer
import io.casperlabs.comm.gossiping
import io.casperlabs.crypto.codec.Base16
import io.casperlabs.ipc
import io.casperlabs.ipc.{ProtocolVersion, ValidateRequest}
import io.casperlabs.shared._
import io.casperlabs.smartcontracts.ExecutionEngineService
import io.casperlabs.storage.BlockMsgWithTransform

/**
  Encapsulates mutable state of the MultiParentCasperImpl

  @param seenBlockHashes - tracks hashes of all blocks seen so far
  @param blockBuffer
  @param deployBuffer
  @param invalidBlockTracker
  @param equivocationsTracker: Used to keep track of when other validators detect the equivocation consisting of the base block at the sequence number identified by the (validator, base equivocation sequence number) pair of each EquivocationRecord.
  */
final case class CasperState(
    seenBlockHashes: Set[BlockHash] = Set.empty[BlockHash],
    blockBuffer: Set[BlockMessage] = Set.empty[BlockMessage],
    deployBuffer: Set[DeployData] = Set.empty[DeployData],
    invalidBlockTracker: Set[BlockHash] = Set.empty[BlockHash],
    dependencyDag: DoublyLinkedDag[BlockHash] = BlockDependencyDag.empty,
    equivocationsTracker: Set[EquivocationRecord] = Set.empty[EquivocationRecord]
)

class MultiParentCasperImpl[F[_]: Sync: Log: Time: SafetyOracle: BlockStore: BlockDagStorage: ExecutionEngineService](
    statelessExecutor: MultiParentCasperImpl.StatelessExecutor[F],
    broadcaster: MultiParentCasperImpl.Broadcaster[F],
    validatorId: Option[ValidatorIdentity],
    genesis: BlockMessage,
    shardId: String,
    blockProcessingLock: Semaphore[F],
    faultToleranceThreshold: Float = 0f
)(implicit state: Cell[F, CasperState])
    extends MultiParentCasper[F] {

  import MultiParentCasperImpl._

  private implicit val logSource: LogSource = LogSource(this.getClass)

  implicit val functorRaiseInvalidBlock = Validate.raiseValidateErrorThroughSync[F]

  type Validator = ByteString

  private val lastFinalizedBlockHashContainer = Ref.unsafe[F, BlockHash](genesis.blockHash)

  /** Add a block if it hasn't been added yet. */
  def addBlock(
      block: BlockMessage
  ): F[BlockStatus] =
    Sync[F].bracket(blockProcessingLock.acquire)(
      _ =>
        for {
          dag            <- blockDag
          blockHash      = block.blockHash
          containsResult <- dag.contains(blockHash)
          casperState    <- Cell[F, CasperState].read
          attempts <- if (containsResult || casperState.seenBlockHashes
                            .contains(blockHash)) {
                       Log[F]
                         .info(
                           s"Block ${PrettyPrinter.buildString(blockHash)} has already been processed by another thread."
                         )
                         .map(
                           _ =>
                             List(
                               block -> BlockStatus.processing
                             )
                         )
                     } else {
                       Cell[F, CasperState].modify { s =>
                         s.copy(
                           seenBlockHashes = s.seenBlockHashes + blockHash
                         )
                       } *> internalAddBlock(block, dag)
                     }
          // TODO: Ideally this method would just return the block hashes it created,
          // but for now it does gossiping as well. The methods return the full blocks
          // because for missing blocks it's not yet saved to the database.
          _ <- attempts.traverse {
                case (attemptedBlock, status) =>
                  broadcaster.networkEffects(attemptedBlock, status)
              }
        } yield attempts.head._2
    )(_ => blockProcessingLock.release)

  /** Validate the block, try to execute and store it,
    * execute any other block that depended on it,
    * update the finalized block reference. */
  private def internalAddBlock(
      block: BlockMessage,
      dag: BlockDagRepresentation[F]
  ): F[List[(BlockMessage, BlockStatus)]] =
    for {
      lastFinalizedBlockHash <- lastFinalizedBlockHashContainer.get
      attemptResult <- statelessExecutor.validateAndAddBlock(
                        StatelessExecutor.Context(genesis, lastFinalizedBlockHash).some,
                        dag,
                        block
                      )
      (status, updatedDag) = attemptResult
      _                    <- removeAdded(List(block -> status), canRemove = _ != MissingBlocks)
      furtherAttempts <- status match {
                          case MissingBlocks           => List.empty.pure[F]
                          case IgnorableEquivocation   => List.empty.pure[F]
                          case InvalidUnslashableBlock => List.empty.pure[F]
                          case _ =>
                            reAttemptBuffer(updatedDag, lastFinalizedBlockHash) // reAttempt for any status that resulted in the adding of the block into the view
                        }
      tipHashes <- estimator(updatedDag)
      _ <- Log[F].debug(
            s"Tip estimates: ${tipHashes.map(PrettyPrinter.buildString).mkString(", ")}"
          )
      tipHash                       = tipHashes.head
      _                             <- Log[F].info(s"New fork-choice tip is block ${PrettyPrinter.buildString(tipHash)}.")
      lastFinalizedBlockHash        <- lastFinalizedBlockHashContainer.get
      updatedLastFinalizedBlockHash <- updateLastFinalizedBlock(updatedDag, lastFinalizedBlockHash)
      _                             <- lastFinalizedBlockHashContainer.set(updatedLastFinalizedBlockHash)
      _ <- Log[F].info(
            s"New last finalized block hash is ${PrettyPrinter.buildString(updatedLastFinalizedBlockHash)}."
          )
    } yield (block, status) :: furtherAttempts

  /** Go from the last finalized block and visit all children that can be finalized now.
    * Remove all of the deploys that are in any of them as they won't have to be attempted again. */
  private def updateLastFinalizedBlock(
      dag: BlockDagRepresentation[F],
      lastFinalizedBlockHash: BlockHash
  ): F[BlockHash] =
    for {
      childrenHashes <- dag
                         .children(lastFinalizedBlockHash)
                         .map(_.getOrElse(Set.empty[BlockHash]).toList)
      // Find all finalized children so that we can get rid of their deploys.
      finalizedChildren <- ListContrib.filterM(
                            childrenHashes,
                            (blockHash: BlockHash) =>
                              isGreaterThanFaultToleranceThreshold(dag, blockHash)
                          )
      newFinalizedBlock <- if (finalizedChildren.isEmpty) {
                            lastFinalizedBlockHash.pure[F]
                          } else {
                            finalizedChildren.traverse { childHash =>
                              for {
                                removed <- removeDeploysInBlock(childHash)
                                _ <- Log[F].info(
                                      s"Removed $removed deploys from deploy history as we finalized block ${PrettyPrinter
                                        .buildString(childHash)}."
                                    )
                                finalizedHash <- updateLastFinalizedBlock(dag, childHash)
                              } yield finalizedHash
                            } map (_.head)
                          }
    } yield newFinalizedBlock

  /** Remove deploys from the history which are included in a just finalised block. */
  private def removeDeploysInBlock(blockHash: BlockHash): F[Int] =
    for {
      block              <- ProtoUtil.unsafeGetBlock[F](blockHash)
      deploysToRemove    = block.body.get.deploys.map(_.deploy.get).toSet
      stateBefore        <- Cell[F, CasperState].read
      initialHistorySize = stateBefore.deployBuffer.size
      _ <- Cell[F, CasperState].modify { s =>
            s.copy(deployBuffer = s.deployBuffer.filterNot(deploysToRemove))
          }
      stateAfter     <- Cell[F, CasperState].read
      deploysRemoved = initialHistorySize - stateAfter.deployBuffer.size
    } yield deploysRemoved

  /*
   * On the first pass, block B is finalized if B's main parent block is finalized
   * and the safety oracle says B's normalized fault tolerance is above the threshold.
   * On the second pass, block B is finalized if any of B's children blocks are finalized.
   *
   * TODO: Implement the second pass in BlockAPI
   */
  private def isGreaterThanFaultToleranceThreshold(
      dag: BlockDagRepresentation[F],
      blockHash: BlockHash
  ): F[Boolean] =
    for {
      faultTolerance <- SafetyOracle[F].normalizedFaultTolerance(dag, blockHash)
      _ <- Log[F].info(
            s"Fault tolerance for block ${PrettyPrinter.buildString(blockHash)} is $faultTolerance; threshold is $faultToleranceThreshold"
          )
    } yield faultTolerance > faultToleranceThreshold

  /** Check that either we have the block already scheduled but missing dependencies, or it's in the store */
  def contains(
      block: BlockMessage
  ): F[Boolean] =
    Cell[F, CasperState].read
      .map(_.blockBuffer.exists(_.blockHash == block.blockHash))
      .ifM(
        true.pure[F],
        BlockStore[F].contains(block.blockHash)
      )

  /** Add a deploy to the buffer, if the code passes basic validation. */
  def deploy(deployData: DeployData): F[Either[Throwable, Unit]] =
    (deployData.session, deployData.payment) match {
      //TODO: verify sig immediately (again, so we fail fast)
      case (Some(session), Some(payment)) =>
        val req = ExecutionEngineService[F].verifyWasm(ValidateRequest(session.code, payment.code))

        EitherT(req)
          .leftMap(c => new IllegalArgumentException(s"Contract verification failed: $c"))
          .flatMapF(_ => addDeploy(deployData) map (_.asRight[Throwable]))
          .value
      // TODO: Genesis doesn't have payment code; does it come here?
      case (None, _) | (_, None) =>
        Either
          .left[Throwable, Unit](
            // TODO: Use IllegalArgument from comms.
            new IllegalArgumentException(s"Deploy was missing session and/or payment code.")
          )
          .pure[F]
    }

  /** Add a deploy to the buffer, to be executed later. */
  private def addDeploy(deployData: DeployData): F[Unit] =
    Cell[F, CasperState].modify { s =>
      s.copy(deployBuffer = s.deployBuffer + deployData)
    } *> Log[F].info(s"Received ${PrettyPrinter.buildString(deployData)}")

  /** Return the list of tips. */
  def estimator(dag: BlockDagRepresentation[F]): F[IndexedSeq[BlockHash]] =
    for {
      lastFinalizedBlockHash <- lastFinalizedBlockHashContainer.get
      rankedEstimates        <- Estimator.tips[F](dag, lastFinalizedBlockHash)
    } yield rankedEstimates

  /*
   * Logic:
   *  -Score each of the blockDAG heads extracted from the block messages via GHOST
   *  -Let P = subset of heads such that P contains no conflicts and the total score is maximized
   *  -Let R = subset of deploy messages which are not included in DAG obtained by following blocks in P
   *  -If R is non-empty then create a new block with parents equal to P and (non-conflicting) txns obtained from R
   *  -Else if R is empty and |P| > 1 then create a block with parents equal to P and no transactions
   *  -Else None
   *
   *  TODO: Make this return Either so that we get more information about why not block was
   *  produced (no deploys, already processing, no validator id)
   */
  def createBlock: F[CreateBlockStatus] = validatorId match {
    case Some(ValidatorIdentity(publicKey, privateKey, sigAlgorithm)) =>
      for {
        dag       <- blockDag
        tipHashes <- dag.latestMessageHashes.flatMap(MultiParentCasperImpl.tempEstimator(_, dag))
        tips      <- tipHashes.traverse(ProtoUtil.unsafeGetBlock[F])
        merged    <- ExecEngineUtil.merge[F](tips, dag)
        parents   = merged.parents
        _ <- Log[F].info(
              s"${parents.size} parents out of ${tipHashes.size} latest blocks will be used."
            )
        remaining        <- remainingDeploys(dag, parents)
        bondedValidators = bonds(parents.head).map(_.validator).toSet
        //We ensure that only the justifications given in the block are those
        //which are bonded validators in the chosen parent. This is safe because
        //any latest message not from a bonded validator will not change the
        //final fork-choice.
        latestMessages <- dag.latestMessages
        justifications = toJustification(latestMessages)
          .filter(j => bondedValidators.contains(j.validator))
        maxBlockNumber = parents.foldLeft(-1L) {
          case (acc, b) => math.max(acc, blockNumber(b))
        }
        number          = maxBlockNumber + 1
        protocolVersion = CasperLabsProtocolVersions.thresholdsVersionMap.versionAt(number)
        proposal <- if (remaining.nonEmpty || parents.length > 1) {
                     createProposal(
                       dag,
                       parents,
                       merged,
                       remaining,
                       justifications,
                       protocolVersion
                     )
                   } else {
                     CreateBlockStatus.noNewDeploys.pure[F]
                   }
        signedBlock <- proposal match {
                        case Created(blockMessage) =>
                          signBlock(blockMessage, dag, publicKey, privateKey, sigAlgorithm, shardId)
                            .map(Created.apply)
                        case _ => proposal.pure[F]
                      }
      } yield signedBlock
    case None => CreateBlockStatus.readOnlyMode.pure[F]
  }

  def lastFinalizedBlock: F[BlockMessage] =
    for {
      lastFinalizedBlockHash <- lastFinalizedBlockHashContainer.get
      blockMessage           <- ProtoUtil.unsafeGetBlock[F](lastFinalizedBlockHash)
    } yield blockMessage

  // TODO: Optimize for large number of deploys accumulated over history
  /** Get the deploys that are not present in the past of the chosen parents. */
  private def remainingDeploys(
      dag: BlockDagRepresentation[F],
      parents: Seq[BlockMessage]
  ): F[Seq[DeployData]] =
    for {
      state <- Cell[F, CasperState].read
      hist  = state.deployBuffer
      unprocessed <- DagOperations
                      .bfTraverseF[F, BlockMessage](parents.toList)(ProtoUtil.unsafeGetParents[F])
                      .foldWhileLeft(state.deployBuffer) {
                        case (deployPool, block) =>
                          val processedDeploys = block.getBody.deploys.flatMap(_.deploy)
                          val remDeploys       = deployPool -- processedDeploys
                          if (remDeploys.nonEmpty) Left(remDeploys) else Right(Set.empty)
                      }
    } yield unprocessed.toSeq

  //TODO: Need to specify SEQ vs PAR type block?
  /** Execute a set of deploys in the context of chosen parents. Compile them into a block if everything goes fine. */
  private def createProposal(
      dag: BlockDagRepresentation[F],
      parents: Seq[BlockMessage],
      merged: ExecEngineUtil.MergeResult[ExecEngineUtil.TransformMap, BlockMessage],
      deploys: Seq[DeployData],
      justifications: Seq[Justification],
      protocolVersion: ProtocolVersion
  ): F[CreateBlockStatus] =
    (for {
      now <- Time[F].currentMillis
      s   <- Cell[F, CasperState].read
      stateResult <- ExecEngineUtil
                      .computeDeploysCheckpoint[F](
                        merged,
                        deploys,
                        protocolVersion
                      )
      DeploysCheckpoint(preStateHash, postStateHash, deploysForBlock, number, protocolVersion) = stateResult
      //TODO: compute bonds properly
      newBonds = ProtoUtil.bonds(parents.head)
      postState = RChainState()
        .withPreStateHash(preStateHash)
        .withPostStateHash(postStateHash)
        .withBonds(newBonds)
        .withBlockNumber(number)

      body = Body()
        .withState(postState)
        .withDeploys(deploysForBlock)
      header = blockHeader(body, parents.map(_.blockHash), protocolVersion.version, now)
      block  = unsignedBlockProto(body, header, justifications, shardId)
    } yield CreateBlockStatus.created(block)).handleErrorWith(
      ex =>
        Log[F]
          .error(
            s"Critical error encountered while processing deploys: ${ex.getMessage}"
          )
          .map(_ => CreateBlockStatus.internalDeployError(ex))
    )

  // MultiParentCasper Exposes the block DAG to those who need it.
  def blockDag: F[BlockDagRepresentation[F]] =
    BlockDagStorage[F].getRepresentation

  // RChain used to return the whole database as a String for testing.
  def storageContents(hash: StateHash): F[String] =
    """""".pure[F]

  def normalizedInitialFault(weights: Map[Validator, Long]): F[Float] =
    for {
      state   <- Cell[F, CasperState].read
      tracker = state.equivocationsTracker
    } yield
      (tracker
        .map(_.equivocator)
        .flatMap(weights.get)
        .sum
        .toFloat / weightMapTotal(weights))

  /** After a block is executed we can try to execute the other blocks in the buffer that dependend on it. */
  private def reAttemptBuffer(
      dag: BlockDagRepresentation[F],
      lastFinalizedBlockHash: BlockHash
  ): F[List[(BlockMessage, BlockStatus)]] =
    for {
      casperState    <- Cell[F, CasperState].read
      dependencyFree = casperState.dependencyDag.dependencyFree
      dependencyFreeBlocks = casperState.blockBuffer
        .filter(block => dependencyFree.contains(block.blockHash))
        .toList
      dependencyFreeAttempts <- dependencyFreeBlocks.foldM(
                                 (
                                   List.empty[(BlockMessage, BlockStatus)],
                                   dag
                                 )
                               ) {
                                 case ((attempts, updatedDag), block) =>
                                   for {
                                     attempt <- statelessExecutor.attemptAdd(
                                                 StatelessExecutor
                                                   .Context(genesis, lastFinalizedBlockHash)
                                                   .some,
                                                 updatedDag,
                                                 block,
                                                 treatAsGenesis = false
                                               )
                                     (status, updatedDag) = attempt
                                   } yield ((block, status) :: attempts, updatedDag)
                               }
      (attempts, updatedDag) = dependencyFreeAttempts
      furtherAttempts <- if (attempts.isEmpty) List.empty.pure[F]
                        else {
                          removeAdded(attempts, canRemove = _.inDag) *>
                            reAttemptBuffer(updatedDag, lastFinalizedBlockHash)
                        }
    } yield attempts ++ furtherAttempts

  /** Remove all the blocks that were successfully added from the block buffer and the dependency DAG. */
  private def removeAdded(
      attempts: List[(BlockMessage, BlockStatus)],
      canRemove: BlockStatus => Boolean
  ): F[Unit] = {
    val addedBlockHashes = attempts.collect {
      case (block, status) if canRemove(status) => block.blockHash
    }.toSet

    Cell[F, CasperState].modify { s =>
      s.copy(
        blockBuffer = s.blockBuffer.filterNot(addedBlockHashes contains _.blockHash),
        dependencyDag = addedBlockHashes.foldLeft(s.dependencyDag) {
          case (dag, blockHash) =>
            DoublyLinkedDagOperations.remove(dag, blockHash)
        }
      )
    }
  }

  /** Called periodically from outside to ask all peers again
    * to send us blocks for which we are missing some dependencies. */
  def fetchDependencies: F[Unit] =
    for {
      s <- Cell[F, CasperState].read
      _ <- s.dependencyDag.dependencyFree.toList.traverse(broadcaster.requestMissingDependency(_))
    } yield ()

  /** The new gossiping first syncs the missing DAG, then downloads and adds the blocks in topological order.
    * However the EquivocationDetector wants to know about dependencies so it can assign different statuses,
    * so we'll make the synchronized DAG known via a partial block message, so any missing dependencies can
    * be tracked, i.e. Casper will know about the pending graph.  */
  def addMissingDependencies(block: BlockMessage): F[Unit] =
    for {
      dag <- blockDag
      _   <- statelessExecutor.addMissingDependencies(block, dag)
    } yield ()
}

object MultiParentCasperImpl {
  import io.casperlabs.casper.Estimator.{BlockHash, Validator}
  def tempEstimator[F[_]: Monad](
      tipHashes: Map[Validator, BlockHash],
      dag: BlockDagRepresentation[F]
  ): F[Vector[BlockHash]] =
    for {
      tips <- tipHashes.values.toVector.distinct
               .sortBy(PrettyPrinter.buildStringNoLimit)
               .traverse(dag.lookup)
               .map(_.flatten)
      order    <- dag.deriveOrdering(0L)
      unc      <- DagOperations.uncommonAncestors[F](tips, dag)(Monad[F], order)
      filtered = tips.filter(unc.contains)
    } yield filtered.map(_.blockHash)

  // TODO: Mateusz will move this to its own place.
  val version = 1L

  /** Component purely to validate, execute and store blocks.
    * Even the Genesis, to create it in the first place. */
  class StatelessExecutor[F[_]: Sync: Time: Log: BlockStore: BlockDagStorage: ExecutionEngineService](
      shardId: String
  ) {

    implicit val functorRaiseInvalidBlock = Validate.raiseValidateErrorThroughSync[F]

    def validateAndAddBlock(
        maybeContext: Option[StatelessExecutor.Context],
        dag: BlockDagRepresentation[F],
        block: BlockMessage
    )(implicit state: Cell[F, CasperState]): F[(BlockStatus, BlockDagRepresentation[F])] = {
      val treatAsGenesis =
        block.getHeader.parentsHashList.isEmpty &&
          block.sender.isEmpty &&
          block.sig.isEmpty

      for {
        dag         <- BlockDagStorage[F].getRepresentation
        validFormat <- Validate.formatOfFields[F](block, treatAsGenesis)
        validSig    <- if (!treatAsGenesis) Validate.blockSignature[F](block) else true.pure[F]
        validSender <- maybeContext.map { ctx =>
                        Validate.blockSender[F](block, ctx.genesis, dag)
                      } getOrElse (treatAsGenesis).pure[F]
        validVersion <- Validate.version[F](
                         block,
                         CasperLabsProtocolVersions.thresholdsVersionMap.versionAt
                       )
        attemptResult <- if (!validFormat) (InvalidUnslashableBlock, dag).pure[F]
                        else if (!validSig) (InvalidUnslashableBlock, dag).pure[F]
                        else if (!validSender) (InvalidUnslashableBlock, dag).pure[F]
                        else if (!validVersion) (InvalidUnslashableBlock, dag).pure[F]
                        else attemptAdd(maybeContext, dag, block, treatAsGenesis)
        (status, updatedDag) = attemptResult
      } yield (status, updatedDag)
    }
    /* Execute the block to get the effects then do some more validation.
     * Save the block if everything checks out.
     * We want to catch equivocations only after we confirm that the block completing
     * the equivocation is otherwise valid. */
    def attemptAdd(
        maybeContext: Option[StatelessExecutor.Context],
        dag: BlockDagRepresentation[F],
        block: BlockMessage,
        treatAsGenesis: Boolean
    )(implicit state: Cell[F, CasperState]): F[(BlockStatus, BlockDagRepresentation[F])] = {
      val validationStatus = (for {
        _ <- Log[F].info(
              s"Attempting to add Block ${PrettyPrinter.buildString(block.blockHash)} to DAG."
            )
        postValidationStatus <- maybeContext map { ctx =>
                                 Validate.blockSummary[F](
                                   block,
                                   ctx.genesis,
                                   dag,
                                   shardId,
                                   ctx.lastFinalizedBlockHash
                                 )
                               } getOrElse {
                                 Validate
                                   .blockSummaryPreGenesis[F](block, dag, shardId, treatAsGenesis)
                               }
        casperState <- Cell[F, CasperState].read
        // Confirm the parents are correct (including checking they commute) and capture
        // the effect needed to compute the correct pre-state as well.
        merged <- maybeContext.fold(
                   ExecEngineUtil.MergeResult
                     .empty[ExecEngineUtil.TransformMap, BlockMessage]
                     .pure[F]
                 ) { ctx =>
                   Validate.parents[F](block, ctx.lastFinalizedBlockHash, dag)
                 }
        preStateHash <- ExecEngineUtil.computePrestate[F](merged)
        blockEffects <- ExecEngineUtil
                         .effectsForBlock[F](block, preStateHash, dag)
                         .recoverWith {
                           case _ => FunctorRaise[F, InvalidBlock].raise(InvalidTransaction)
                         }
        _ <- Validate.transactions[F](
              block,
              dag,
              preStateHash,
              blockEffects
            )
        _ <- maybeContext.fold(().pure[F]) { ctx =>
              Validate.bondsCache[F](block, ProtoUtil.bonds(ctx.genesis)) >>
                EquivocationDetector
                  .checkNeglectedEquivocationsWithUpdate[F](
                    block,
                    dag,
                    ctx.genesis
                  )
            }
        _ <- Validate
              .neglectedInvalidBlock[F](
                block,
                casperState.invalidBlockTracker
              )
        _ <- EquivocationDetector.checkEquivocations[F](casperState.dependencyDag, block, dag)
      } yield blockEffects).attempt

      validationStatus.flatMap {
        case Right(effects) =>
          addEffects(Valid, block, effects, dag).tupleLeft(Valid)
        case Left(ValidateErrorWrapper(invalid)) =>
          addEffects(invalid, block, Seq.empty, dag)
            .tupleLeft(invalid)
        case Left(unexpected) =>
          for {
            _ <- Log[F].error(
                  s"Unexpected exception during validation of the block ${PrettyPrinter
                    .buildString(block.blockHash)}",
                  unexpected
                )
            _ <- Sync[F].raiseError[BlockStatus](unexpected)
          } yield (BlockException(unexpected), dag)
      }
    }

    // TODO: Handle slashing
    /** Either store the block with its transformation,
      * or add it to the buffer in case the dependencies are missing. */
    private def addEffects(
        status: BlockStatus,
        block: BlockMessage,
        transforms: Seq[ipc.TransformEntry],
        dag: BlockDagRepresentation[F]
    )(implicit state: Cell[F, CasperState]): F[BlockDagRepresentation[F]] =
      status match {
        //Add successful! Send block to peers, log success, try to add other blocks
        case Valid =>
          for {
            updatedDag <- addToState(block, transforms)
            _ <- Log[F].info(
                  s"Added ${PrettyPrinter.buildString(block.blockHash)}"
                )
          } yield updatedDag

        case MissingBlocks =>
          Cell[F, CasperState].modify { s =>
            s.copy(blockBuffer = s.blockBuffer + block)
          } *>
            addMissingDependencies(block, dag) *>
            dag.pure[F]

        case AdmissibleEquivocation =>
          val baseEquivocationBlockSeqNum = block.seqNum - 1
          for {
            _ <- Cell[F, CasperState].modify { s =>
                  if (s.equivocationsTracker.exists {
                        case EquivocationRecord(validator, seqNum, _) =>
                          block.sender == validator && baseEquivocationBlockSeqNum == seqNum
                      }) {
                    // More than 2 equivocating children from base equivocation block and base block has already been recorded
                    s
                  } else {
                    val newEquivocationRecord =
                      EquivocationRecord(
                        block.sender,
                        baseEquivocationBlockSeqNum,
                        Set.empty[BlockHash]
                      )
                    s.copy(equivocationsTracker = s.equivocationsTracker + newEquivocationRecord)
                  }
                }
            updatedDag <- addToState(block, transforms)
            _ <- Log[F].info(
                  s"Added admissible equivocation child block ${PrettyPrinter.buildString(block.blockHash)}"
                )
          } yield updatedDag

        case IgnorableEquivocation =>
          /*
           * We don't have to include these blocks to the equivocation tracker because if any validator
           * will build off this side of the equivocation, we will get another attempt to add this block
           * through the admissible equivocations.
           */
          Log[F]
            .info(
              s"Did not add block ${PrettyPrinter.buildString(block.blockHash)} as that would add an equivocation to the BlockDAG"
            ) *> dag.pure[F]

        case InvalidUnslashableBlock | InvalidFollows | InvalidBlockNumber | InvalidParents |
            JustificationRegression | InvalidSequenceNumber | NeglectedInvalidBlock |
            NeglectedEquivocation | InvalidTransaction | InvalidBondsCache | InvalidRepeatDeploy |
            InvalidShardId | InvalidBlockHash | InvalidDeployCount | InvalidPreStateHash |
            InvalidPostStateHash =>
          handleInvalidBlockEffect(status, block, transforms)

        case Processing =>
          throw new RuntimeException(s"A block should not be processing at this stage.")

        case BlockException(ex) =>
          Log[F].error(s"Encountered exception in while processing block ${PrettyPrinter
            .buildString(block.blockHash)}: ${ex.getMessage}") *> dag.pure[F]
      }

    /** Remember a block as being invalid, then save it to storage. */
    private def handleInvalidBlockEffect(
        status: BlockStatus,
        block: BlockMessage,
        effects: Seq[ipc.TransformEntry]
    )(implicit state: Cell[F, CasperState]): F[BlockDagRepresentation[F]] =
      for {
        _ <- Log[F].warn(
              s"Recording invalid block ${PrettyPrinter.buildString(block.blockHash)} for ${status.toString}."
            )
        // TODO: Slash block for status except InvalidUnslashableBlock
        _ <- Cell[F, CasperState].modify { s =>
              s.copy(invalidBlockTracker = s.invalidBlockTracker + block.blockHash)
            }
        updateDag <- addToState(block, effects)
      } yield updateDag

    /** Save the block to the block and DAG storage. */
    private def addToState(
        block: BlockMessage,
        effects: Seq[ipc.TransformEntry]
    ): F[BlockDagRepresentation[F]] =
      for {
        _          <- BlockStore[F].put(block.blockHash, BlockMsgWithTransform(Some(block), effects))
        updatedDag <- BlockDagStorage[F].insert(block)
        hash       = block.blockHash
      } yield updatedDag

    /** Check if the block has dependencies that we don't have in store.
      * Add those to the dependency DAG. */
    def addMissingDependencies(
        block: BlockMessage,
        dag: BlockDagRepresentation[F]
    )(implicit state: Cell[F, CasperState]): F[Unit] =
      for {
        missingDependencies <- dependenciesHashesOf(block).filterA(dag.contains(_).map(!_))
        _                   <- missingDependencies.traverse(hash => addMissingDependency(hash, block.blockHash))
      } yield ()

    /** Keep track of a block depending on another one, so we know when we can start processing. */
    private def addMissingDependency(
        ancestorHash: BlockHash,
        childHash: BlockHash
    )(implicit state: Cell[F, CasperState]): F[Unit] =
      Cell[F, CasperState].modify(
        s =>
          s.copy(
            dependencyDag = DoublyLinkedDagOperations
              .add[BlockHash](s.dependencyDag, ancestorHash, childHash)
          )
      )
  }

  object StatelessExecutor {
    case class Context(genesis: BlockMessage, lastFinalizedBlockHash: BlockHash)
  }

  /** Encapsulating all methods that might use peer-to-peer communication. */
  trait Broadcaster[F[_]] {
    def networkEffects(
        block: BlockMessage,
        status: BlockStatus
    ): F[Unit]

    def requestMissingDependency(blockHash: BlockHash): F[Unit]
  }

  object Broadcaster {
    def fromTransportLayer[F[_]: Monad: ConnectionsCell: TransportLayer: Log: Time: ErrorHandler: RPConfAsk]()(
        implicit state: Cell[F, CasperState]
    ) =
      new Broadcaster[F] {

        /** Gossip the created block, or ask for dependencies. */
        def networkEffects(
            block: BlockMessage, // Not just BlockHash because if the status MissingBlocks it's not in store yet; although we should be able to get it from CasperState.
            status: BlockStatus
        ): F[Unit] =
          status match {
            //Add successful! Send block to peers.
            case Valid | AdmissibleEquivocation =>
              CommUtil.sendBlock[F](block)

            case MissingBlocks =>
              // In the future this won't happen because the DownloadManager won't try to add blocks with missing dependencies.
              fetchMissingDependencies(block)

            case IgnorableEquivocation | InvalidUnslashableBlock | InvalidFollows |
                InvalidBlockNumber | InvalidParents | JustificationRegression |
                InvalidSequenceNumber | NeglectedInvalidBlock | NeglectedEquivocation |
                InvalidTransaction | InvalidBondsCache | InvalidRepeatDeploy | InvalidShardId |
                InvalidBlockHash | InvalidDeployCount | InvalidPreStateHash | InvalidPostStateHash |
                Processing =>
              Log[F].debug(
                s"Not sending notification about ${PrettyPrinter.buildString(block.blockHash)}: $status"
              )

            case BlockException(ex) =>
              Log[F].debug(
                s"Not sending notification about ${PrettyPrinter.buildString(block.blockHash)}: $ex"
              )
          }

        /** Ask all peers to send us a block. */
        def requestMissingDependency(blockHash: BlockHash) =
          CommUtil.sendBlockRequest[F](
            BlockRequest(Base16.encode(blockHash.toByteArray), blockHash)
          )

        /** Check if the block has dependencies that we don't have in store of buffer.
          * Add those to the dependency DAG and ask peers to send it. */
        private def fetchMissingDependencies(
            block: BlockMessage
        )(implicit state: Cell[F, CasperState]): F[Unit] =
          for {
            casperState <- Cell[F, CasperState].read
            missingDependencies = casperState.dependencyDag
              .childToParentAdjacencyList(block.blockHash)
              .toList
            missingUnseenDependencies = missingDependencies.filter(
              blockHash => !casperState.seenBlockHashes.contains(blockHash)
            )
            _ <- missingUnseenDependencies.traverse(hash => requestMissingDependency(hash))
          } yield ()
      }

    /** Network access using the new RPC style gossiping. */
    def fromGossipServices[F[_]: Applicative](
        validatorId: Option[ValidatorIdentity],
        relaying: gossiping.Relaying[F]
    ) = new Broadcaster[F] {

      val maybeOwnPublickKey = validatorId map {
        case ValidatorIdentity(publicKey, _, _) =>
          ByteString.copyFrom(publicKey)
      }

      def networkEffects(
          block: BlockMessage,
          status: BlockStatus
      ): F[Unit] =
        status match {
          case Valid | AdmissibleEquivocation =>
            maybeOwnPublickKey match {
              case Some(key) if key == block.sender =>
                relaying.relay(List(block.blockHash))
              case _ =>
                // We were adding somebody else's block. The DownloadManager did the gossiping.
                ().pure[F]
            }

          case MissingBlocks =>
            throw new RuntimeException(
              "Impossible! The DownloadManager should not tell Casper about blocks with missing dependencies!"
            )

          case IgnorableEquivocation | InvalidUnslashableBlock | InvalidFollows |
              InvalidBlockNumber | InvalidParents | JustificationRegression |
              InvalidSequenceNumber | NeglectedInvalidBlock | NeglectedEquivocation |
              InvalidTransaction | InvalidBondsCache | InvalidRepeatDeploy | InvalidShardId |
              InvalidBlockHash | InvalidDeployCount | InvalidPreStateHash | InvalidPostStateHash |
              Processing =>
            ().pure[F]

          case BlockException(_) =>
            ().pure[F]
        }

      def requestMissingDependency(blockHash: BlockHash): F[Unit] =
        // We are letting Casper know about the pending DAG, so it may try to ask for dependencies,
        // but those will be naturally downloaded by the DownloadManager.
        ().pure[F]
    }
  }
}
