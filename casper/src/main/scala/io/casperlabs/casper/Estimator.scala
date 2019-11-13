package io.casperlabs.casper

import cats.Monad
import cats.data.NonEmptyList
import cats.effect.Sync
import cats.effect.concurrent.Ref
import com.google.protobuf.ByteString
import io.casperlabs.casper.util.DagOperations
import io.casperlabs.casper.util.ProtoUtil.weightFromValidatorByDag
import io.casperlabs.catscontrib.MonadThrowable
import io.casperlabs.metrics.Metrics
import io.casperlabs.metrics.implicits._
import io.casperlabs.models.{Message, Weight}
import io.casperlabs.shared.StreamT
import io.casperlabs.storage.dag.DagRepresentation
import cats.implicits._
import com.github.ghik.silencer.silent

import scala.collection.immutable.Map

@silent("is never used")
object Estimator {
  type BlockHash = ByteString
  type Validator = ByteString

  import Weight._

  implicit val metricsSource = CasperMetricsSource

  def tips[F[_]: Sync: Metrics](
      dag: DagRepresentation[F],
      genesis: BlockHash,
      latestMessageHashes: Map[Validator, Set[BlockHash]],
      equivocators: Set[Validator]
  ): F[List[BlockHash]] =
    DagView.fromLatestMessageHashes[F](latestMessageHashes, dag) flatMap { view =>
      NonEmptyList.fromList(view.latestMessages.toList) match {
        case None => List(genesis).pure[F]

        case Some(latestMessages) =>
          for {
            lca <- DagOperations
                    .latestCommonAncestorsMainParent(dag, latestMessages.map(_.messageHash))
                    .timer("calculateLCA")
            _ <- Metrics[F]
                  .record("lcaDistance", latestMessages.toList.maxBy(_.rank).rank - lca.rank)
            honestValidators <- latestHashesToLatestMessage[F](
                                 dag,
                                 latestMessageHashes,
                                 equivocators
                               )
            init   = List(ForkChoiceLoopStatus.continue(lca.messageHash))
            result <- forkChoiceLoop(init, honestValidators, view).timer("forkChoiceLoop")
            secondaryParents <- filterSecondaryParents[F](
                                 result.head,
                                 result.tail,
                                 equivocators,
                                 dag
                               )
          } yield result.head :: secondaryParents
      }
    }

  private def filterSecondaryParents[F[_]: MonadThrowable](
      primaryParent: BlockHash,
      secondaryParents: List[BlockHash],
      equivocators: Set[Validator],
      dag: DagRepresentation[F]
  ): F[List[BlockHash]] =
    for {
      // cannot choose blocks from equivocators as secondary parents
      honestParents <- secondaryParents
                        .traverse(dag.lookup)
                        .map(_.flatten.filter(m => !equivocators(m.validatorId)).map(_.messageHash))
      // Since the fork-choice rule only looks at main parents, it is
      // possible that some of the chosen secondary parents are reachable
      // from the chosen main parent. Therefore, we look for and remove
      // any such redundancies. Note that it may also be the case that
      // some blocks in the secondary parents are redundant amongst
      // themselves, however we do not remove them at this stage since
      // they may or may not be used during merging.
      redundancies <- DagOperations.collectWhereDescendantPathExists[F](
                       dag,
                       honestParents.toSet,
                       Set(primaryParent)
                     )
    } yield honestParents.filter(!redundancies(_))

  /**
    * Looks up the latest message for each honest validator from
    * the latest message hash. Note this is unique because honest
    * validators have not equivocated, so have a unique latest message.
    */
  private def latestHashesToLatestMessage[F[_]: Monad](
      dag: DagRepresentation[F],
      latestMessageHashes: Map[Validator, Set[BlockHash]],
      equivocators: Set[Validator]
  ): F[List[(Validator, Message)]] =
    latestMessageHashes
      .filterKeys(!equivocators(_))
      .toList
      .traverse { case (v, lms) => lms.headOption.flatTraverse(dag.lookup).map(v -> _) }
      .map(_.collect { case (v, Some(m)) => v -> m })

  /**
    * Finds the main child of `b` that the validator with latest message
    * `latestMessage` votes for, if any.
    */
  private def childVotedFor[F[_]: MonadThrowable: Metrics](
      b: BlockHash,
      latestMessage: Message,
      dag: DagRepresentation[F]
  ): F[Option[Message]] = Metrics[F].timer("childVotedFor") {
    dag.lookup(b) flatMap {
      case None => none[Message].pure[F]

      case Some(message) =>
        DagOperations
          .swimlaneV[F](latestMessage.validatorId, latestMessage, dag)
          .takeWhile(_.rank > message.rank)
          // for each message, `lm`, from this validator,
          // (lazily) find the child of `message` it votes for, if any
          .flatMap(lm => StreamT.lift(DagOperations.findMainAncestor[F](message, lm, dag)))
          // find the most recent vote
          .find(_.nonEmpty)
          .map(_.flatten)
    }
  }

  private def forkChoiceLoop[F[_]: Sync: Metrics](
      orderedCandidates: List[ForkChoiceLoopStatus],
      honestLatestMessages: List[(Validator, Message)],
      view: DagView[F]
  ): F[List[BlockHash]] =
    orderedCandidates
      .traverse {
        case terminal @ ForkChoiceLoopStatus.Terminated(_) =>
          List[ForkChoiceLoopStatus](terminal).pure[F]

        case ForkChoiceLoopStatus.Continue(block) =>
          orderChildren[F](block, honestLatestMessages, view)
      }
      .flatMap { orderedChildren =>
        val newCandidates = orderedChildren.flatten

        if (orderedCandidates == newCandidates) orderedCandidates.map(_.messageHash).pure[F]
        else forkChoiceLoop(newCandidates, honestLatestMessages, view)
      }

  /**
    * Orders the children of `block` by how much weight votes for each of them
    * (using block hash has a tie breaker). If `block` has no children, then the
    * block itself is returned.
    */
  private def orderChildren[F[_]: Sync: Metrics](
      block: BlockHash,
      honestLatestMessages: List[(Validator, Message)],
      view: DagView[F]
  ): F[List[ForkChoiceLoopStatus]] = Metrics[F].timer("orderChildren") {
    view.getMainChildrenInView(block) flatMap {
      case Nil => List(ForkChoiceLoopStatus.terminated(block)).pure[F]

      case children =>
        honestLatestMessages
          .traverse {
            case (v, lm) =>
              view.childVotedFor(block, lm) flatMap {
                case None => none[(BlockHash, BigInt)].pure[F]

                case Some(child) =>
                  weightFromValidatorByDag[F](view.dag, block, v).map { weight =>
                    (child.messageHash -> weight).some
                  }
              }
          }
          .map { votes =>
            val scores = votes.flatten
              .groupBy(_._1)
              .mapValues(_.foldLeft(Zero) { case (acc, (_, weight)) => acc + weight })

            children
              .sortBy(child => scores.getOrElse(child, Zero) -> child.toStringUtf8)(
                Ordering[(BigInt, String)].reverse
              )
              .map(ForkChoiceLoopStatus.continue)
          }
    }
  }

  private class DagView[F[_]](
      val dag: DagRepresentation[F],
      val latestMessages: Set[Message],
      private val inViewSet: Ref[F, Set[BlockHash]],
      private val voteLookupTable: Ref[F, Map[Validator, Map[BlockHash, Message]]]
  ) {
    private val maxRank =
      if (latestMessages.isEmpty) 0L
      else latestMessages.map(_.rank).max

    def inView(blockHash: BlockHash)(implicit monad: Monad[F]): F[Boolean] =
      monad.ifM(inViewSet.get.map(_.contains(blockHash)))(
        true.pure[F],
        dag.lookup(blockHash) flatMap {
          case None => false.pure[F]

          case Some(message) =>
            if (message.rank > maxRank) false.pure[F]
            else {
              // Doing it manually so that I can cache visited blocks in the j-past-cone.
              DagOperations
                .bfTraverseF[F, Message](latestMessages.toList)(
                  _.justifications.toList
                    .traverse(j => dag.lookup(j.latestBlockHash))
                    .map(_.flatten)
                )
                .flatMap(m => StreamT.lift(inViewSet.update(_ + m.messageHash).as(m)))
                .find(Set(message))
                .map(_.nonEmpty)
            }
        }
      )

    def childVotedFor(b: BlockHash, latestMessage: Message)(
        implicit S: Sync[F],
        M: Metrics[F]
    ): F[Option[Message]] = M.timer("childVotedFor") {
      voteLookupTable.get.flatMap { lookupTable =>
        lookupTable.get(latestMessage.validatorId).flatMap(_.get(b)) match {
          case Some(childVotedFor) =>
            Option(childVotedFor).pure[F]
          case None =>
            dag.lookup(b).map(_.get).flatMap { message =>
              DagOperations
                .swimlaneV[F](latestMessage.validatorId, latestMessage, dag)
                .takeWhile(_.rank > message.rank)
                .flatMap(m => StreamT.lift(dag.lookup(m.parentBlock).map(_.get).tupleLeft(m))) // TODO: replace .get with lookupUnsafe
                .flatMap {
                  case (m, parent) =>
                    // TODO: Possible optimization is to collect all the (message, main parent) pairs
                    // while traversing and set the lookup table once, at the end.
                    StreamT.lift(
                      voteLookupTable
                        .set(
                          lookupTable.updated(
                            latestMessage.validatorId,
                            lookupTable
                              .getOrElse(latestMessage.validatorId, Map.empty)
                              .updated(parent.messageHash, m)
                          )
                        )
                        .as(m)
                    )
                }
                // TODO: We shouldn't need this. Just traverse the main tree once from latest messages to LCA
                // and build up the lookup table.
                // for each message, `lm`, from this validator,
                // (lazily) find the child of `message` it votes for, if any
                .flatMap(
                  lm => StreamT.lift(DagOperations.findMainAncestor[F](message, lm, dag))
                )
                // find the most recent vote
                .find(_.nonEmpty)
                .map(_.flatten)
            }
        }
      }
    }

    /**
      * Returns the children of `block` in the main tree which are visible from the
      * given latest messages.
      */
    def getMainChildrenInView(block: BlockHash)(implicit monad: Monad[F]): F[List[BlockHash]] =
      dag.getMainChildren(block) flatMap (
        _.filterA(inView)
      )
  }

  private object DagView {
    def apply[F[_]: Sync](dag: DagRepresentation[F], latestMessages: Set[Message]): F[DagView[F]] =
      for {
        inViewSet <- Ref
                      .of[F, Set[BlockHash]](latestMessages.map(_.messageHash))
        votesLookupTable <- Ref.of[F, Map[Validator, Map[BlockHash, Message]]](Map.empty)
      } yield new DagView[F](dag, latestMessages, inViewSet, votesLookupTable)

    def fromLatestMessageHashes[F[_]: Sync](
        latestMessageHashes: Map[Validator, Set[BlockHash]],
        dag: DagRepresentation[F]
    ): F[DagView[F]] =
      latestMessageHashes.values
        .foldLeft(Set.empty[BlockHash]) {
          case (acc, ms) => acc union ms
        }
        .toList
        .traverse(dag.lookup)
        .flatMap(lms => DagView[F](dag, lms.flatten.toSet))
  }

  /**
    * Helper newtype class for bookkeeping during the fork-choice loop.
    * If we know a block is a tip of the main tree then we mark it as
    * `Terminated` and no longer call `orderChildren` on it.
    */
  private sealed trait ForkChoiceLoopStatus {
    val messageHash: BlockHash
  }

  private object ForkChoiceLoopStatus {
    def terminated(b: BlockHash): ForkChoiceLoopStatus = Terminated(b)
    def continue(b: BlockHash): ForkChoiceLoopStatus   = Continue(b)

    case class Terminated(messageHash: BlockHash) extends ForkChoiceLoopStatus
    case class Continue(messageHash: BlockHash)   extends ForkChoiceLoopStatus
  }
}
