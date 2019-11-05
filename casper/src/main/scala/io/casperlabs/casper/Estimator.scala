package io.casperlabs.casper

import cats.Monad
import cats.data.NonEmptyList
import cats.implicits._
import com.google.protobuf.ByteString
import io.casperlabs.casper.util.DagOperations
import io.casperlabs.casper.util.ProtoUtil.weightFromValidatorByDag
import io.casperlabs.catscontrib.MonadThrowable
import io.casperlabs.models.{Message, Weight}
import io.casperlabs.storage.dag.DagRepresentation

import scala.collection.immutable.Map

object Estimator {
  type BlockHash = ByteString
  type Validator = ByteString

  import Weight._

  def tips[F[_]: MonadThrowable](
      dag: DagRepresentation[F],
      genesis: BlockHash,
      latestMessageHashes: Map[Validator, Set[BlockHash]],
      equivocators: Set[Validator]
  ): F[List[BlockHash]] =
    latestHashesToLatestMessage[F](dag, latestMessageHashes, equivocators) flatMap {
      honestValidators =>
        val honestTips = honestValidators.map(_._2.messageHash)

        NonEmptyList.fromList(honestTips) match {
          case None => List(genesis).pure[F]

          case Some(tips) =>
            for {
              lca    <- DagOperations.latestCommonAncestorsMainParent(dag, tips)
              result <- forkChoiceLoop(List(lca), honestValidators, dag)
              // Since the fork-choice rule only looks at main parents, it is
              // possible that some of the chosen secondary parents are reachable
              // from the chosen main parent. Therefore, we look for and remove
              // and such redundancies. Note that it may also be the case that
              // some blocks in the secondary parents are redundant amongst
              // themselves, however we do not remove them at this stage since
              // they may or may not be used during merging.
              redundancies <- DagOperations.collectWhereDescendantPathExists[F](
                               dag,
                               result.tail.toSet,
                               Set(result.head)
                             )
            } yield result.filter(!redundancies(_))
        }
    }

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
    * Finds the latest message of validator `v` voting for block `b`, if any.
    */
  private def latestMessageForBlock[F[_]: MonadThrowable](
      b: BlockHash,
      v: Validator,
      latestMessage: Message,
      dag: DagRepresentation[F]
  ): F[Option[Message]] =
    DagOperations
      .swimlaneV[F](v, latestMessage, dag)
      .findF(isMainAncestor(b, _, dag))

  private def isMainAncestor[F[_]: MonadThrowable](
      smallerRank: BlockHash,
      largerRank: Message,
      dag: DagRepresentation[F]
  ): F[Boolean] = dag.lookup(smallerRank) flatMap {
    case None          => false.pure[F]
    case Some(message) => DagOperations.isMainAncestor(message, largerRank, dag)
  }

  private def forkChoiceLoop[F[_]: MonadThrowable](
      result: List[BlockHash],
      honestLatestMessages: List[(Validator, Message)],
      dag: DagRepresentation[F]
  ): F[List[BlockHash]] =
    result
      .traverse { block =>
        orderChildren[F](block, honestLatestMessages, dag)
      }
      .flatMap { orderedChildren =>
        val newResult = orderedChildren.flatten

        if (result == newResult) result.pure[F]
        else forkChoiceLoop(newResult, honestLatestMessages, dag)
      }

  /**
    * Returns the children of `block` in the main tree which are visible from the
    * given latest messages.
    */
  private def getMainChildrenInView[F[_]: MonadThrowable](
      block: BlockHash,
      honestLatestMessages: List[(Validator, Message)],
      dag: DagRepresentation[F]
  ): F[List[BlockHash]] = dag.getMainChildren(block) flatMap { children =>
    val lms = honestLatestMessages.map(_._2)

    // keep children which can be reached from latest message justifications
    children.filterA { child =>
      DagOperations.toposortJDagDesc[F](dag, lms).find(m => m.messageHash == child).map(_.nonEmpty)
    }
  }

  /**
    * Orders the children of `block` by how much weight votes for each of them
    * (using block hash has a tie breaker). If `block` has no children, then the
    * block itself is returned.
    */
  private def orderChildren[F[_]: MonadThrowable](
      block: BlockHash,
      honestLatestMessages: List[(Validator, Message)],
      dag: DagRepresentation[F]
  ): F[List[BlockHash]] = getMainChildrenInView[F](block, honestLatestMessages, dag) flatMap {
    case Nil => List(block).pure[F]

    case children =>
      honestLatestMessages
        .traverse { case (v, lm) => latestMessageForBlock[F](block, v, lm, dag).map(v -> _) }
        .map(_.collect { case (v, Some(lm)) if lm.messageHash != block => v -> lm })
        .flatMap(_.traverse {
          case (v, latestMessage) =>
            for {
              weight     <- weightFromValidatorByDag[F](dag, block, v)
              maybeChild <- children.findM(child => isMainAncestor[F](child, latestMessage, dag))
              child <- maybeChild.fold(
                        // This case should never happen because if `latestMessageForBlock`
                        // returned `Some(latestMessage)` then `block` is an ancestor of
                        // `latestMessage`, which means either one of the children of `block`
                        // must also be an ancestor of `latestMessage`, or `latestMessage`
                        // equals `block`. The latter case is taken care of in the
                        // `collect` statement where we explicitly filter out latest messages
                        // equal to the current block. Note that this filtering is valid
                        // because in that case the validator does not vote for any child.
                        MonadThrowable[F].raiseError[BlockHash](
                          new Exception("Latest message votes for no child!")
                        )
                      )(child => child.pure[F])
            } yield (child, weight)
        }.map { votes =>
          val scores = votes
            .groupBy(_._1)
            .mapValues(_.foldLeft(Zero) { case (acc, (_, weight)) => acc + weight })

          children.sortBy(child => scores.getOrElse(child, Zero) -> child.toStringUtf8)(
            Ordering[(BigInt, String)].reverse
          )
        })
  }
}
