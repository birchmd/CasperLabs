package io.casperlabs.casper

import cats.data.NonEmptyList
import com.github.ghik.silencer.silent
import com.google.protobuf.ByteString
import io.casperlabs.casper.Estimator.{BlockHash, Validator}
import io.casperlabs.casper.equivocations.EquivocationDetector
import io.casperlabs.casper.helper.BlockGenerator._
import io.casperlabs.casper.helper.BlockUtil.generateValidator
import io.casperlabs.casper.helper.{BlockGenerator, StorageFixture}
import io.casperlabs.casper.util.BondingUtil.Bond
import io.casperlabs.casper.util.DagOperations
import io.casperlabs.models.Weight
import io.casperlabs.storage.dag.DagRepresentation
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.{Gen, Shrink}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.{HashMap, Map}
import scala.concurrent.duration._

@silent("is never used")
class ForkchoiceTest
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with BlockGenerator
    with StorageFixture {

  "Estimator on empty latestMessages" should "return the genesis regardless of DAG" in withStorage {
    implicit blockStorage => implicit dagStorage => implicit deployStorage =>
      val v1     = generateValidator("V1")
      val v2     = generateValidator("V2")
      val v1Bond = Bond(v1, 2)
      val v2Bond = Bond(v2, 3)
      val bonds  = Seq(v1Bond, v2Bond)
      for {
        genesis <- createAndStoreBlock[Task](Seq(), ByteString.EMPTY, bonds)
        b2 <- createAndStoreBlock[Task](
               Seq(genesis.blockHash),
               v2,
               bonds,
               HashMap(v1 -> genesis.blockHash, v2 -> genesis.blockHash)
             )
        b3 <- createAndStoreBlock[Task](
               Seq(genesis.blockHash),
               v1,
               bonds,
               HashMap(v1 -> genesis.blockHash, v2 -> genesis.blockHash)
             )
        b4 <- createAndStoreBlock[Task](
               Seq(b2.blockHash),
               v2,
               bonds,
               HashMap(v1 -> genesis.blockHash, v2 -> b2.blockHash)
             )
        b5 <- createAndStoreBlock[Task](
               Seq(b2.blockHash),
               v1,
               bonds,
               HashMap(v1 -> b3.blockHash, v2 -> b2.blockHash)
             )
        b6 <- createAndStoreBlock[Task](
               Seq(b4.blockHash),
               v2,
               bonds,
               HashMap(v1 -> b5.blockHash, v2 -> b4.blockHash)
             )
        b7 <- createAndStoreBlock[Task](
               Seq(b4.blockHash),
               v1,
               bonds,
               HashMap(v1 -> b5.blockHash, v2 -> b4.blockHash)
             )
        b8 <- createAndStoreBlock[Task](
               Seq(b7.blockHash),
               v1,
               bonds,
               HashMap(v1 -> b7.blockHash, v2 -> b4.blockHash)
             )
        dag          <- dagStorage.getRepresentation
        equivocators <- dag.getEquivocators
        forkchoice <- Estimator.tips[Task](
                       dag,
                       genesis.blockHash,
                       Map.empty,
                       equivocators
                     )
      } yield forkchoice.head should be(genesis.blockHash)
  }

  // See https://docs.google.com/presentation/d/1znz01SF1ljriPzbMoFV0J127ryPglUYLFyhvsb-ftQk/edit?usp=sharing slide 29 for diagram
  "Estimator on Simple DAG" should "return the appropriate score map and forkchoice" in withStorage {
    implicit blockStorage => implicit dagStorage => implicit deployStorage =>
      val v1     = generateValidator("V1")
      val v2     = generateValidator("V2")
      val v1Bond = Bond(v1, 2)
      val v2Bond = Bond(v2, 3)
      val bonds  = Seq(v1Bond, v2Bond)
      for {
        genesis <- createAndStoreBlock[Task](Seq(), ByteString.EMPTY, bonds)
        b2 <- createAndStoreBlock[Task](
               Seq(genesis.blockHash),
               v2,
               bonds,
               HashMap(v1 -> genesis.blockHash, v2 -> genesis.blockHash)
             )
        b3 <- createAndStoreBlock[Task](
               Seq(genesis.blockHash),
               v1,
               bonds,
               HashMap(v1 -> genesis.blockHash, v2 -> genesis.blockHash)
             )
        b4 <- createAndStoreBlock[Task](
               Seq(b2.blockHash),
               v2,
               bonds,
               HashMap(v1 -> genesis.blockHash, v2 -> b2.blockHash)
             )
        b5 <- createAndStoreBlock[Task](
               Seq(b2.blockHash),
               v1,
               bonds,
               HashMap(v1 -> b3.blockHash, v2 -> b2.blockHash)
             )
        b6 <- createAndStoreBlock[Task](
               Seq(b4.blockHash),
               v2,
               bonds,
               HashMap(v1 -> b5.blockHash, v2 -> b4.blockHash)
             )
        b7 <- createAndStoreBlock[Task](
               Seq(b4.blockHash),
               v1,
               bonds,
               HashMap(v1 -> b5.blockHash, v2 -> b4.blockHash)
             )
        b8 <- createAndStoreBlock[Task](
               Seq(b7.blockHash),
               v1,
               bonds,
               HashMap(v1 -> b7.blockHash, v2 -> b4.blockHash)
             )
        dag          <- dagStorage.getRepresentation
        latestBlocks <- dag.latestMessageHashes
        equivocators <- dag.getEquivocators
        forkchoice <- Estimator.tips[Task](
                       dag,
                       genesis.blockHash,
                       latestBlocks,
                       equivocators
                     )
        _      = forkchoice.head should be(b6.blockHash)
        result = forkchoice(1) should be(b8.blockHash)
      } yield result
  }

  // See [[/docs/casper/images/no_finalizable_block_mistake_with_no_disagreement_check.png]]
  "Estimator on flipping forkchoice DAG" should "return the appropriate score map and forkchoice" in withStorage {
    implicit blockStorage => implicit dagStorage => implicit deployStorage =>
      val v1     = generateValidator("V1")
      val v2     = generateValidator("V2")
      val v3     = generateValidator("V3")
      val v1Bond = Bond(v1, 25)
      val v2Bond = Bond(v2, 20)
      val v3Bond = Bond(v3, 15)
      val bonds  = Seq(v1Bond, v2Bond, v3Bond)
      for {
        genesis <- createAndStoreBlock[Task](Seq(), ByteString.EMPTY, bonds)
        b2 <- createAndStoreBlock[Task](
               Seq(genesis.blockHash),
               v2,
               bonds,
               HashMap(v1 -> genesis.blockHash, v2 -> genesis.blockHash, v3 -> genesis.blockHash)
             )
        b3 <- createAndStoreBlock[Task](
               Seq(genesis.blockHash),
               v1,
               bonds,
               HashMap(v1 -> genesis.blockHash, v2 -> genesis.blockHash, v3 -> genesis.blockHash)
             )
        b4 <- createAndStoreBlock[Task](
               Seq(b2.blockHash),
               v3,
               bonds,
               HashMap(v1 -> genesis.blockHash, v2 -> b2.blockHash, v3 -> b2.blockHash)
             )
        b5 <- createAndStoreBlock[Task](
               Seq(b3.blockHash),
               v2,
               bonds,
               HashMap(v1 -> b3.blockHash, v2 -> b2.blockHash, v3 -> genesis.blockHash)
             )
        b6 <- createAndStoreBlock[Task](
               Seq(b3.blockHash),
               v1,
               bonds,
               HashMap(v1 -> b3.blockHash, v2 -> b2.blockHash, v3 -> b4.blockHash)
             )
        b7 <- createAndStoreBlock[Task](
               Seq(b5.blockHash),
               v3,
               bonds,
               HashMap(v1 -> b3.blockHash, v2 -> b5.blockHash, v3 -> b4.blockHash)
             )
        b8 <- createAndStoreBlock[Task](
               Seq(b6.blockHash),
               v2,
               bonds,
               HashMap(v1 -> b6.blockHash, v2 -> b5.blockHash, v3 -> b4.blockHash)
             )
        dag          <- dagStorage.getRepresentation
        latestBlocks <- dag.latestMessageHashes
        equivocators <- dag.getEquivocators
        forkchoice <- Estimator.tips[Task](
                       dag,
                       genesis.blockHash,
                       latestBlocks,
                       equivocators
                     )
        _      = forkchoice.head should be(b8.blockHash)
        result = forkchoice(1) should be(b7.blockHash)
      } yield result
  }

  // See [[casper/src/test/resources/casper/tipsHavingEquivocating.png]]
  "Estimator on DAG having validators equivocated" should "return the appropriate score map and main parent" in withStorage {
    implicit blockStorage => implicit dagStorage => _ =>
      val v1     = generateValidator("V1")
      val v2     = generateValidator("V2")
      val v1Bond = Bond(v1, 5)
      val v2Bond = Bond(v2, 3)
      val bonds  = Seq(v1Bond, v2Bond)

      for {
        genesis <- createAndStoreBlock[Task](Seq(), ByteString.EMPTY, bonds)
        a1      <- createAndStoreBlock[Task](Seq(genesis.blockHash), v1, bonds)
        a2      <- createAndStoreBlock[Task](Seq(genesis.blockHash), v1, bonds)
        b       <- createAndStoreBlock[Task](Seq(a2.blockHash), v2, bonds, Map(v1 -> a2.blockHash))
        c       <- createAndStoreBlock[Task](Seq(b.blockHash), v1, bonds, Map(v1 -> a1.blockHash))
        dag     <- dagStorage.getRepresentation

        latestMessageHashes <- dag.latestMessageHashes
        equivocatingValidators <- EquivocationDetector.detectVisibleFromJustifications(
                                   dag,
                                   latestMessageHashes
                                 )
        _            = equivocatingValidators shouldBe Set(v1)
        equivocators <- dag.getEquivocators
        tips <- Estimator.tips(
                 dag,
                 genesis.blockHash,
                 latestMessageHashes,
                 equivocators
               )
        _ = tips.head shouldBe c.blockHash
      } yield ()
  }

  it should "not use blocks from equivocators as secondary parents" in withStorage {
    implicit blockStorage => implicit dagStorage => implicit deployStorage =>
      val v1    = generateValidator("v1")
      val v2    = generateValidator("v2")
      val bonds = Seq(Bond(v1, 10), Bond(v2, 10))

      for {
        genesis <- createAndStoreBlock[Task](Seq(), ByteString.EMPTY, bonds)
        // v1 equivocates
        a1                  <- createAndStoreBlock[Task](Seq(genesis.blockHash), v1, bonds)
        a2                  <- createAndStoreBlock[Task](Seq(genesis.blockHash), v1, bonds)
        b                   <- createAndStoreBlock[Task](Seq(genesis.blockHash), v2, bonds)
        dag                 <- dagStorage.getRepresentation
        equivocators        <- dag.getEquivocators
        latestMessageHashes <- dag.latestMessageHashes
        tips                <- Estimator.tips(dag, genesis.blockHash, latestMessageHashes, equivocators)
        _                   = tips shouldBe List(b.blockHash)
      } yield ()
  }

  "Estimator on DAG with latest messages having secondary parents in the path (NODE-943)" should "propagate 0 scores to secondary parents and choose the right tips" in withStorage {
    implicit blockStorage => implicit dagStorage => implicit deployStorage =>
      val v1 = generateValidator("V1")
      val v2 = generateValidator("V2")
      val v3 = generateValidator("V3")
      val bonds = Seq(
        Bond(v1, 30),
        Bond(v2, 80),
        Bond(v3, 10)
      )

      // DAG:
      //    B1
      //  //  \\
      // G      B3 -- B4
      //  \\         //
      //    B2 ======
      // The bug in NODE-943 was that B4 did not propagate a score to B3,
      // so we ended up with tips [B4, B1] instead of [B4]

      for {
        genesis <- createAndStoreBlock[Task](Seq(), ByteString.EMPTY, bonds)
        b1 <- createAndStoreBlock[Task](
               Seq(genesis.blockHash),
               v1,
               bonds,
               HashMap(v1 -> genesis.blockHash, v2 -> genesis.blockHash, v3 -> genesis.blockHash)
             )
        b2 <- createAndStoreBlock[Task](
               Seq(genesis.blockHash),
               v2,
               bonds,
               HashMap(v1 -> genesis.blockHash, v2 -> genesis.blockHash, v3 -> genesis.blockHash)
             )
        b3 <- createAndStoreBlock[Task](
               Seq(b1.blockHash),
               v3,
               bonds,
               HashMap(v1 -> b1.blockHash, v2 -> genesis.blockHash, v3 -> genesis.blockHash)
             )
        b4 <- createAndStoreBlock[Task](
               Seq(b2.blockHash, b3.blockHash),
               v3,
               bonds,
               HashMap(v1 -> b1.blockHash, v2 -> b2.blockHash, v3 -> b3.blockHash)
             )
        dag          <- dagStorage.getRepresentation
        latestBlocks <- dag.latestMessageHashes
        equivocators <- dag.getEquivocators
        forkchoice <- Estimator.tips[Task](
                       dag,
                       genesis.blockHash,
                       latestBlocks,
                       equivocators
                     )
        _ = forkchoice shouldBe List(b4.blockHash)
      } yield ()
  }

  "lmdMainchainGhost" should "pick the correct fork choice tip" in withStorage {
    implicit blockStorage => implicit dagStorage =>
      implicit deployStorage =>
        /* The DAG looks like:
         *
         *
         *            ---i
         *          /    |
         *        g   h  |
         *       | \ /  \|
         *       d  e   f
         *      / \/   /
         *     a  b   c
         *      \ |  /
         *       genesis
         */
        val v1     = generateValidator("V1")
        val v2     = generateValidator("V2")
        val v3     = generateValidator("V3")
        val v1Bond = Bond(v1, 7)
        val v2Bond = Bond(v2, 5)
        val v3Bond = Bond(v3, 3)
        val bonds  = Seq(v1Bond, v2Bond, v3Bond)

        for {
          genesis      <- createAndStoreBlock[Task](Seq(), ByteString.EMPTY, bonds)
          a            <- createAndStoreBlock[Task](Seq(genesis.blockHash), v1, bonds)
          b            <- createAndStoreBlock[Task](Seq(genesis.blockHash), v2, bonds)
          c            <- createAndStoreBlock[Task](Seq(genesis.blockHash), v3, bonds)
          d            <- createAndStoreBlock[Task](Seq(a.blockHash, b.blockHash), v1, bonds)
          e            <- createAndStoreBlock[Task](Seq(b.blockHash), v2, bonds)
          f            <- createAndStoreBlock[Task](Seq(c.blockHash), v3, bonds)
          g            <- createAndStoreBlock[Task](Seq(d.blockHash, e.blockHash), v1, bonds)
          h            <- createAndStoreBlock[Task](Seq(e.blockHash, f.blockHash), v2, bonds)
          i            <- createAndStoreBlock[Task](Seq(g.blockHash, f.blockHash), v3, bonds)
          dag          <- dagStorage.getRepresentation
          latestBlocks <- dag.latestMessageHashes
          equivocators <- dag.getEquivocators
          tips         <- Estimator.tips(dag, genesis.blockHash, latestBlocks, equivocators)
          _            = tips.head shouldEqual i.blockHash
        } yield ()
  }

}
