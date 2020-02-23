package db.migration

import cats.effect.Blocker
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
import io.casperlabs.casper.consensus.BlockSummary
import io.casperlabs.storage.block.BlockStorage.BlockHash
import io.casperlabs.storage.util.DoobieCodecs
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.CanBlock
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

class V20200218_1143__Add_validator_block_seq_num extends BaseJavaMigration with DoobieCodecs {
  override def migrate(context: Context) = {
    val connection            = context.getConnection
    implicit val scheduler    = Scheduler(ExecutionContexts.synchronous)
    implicit val contextShift = Task.contextShift(scheduler)
    implicit val canBlock     = CanBlock.permit
    val xa = Transactor
      .fromConnection[Task](connection, Blocker.liftExecutionContext(ExecutionContexts.synchronous))

    sql"ALTER TABLE block_metadata ADD COLUMN validator_block_seq_num INT NOT NULL DEFAULT 0".update.run
      .transact(xa)
      .runSyncUnsafe()

    val data = sql"SELECT block_hash, data FROM block_metadata"
      .query[(BlockHash, BlockSummary)]
      .stream
      .evalMap {
        case (hash, summary) =>
          val n = summary.getHeader.validatorBlockSeqNum
          sql"UPDATE block_metadata SET validator_block_seq_num=$n WHERE block_hash=$hash".update.run
      }
    data.transact(xa).compile.toList.runSyncUnsafe()
  }
}