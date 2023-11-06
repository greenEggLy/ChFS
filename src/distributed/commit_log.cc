#include "distributed/commit_log.h"

#include <algorithm>
#include <chrono>
#include <utility>

#include "common/bitmap.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(std::move(bm)) {
  auto block_cnt = this->bm_->total_blocks();
  this->block_sz = bm_->block_size();
  auto bits_per_block = block_sz * KBitsPerByte;
  auto meta_per_block = block_sz / sizeof(LogInfo);
  // NOTICE: reserve 2 blocks for commit log
  this->log_entry_block_cnt = block_cnt / meta_per_block;
  if (log_entry_block_cnt % meta_per_block != 0) {
    log_entry_block_cnt++;
  }
  this->bitmap_block_cnt = block_cnt / bits_per_block;
  if (bitmap_block_cnt % bits_per_block != 0) {
    bitmap_block_cnt++;
  }
  this->commit_block_id = bm_->get_log_id();
  this->log_entry_block_id = commit_block_id + 2;
  this->bitmap_block_id = log_entry_block_id + 2 + log_entry_block_cnt;
  // block allocator
  block_allocator_ =
      std::make_shared<BlockAllocator>(bm_, bitmap_block_id, true);
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize { return log_entry_block_cnt; }

// {Your code here}
auto CommitLog::append_log(
    txn_id_t txn_id, const std::vector<std::shared_ptr<BlockOperation>> &ops)
    -> void {
  std::vector<u8> buffer(block_sz);
  std::vector<u8> logInfo(sizeof(LogInfo));
  for (const auto &item : ops) {
    auto meta_block_id = item->block_id_;
    auto &new_data = item->new_block_state_;
    auto res = block_allocator_->allocate();
    if (res.is_err()) return;
    auto logger_block_id = res.unwrap();
    // write into entry_table
    auto num_info_per_block = block_sz / sizeof(LogInfo);
    auto entry_block_id =
        log_entry_block_id + logger_block_id / num_info_per_block;
    auto entry_block_offset =
        logger_block_id % num_info_per_block * sizeof(LogInfo);
    bm_->read_block(entry_block_id, buffer.data());
    *((LogInfo *)logInfo.data()) =
        std::make_tuple(txn_id, meta_block_id, logger_block_id);
    bm_->write_partial_block(entry_block_id, logInfo.data(), entry_block_offset,
                             sizeof(LogInfo));
    // write into logger's block
    bm_->write_block(logger_block_id, new_data.data());
    // sync to disk
    bm_->sync(logger_block_id);
    bm_->sync(entry_block_id);
  }
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  auto num_txn_per_block = block_sz / sizeof(txn_id_t);
  auto block_id = this->commit_block_id + commit_num / num_txn_per_block;
  auto offset = commit_num % num_txn_per_block * sizeof(txn_id_t);
  std::vector<u8> buffer(sizeof(txn_id_t));
  *(txn_id_t *)(buffer.data()) = txn_id;
  bm_->write_partial_block(block_id, buffer.data(), offset, sizeof(txn_id_t));
  bm_->sync(block_id);
  commit_num++;
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  // traverse the commits
  std::vector<u8> commit_buffer(block_sz);
  std::vector<u8> entry_buffer(block_sz);
  std::vector<u8> content_buffer(block_sz);
  bm_->read_block(this->commit_block_id, commit_buffer.data());
  txn_id_t inblock_idx = 0;
  for (int i = 0; i < commit_num; ++i, inblock_idx++) {
    // for each commit
    if (i == block_sz / sizeof(txn_id_t)) {
      bm_->read_block(this->commit_block_id + 1, commit_buffer.data());
      inblock_idx = 0;
    }
    txn_id_t trans_id =
        *(txn_id_t *)(commit_buffer.data() + inblock_idx * sizeof(txn_id_t));
    for (int j = 0; j < this->log_entry_block_cnt; ++j) {
      // traverse the logs
      auto entry_block_id = j + this->log_entry_block_id;
      bm_->read_block(entry_block_id, entry_buffer.data());
      auto *logInfo = (LogInfo *)entry_buffer.data();
      auto size = block_sz / sizeof(LogInfo);
      for (int k = 0; k < size; ++k) {
        auto [transaction_id, meta_block_id, logger_block_id] = *logInfo;
        if (transaction_id == trans_id) {
          bm_->read_block(logger_block_id, content_buffer.data());
          bm_->write_block(meta_block_id, content_buffer.data());
        }
        logInfo++;
      }
    }
  }
}
};  // namespace chfs