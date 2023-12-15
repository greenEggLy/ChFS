//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// commit_log.h
//
// Identification: src/include/distributed/commit_log.h
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "block/allocator.h"
#include "block/manager.h"
#include "common/bitmap.h"
#include "common/config.h"
#include "common/macros.h"
#include "filesystem/operations.h"

namespace chfs {
using LogInfo = std::tuple<txn_id_t, block_id_t, block_id_t>;
/**
 * `BlockOperation` is an entry indicates an old block state and
 * a new block state. It's used to redo the operation when
 * the system is crashed.
 */
// class BlockOperation {
//  public:
//   explicit BlockOperation(block_id_t block_id, std::vector<u8>
//   new_block_state)
//       : block_id_(block_id), new_block_state_(new_block_state) {
//     CHFS_ASSERT(new_block_state.size() == DiskBlockSize, "invalid block
//     state");
//   }
//
//   block_id_t block_id_;
//   std::vector<u8> new_block_state_;
// };

/**
 * `CommitLog` is a class that records the block edits into the
 * commit log. It's used to redo the operation when the system
 * is crashed.
 */
class CommitLog {
 public:
  explicit CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled);
  ~CommitLog();
  auto append_log(txn_id_t txn_id,
                  const std::vector<std::shared_ptr<BlockOperation>>& ops)
      -> void;
  auto commit_log(txn_id_t txn_id) -> void;
  auto checkpoint() -> void;
  auto recover() -> void;
  auto get_log_entry_num() const -> usize;
  auto get_used_blocks_num() const -> usize;

  bool is_checkpoint_enabled_;
  std::shared_ptr<BlockManager> bm_;

 private:
  // commit_block_id is the start block id of the section where records
  // committed transaction
  usize commit_block_id_;
  // log entry is a tuple of <txn_id, block_id in meta-server, block_id in
  // logger-in-meta>
  usize entry_block_id_;
  usize entry_block_cnt_;
  // bitmap block
  usize bitmap_block_id_;
  usize bitmap_block_cnt_;
  usize last_bitmap_num_;
  // commit num is the committed transaction number
  usize commit_num_ = 0;
  // txn num is the transaction number
  usize logger_block_id_ = 0;
  usize logger_block_cnt_ = 0;

  usize block_sz;
};

}  // namespace chfs