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

#include "block/manager.h"
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
class BlockOperation {
 public:
  explicit BlockOperation(block_id_t block_id, std::vector<u8> new_block_state)
      : block_id_(block_id), new_block_state_(new_block_state) {
    CHFS_ASSERT(new_block_state.size() == DiskBlockSize, "invalid block state");
  }

  block_id_t block_id_;
  std::vector<u8> new_block_state_;
};

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
  auto get_log_entry_num() -> usize;

  bool is_checkpoint_enabled_;
  std::shared_ptr<BlockManager> bm_;
  /**
   * {Append anything if you need}
   */
 private:
  std::shared_ptr<BlockAllocator> block_allocator_;
  usize log_entry_block_cnt;
  usize bitmap_block_cnt;
  usize commit_block_id;
  usize log_entry_block_id;
  usize bitmap_block_id;
  usize block_sz;
  usize commit_num = 0;
};

}  // namespace chfs