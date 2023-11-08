#include "metadata/inode.h"

namespace chfs {

auto Inode::begin() -> InodeIterator { return InodeIterator(this, 0); }

auto Inode::end() -> InodeIterator {
  return InodeIterator(this, this->nblocks);
}

auto Inode::write_indirect_block(
    std::shared_ptr<BlockManager>& bm, std::vector<u8>& buffer,
    std::vector<std::shared_ptr<BlockOperation>>* ops) -> ChfsNullResult {
  if (this->blocks[this->nblocks - 1] == KInvalidBlockID) {
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }

  auto res = bm->write_block(this->blocks[this->nblocks - 1], buffer.data());
  if (ops) {
    ops->emplace_back(std::make_shared<BlockOperation>(
        this->blocks[this->nblocks - 1], buffer));
  }
  if (res.is_err()) return res.unwrap_error();
  return res;
}

}  // namespace chfs