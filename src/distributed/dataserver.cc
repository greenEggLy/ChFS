#include "distributed/dataserver.h"

#include <memory>

#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  auto version_block_sz =
      (KDefaultBlockCnt * sizeof(version_t)) / DiskBlockSize;
  if (version_block_sz * DiskBlockSize < KDefaultBlockCnt * sizeof(version_t)) {
    version_block_sz++;
  }
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, version_block_sz, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, version_block_sz, true);
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  std::vector<u8> buffer(DiskBlockSize), content(len);
  auto num_per_block = DiskBlockSize / sizeof(version_t);
  auto version_block_id = block_id / num_per_block;
  auto version_block_offset = block_id % num_per_block;
  auto res1 = block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (res1.is_err()) return {};
  auto *real_version =
      (version_t *)(buffer.data() +
                    version_block_offset * (sizeof(version_t) / sizeof(u8)));
  if (*real_version != version) {
    return {};
  }
  auto res = block_allocator_->bm->read_block(block_id, buffer.data());
  if (res.is_err()) {
    return {};
  }
  std::move(buffer.begin() + offset, buffer.begin() + offset + len,
            content.begin());
  return content;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  auto res = block_allocator_->bm->write_partial_block(block_id, buffer.data(),
                                                       offset, buffer.size());
  if (res.is_err()) {
    return false;
  }
  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  auto num_per_block = DiskBlockSize / sizeof(version_t);
  std::vector<u8> buffer(DiskBlockSize);
  auto res = block_allocator_->allocate();
  if (res.is_err()) {
    return {};
  }
  auto block_id = res.unwrap();
  auto version_block_id = block_id / num_per_block;
  auto version_block_offset = block_id % num_per_block;
  auto res1 = block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (res1.is_err()) return {};
  auto *version =
      (version_t *)(buffer.data() +
                    version_block_offset * (sizeof(version_t) / sizeof(u8)));
  *version += 1;
  auto res2 =
      block_allocator_->bm->write_block(version_block_id, buffer.data());
  if (res2.is_err()) return {};
  return {block_id, *version};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  auto num_per_block = DiskBlockSize / sizeof(version_t);
  auto res = block_allocator_->deallocate(block_id);
  if (res.is_err()) {
    return false;
  }
  std::vector<u8> buffer(DiskBlockSize);
  auto version_block_id = block_id / num_per_block;
  auto version_block_offset = block_id % num_per_block;
  auto res1 = block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (res1.is_err()) return false;
  auto *version =
      (version_t *)(buffer.data() +
                    version_block_offset * (sizeof(version_t) / sizeof(u8)));
  *version += 1;
  auto res2 =
      block_allocator_->bm->write_block(version_block_id, buffer.data());
  if (res2.is_err()) return false;
  return true;
}
}  // namespace chfs