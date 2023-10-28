#include "distributed/metadata_server.h"

#include <fstream>

#include "common/logger.h"
#include "common/util.h"
#include "filesystem/directory_op.h"

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0;  // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_) operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled),
      may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled),
      may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  for (auto &item : meta_mtx) {
    item.lock();
  }
  auto res =
      this->operation_->mk_helper(parent, name.data(), (InodeType)(type));
  for (auto &item : meta_mtx) {
    item.unlock();
  }
  if (res.is_err()) return 0;
  return res.unwrap();
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  for (auto &item : meta_mtx) {
    item.lock();
  }
  auto res = this->operation_->unlink(parent, name.data());
  for (auto &item : meta_mtx) {
    item.unlock();
  }
  if (res.is_err()) return false;
  return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  std::shared_lock<std::shared_mutex> lockGuard(
      meta_mtx[meta_lock_num(parent)]);
  auto res = this->operation_->lookup(parent, name.data());
  if (res.is_err()) return 0;
  return res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  std::vector<u8> data;
  {
    std::shared_lock<std::shared_mutex> sharedLock(meta_mtx[meta_lock_num(id)]);
    auto res = this->operation_->read_file(id);
    if (res.is_err()) return {};
    data = res.unwrap();
  }
  std::vector<BlockInfo> result;
  auto tuple_size = sizeof(BlockInfo);
  for (unsigned long i = 0; i < data.size(); i += tuple_size) {
    auto tuple = *(BlockInfo *)(data.data() + i);
    auto [block_id, mac_id, version] = tuple;
    result.emplace_back(block_id, mac_id, version);
  }
  return result;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  auto machine_id = generator.rand(1, this->num_data_servers);
  std::unique_lock<std::shared_mutex> lock(data_mtx[data_lock_num(machine_id)]);
  auto client = clients_[machine_id];
  auto res = client->call("alloc_block");
  lock.unlock();
  if (res.is_err()) return {};
  auto [block_id, version] =
      res.unwrap()->as<std::pair<block_id_t, version_t>>();
  std::lock_guard<std::shared_mutex> lockGuard(meta_mtx[meta_lock_num(id)]);
  auto res1 = operation_->read_file(id);
  if (res1.is_err()) return {};
  auto data = res1.unwrap();
  std::vector<u8> content(sizeof(BlockInfo));
  *((BlockInfo *)content.data()) =
      std::make_tuple(block_id, machine_id, version);
  data.insert(data.end(), content.begin(), content.end());
  auto res2 = operation_->write_file(id, data);
  if (res2.is_err()) return {};
  return {block_id, machine_id, version};
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  std::lock_guard<std::shared_mutex> lockGuard(meta_mtx[meta_lock_num(id)]);
  auto res = this->operation_->read_file(id);
  if (res.is_err()) return false;
  auto data = res.unwrap();
  auto size = data.size();
  auto meta_block_size = sizeof(BlockInfo);
  for (unsigned long i = 0; i < size; i += meta_block_size) {
    auto tuple = *(BlockInfo *)(data.data() + i);
    if (std::get<0>(tuple) == block_id && std::get<1>(tuple) == machine_id) {
      {
        std::lock_guard<std::shared_mutex> lockGuard1(
            data_mtx[data_lock_num(machine_id)]);
        auto client = clients_[machine_id];
        auto res1 = client->call("free_block", block_id);
        if (res1.is_err()) return false;
      }
      data.erase(data.begin() + i, data.begin() + i + meta_block_size);
      this->operation_->write_file(id, data);
      return true;
    }
  }
  return false;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  std::list<DirectoryEntry> list;
  std::vector<std::pair<std::string, inode_id_t>> ret_val;
  {
    std::shared_lock<std::shared_mutex> lockGuard(
        meta_mtx[meta_lock_num(node)]);
    auto res = read_directory(operation_.get(), node, list);
    if (res.is_err()) return {};
  }
  for (const auto &item : list) {
    ret_val.emplace_back(item.name, item.id);
  }
  return ret_val;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  std::shared_lock<std::shared_mutex> lockGuard(meta_mtx[meta_lock_num(id)]);
  auto res = this->operation_->get_type_attr(id);
  if (res.is_err()) return {};
  auto type = (u8)res.unwrap().first;
  auto attribute = res.unwrap().second;
  return {attribute.size, attribute.atime, attribute.mtime, attribute.ctime,
          type};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running) return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

}  // namespace chfs