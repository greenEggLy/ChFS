#include "distributed/client.h"

#include <utility>

#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
    case ServerType::DATA_SERVER:
      num_data_servers += 1;
      data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                  address, port, reliable)});
      break;
    case ServerType::METADATA_SERVER:
      metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
      break;
    default:
      std::cerr << "Unknown Type" << std::endl;
      exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  auto res = metadata_server_->call("mknode", (u8)type, parent, name);
  if (res.is_err()) return {res.unwrap_error()};
  if (res.unwrap()->as<inode_id_t>() == 0) return {ErrorType::INVALID};
  return {res.unwrap()->as<inode_id_t>()};
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  auto res = metadata_server_->call("unlink", parent, name);
  if (res.is_err()) return res.unwrap_error();
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  auto res = metadata_server_->call("lookup", parent, name);
  if (res.is_err()) return res.unwrap_error();
  return {res.unwrap()->as<inode_id_t>()};
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  auto res = metadata_server_->call("readdir", id);
  if (res.is_err()) return res.unwrap_error();
  return {res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>()};
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  auto res = metadata_server_->call("get_type_attr", id);
  if (res.is_err()) return res.unwrap_error();
  std::pair<InodeType, FileAttr> ret;
  // size, atime, mtime, ctime
  auto [size, atime, mtime, ctime, type] =
      res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  //  if (static_cast<InodeType>(type) == InodeType::FILE) {
  //    auto res2 = metadata_server_->call("get_block_map", id);
  //    if (res2.is_err()) return res2.unwrap_error();
  //    size = res2.unwrap()->as<std::vector<BlockInfo>>().size() *
  //    DiskBlockSize;
  //  }
  ret.first = static_cast<InodeType>(type);
  ret.second.size = size;
  ret.second.atime = atime;
  ret.second.mtime = mtime;
  ret.second.ctime = ctime;
  return {ret};
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  auto res = metadata_server_->call("get_block_map", id);
  if (res.is_err()) return res.unwrap_error();
  auto block_info = res.unwrap()->as<std::vector<BlockInfo>>();
  auto file_size = block_info.size() * DiskBlockSize;
  std::vector<u8> content;
  auto block_info_index = offset / DiskBlockSize;
  auto block_info_offset = offset % DiskBlockSize;
  //  auto read_size = size;
  auto read_size = file_size;
  while (size > 0) {
    auto left_size_in_block = DiskBlockSize - block_info_offset;
    read_size = size > left_size_in_block ? left_size_in_block : size;
    auto [blockId, macId, version] = block_info.at(block_info_index);
    auto res1 = data_servers_[macId]->call(
        "read_data", blockId, block_info_offset, read_size, version);
    if (res1.is_err()) return res1.unwrap_error();
    auto data = res1.unwrap()->as<std::vector<u8>>();
    content.insert(content.end(), data.begin(), data.end());

    size -= read_size;
    block_info_index++;
    block_info_offset = 0;
  }
  for (int i = 0; i < 30; i++) {
    std::cerr << content[i];
  }
  std::cerr << std::endl;
  return {content};
}

ChfsNullResult get_or_alloc_block_info(
    const std::shared_ptr<RpcClient> &metadata_server, inode_id_t id,
    std::vector<BlockInfo> &block_infos, size_t index) {
  while (block_infos.size() <= index) {
    auto res = metadata_server->call("alloc_block", id);
    if (res.is_err()) return res.unwrap_error();
    block_infos.emplace_back(res.unwrap()->as<BlockInfo>());
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  auto res = metadata_server_->call("get_block_map", id);
  if (res.is_err()) return res.unwrap_error();
  auto block_info = res.unwrap()->as<std::vector<BlockInfo>>();
  auto block_info_index = offset / DiskBlockSize;
  auto block_info_offset = offset % DiskBlockSize;
  auto write_len = data.size();  // left size to write
  size_t write_start = 0,
         write_end = write_len;  // pointer to the data of start and end
  // if we should alloc new blocks
  while (write_len > 0) {  // still needs to write
    auto left_size_in_block = DiskBlockSize - block_info_offset;
    write_end = left_size_in_block > write_len
                    ? write_start + write_len
                    : write_start + left_size_in_block;
    std::vector<u8> tmp_data(write_end - write_start);
    std::move(data.begin() + (long)write_start, data.begin() + (long)write_end,
              tmp_data.begin());
    get_or_alloc_block_info(metadata_server_, id, block_info, block_info_index);
    auto [blockId, macId, version] = block_info.at(block_info_index);
    auto res1 = data_servers_[macId]->call(
        "write_data", blockId, block_info_offset, std::move(tmp_data));
    //    auto res2 = data_servers_[macId]->call()
    if (res1.is_err()) return res1.unwrap_error();
    write_len -= write_end - write_start;
    write_start = write_end;
    block_info_index++;
    block_info_offset = 0;
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  auto res = metadata_server_->call("free_block", id, block_id, mac_id);
  return KNullOk;
}

}  // namespace chfs