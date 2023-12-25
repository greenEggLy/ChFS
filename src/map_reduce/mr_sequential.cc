#include <string>
#include <utility>
#include <vector>
#include <algorithm>
#include <sstream>

#include "map_reduce/protocol.h"

namespace mapReduce {
SequentialMapReduce::SequentialMapReduce(
    std::shared_ptr<chfs::ChfsClient> client,
    const std::vector<std::string>& files_, std::string resultFile) {
  chfs_client = std::move(client);
  files = files_;
  outPutFile = resultFile;
  // Your code goes here (optional)
}

void SequentialMapReduce::doWork() {
  // Your code goes here
  std::map<std::string, long> map_res;
  for (const auto& file : files) {
    auto res_create = chfs_client->lookup(1,
                                          file);
    auto inode_id = res_create.unwrap();
    auto res_attr = chfs_client->get_type_attr(inode_id);
    auto res_read = chfs_client->read_file(inode_id, 0,
                                           res_attr.unwrap().second.size);
    if (res_read.is_err()) {
      assert(0);
    }
    std::vector<uint8_t> vec = res_read.unwrap();
    std::string content;
    content.assign(vec.begin(), vec.end());
    auto ret = Map(content);
    for (const auto& [key, val] : ret) {
      map_res[key] += std::stol(val);
    }
  }
  std::vector<chfs::u8> buffer;
  buffer.reserve(chfs::DiskBlockSize);
  for (const auto& [word, times] : map_res) {
    std::stringstream ss;
    ss << word << ' ' << times << '\n';
    std::string str = ss.str();
    std::vector<chfs::u8> vec(str.begin(), str.end());
    buffer.insert(buffer.end(), vec.begin(), vec.end());
  }
  auto res_lookup = chfs_client->lookup(1, outPutFile);
  auto inode_id = res_lookup.unwrap();
  chfs_client->write_file(inode_id, 0, buffer);
}
}
