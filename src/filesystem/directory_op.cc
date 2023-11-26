#include "filesystem/directory_op.h"

#include <algorithm>
#include <sstream>

#include "common/logger.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {
  if (src.empty()) {
    return filename + ":" + inode_id_to_string(id);
  }
  src.append("/" + filename + ":" + inode_id_to_string(id));
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {
  list.clear();
  std::istringstream ss(src);
  std::string token;

  while (std::getline(ss, token, '/')) {
    size_t pos = token.find(':');
    if (pos != std::string::npos) {
      auto name = token.substr(0, pos);
      auto inode_str = token.substr(pos + 1);
      auto inode = string_to_inode_id(inode_str);
      list.emplace_back(DirectoryEntry{name, inode});
    }
  }
}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {
  std::string res;

  auto list = std::list<DirectoryEntry>();
  parse_directory(src, list);
  for (const auto &item : list) {
    if (item.name == filename) continue;
    res = append_to_directory(res, item.name, item.id);
  }
  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  // through fs to fill the list
  auto content = fs->read_file(id);
  if (content.is_err()) {
    return {content.unwrap_error()};
  }
  auto contents = content.unwrap();
  std::string src(contents.begin(), contents.end());
  parse_directory(src, list);
  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;
  auto res = read_directory(this, id, list);
  if (res.is_err()) {
    return {res.unwrap_error()};
  }
  for (const auto &item : list) {
    if (item.name == name) {
      return {item.id};
    }
  }
  //        return {ErrorType::NotEmpty};
  return {ErrorType::NotExist};
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type,
                              std::vector<std::shared_ptr<BlockOperation>> *ops)
    -> ChfsResult<inode_id_t> {
  auto res = lookup(id, name);
  if (res.is_ok()) {
    return {ErrorType::AlreadyExist};
  }
  // save block changes
  ChfsResult<inode_id_t> res_(0);
  std::vector<u8> buffer(this->block_manager_->block_size());

  inode_id_t inode_id = 0;
  res_ = alloc_inode(type, ops, &inode_id);

  if (res_.is_err() && !ops) {
    return {res_.unwrap_error()};
  } else if (res_.is_ok()) {
    inode_id = res_.unwrap();
  }

  // append to parent
  auto content = read_file(id);
  if (content.is_err()) {
    return {content.unwrap_error()};
  }
  auto contents = content.unwrap();
  auto src = std::string(contents.begin(), contents.end());
  src = append_to_directory(src, name, inode_id);
  auto _res = write_file(id, std::vector<u8>(src.begin(), src.end()), ops);
  if (_res.is_err()) {
    return {_res.unwrap_error()};
  }
  if (res_.is_err()) {
    return {res_.unwrap_error()};
  }
  return {inode_id};
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name,
                           std::vector<std::shared_ptr<BlockOperation>> *ops)
    -> ChfsNullResult {
  // remove from parent
  auto res = read_file(parent);
  if (res.is_err()) {
    return {res.unwrap_error()};
  }
  auto content = res.unwrap();
  auto src = std::string(content.begin(), content.end());
  std::list<DirectoryEntry> list;
  parse_directory(src, list);
  for (const auto &item : list) {
    if (item.name == name) {
      remove_file(item.id, ops);
      break;
    }
  }
  src = rm_from_directory(src, std::string{name});
  auto res2 = write_file(parent, std::vector<u8>(src.begin(), src.end()), ops);
  if (res2.is_err()) {
    return {res2.unwrap_error()};
  }
  return KNullOk;
}

}  // namespace chfs
