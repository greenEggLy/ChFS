#include <mutex>
#include <sstream>

#include "rsm/state_machine.h"

namespace chfs {
class ListCommand : public ChfsCommand {
public:
  ListCommand() {
  }

  ListCommand(int v)
    : value(v) {
  }

  ~ListCommand() {
  }

  size_t size() const override { return 4; }

  std::vector<u8> serialize(int sz) const override {
    std::vector<u8> buf;

    if (sz != size()) return buf;

    buf.push_back((value >> 24) & 0xff);
    buf.push_back((value >> 16) & 0xff);
    buf.push_back((value >> 8) & 0xff);
    buf.push_back(value & 0xff);

    return buf;
  }

  void deserialize(std::vector<u8> data, int sz) override {
    if (sz != size()) return;
    value = (data[0] & 0xff) << 24;
    value |= (data[1] & 0xff) << 16;
    value |= (data[2] & 0xff) << 8;
    value |= data[3] & 0xff;
  }

  int value;
};

class ListStateMachine : public ChfsStateMachine {
public:
  ListStateMachine() {
    store.push_back(0);
    num_append_logs = 0;
  }

  virtual ~ListStateMachine() {
  }

  virtual std::vector<u8> snapshot() override {
    std::unique_lock lock(mtx);
    std::vector<u8> data;
    std::stringstream ss;
    ss << (int)store.size();
    for (auto value : store) ss << ' ' << value;
    std::string str = ss.str();
    data.assign(str.begin(), str.end());
    return data;
  }

  void apply_log(ChfsCommand& cmd) override {
    std::unique_lock lock(mtx);
    const ListCommand& list_cmd = dynamic_cast<const ListCommand&>(cmd);
    store.push_back(list_cmd.value);
    ++num_append_logs;
  }

  //  bool check_apply_log(commit_id_t index) {
  //    if (index > store.size()) return false;
  //  }

  size_t get_applied_size() const { return store.size(); }

  void apply_snapshot(const std::vector<u8>& snapshot) override {
    std::unique_lock<std::mutex> lock(mtx);
    std::string str;
    str.assign(snapshot.begin(), snapshot.end());
    std::stringstream ss(str);
    store = std::vector<int>();
    int size;
    ss >> size;
    // if (size > 3) {
    //   LOG_FORMAT_INFO("larger than 3: {}, num_applied_logs: {}", size,
    //                   num_append_logs);
    // }
    for (int i = 0; i < size; i++) {
      int temp;
      ss >> temp;
      // if (size > 3) {
      //   LOG_FORMAT_INFO("temp: {}", temp);
      // }
      store.push_back(temp);
    }
  }

  std::mutex mtx;
  std::vector<int> store;
  std::atomic<int> num_append_logs;
};
} /* namespace chfs */
