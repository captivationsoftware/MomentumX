
#include "control.h"
#include <nlohmann/json.hpp>

namespace MomentumX {

    std::string BufferSync::dumps(int64_t indent) const {
        return nlohmann::json(*this).dump(indent);
    }
    void to_json(nlohmann::json& j, const BufferSync& bs) {
        j = nlohmann::json{{{"is_sent", bs._is_sent},
                            {"state", bs.to_str(bs.get_state())},
                            {"active_readers", bs._active_readers},
                            {"active_writers", bs._active_writers},
                            {"done_readers", bs._done_readers},
                            {"done_writers", bs._done_writers},
                            {"required_readers", bs._required_readers}}};
    }

    std::string LockableBufferState::dumps(int64_t indent) const {
        return nlohmann::json(*this).dump(indent);
    }
    void to_json(nlohmann::json& j, const LockableBufferState& lbs) {
        j = nlohmann::json{{{"buffer_state", lbs.buffer_state}, {"buffer_sync", lbs.buffer_sync}}};
    }

    std::string ControlBlock::dumps(int64_t indent) const {
        return nlohmann::json(*this).dump(indent);
    }
    void to_json(nlohmann::json& j, const ControlBlock& cb) {
        j = nlohmann::json{{{"sync", cb.sync},
                            {"is_ended", cb.is_ended},
                            {"buffer_size", cb.buffer_size},
                            {"buffer_count", cb.buffer_count},
                            {"subscriber_count", cb.subscriber_count},
                            {"last_sent_index", cb.last_sent_index},
                            {"last_sent_iteration", cb.last_sent_iteration()},
                            {"buffers", cb.buffers}}};
    }

}  // namespace MomentumX