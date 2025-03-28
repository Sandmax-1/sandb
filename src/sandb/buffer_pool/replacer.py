from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass
class LRUNode:
    frame_id: int
    evictable: bool
    k: int
    k_distance: timedelta | None = None
    history: deque[datetime] = field(default_factory=deque)

    def add_history(self, time_to_add: datetime) -> None:
        self.history.append(time_to_add)
        if len(self.history) == self.k + 1:
            self.history.popleft()


@dataclass
class LRUKReplcer:
    k: int
    max_number_frames: int
    nodes: dict[int, LRUNode] = field(default_factory=dict)
    evictable: list[int] = field(default_factory=list)

    def evict(self) -> int:
        current_timestamp = datetime.now()
        _, frame_id = max(
            (
                (current_timestamp - self.nodes[frame_id].history[0], frame_id)
                if len(self.nodes[frame_id].history) == self.k
                else (datetime.max, frame_id)
            )
            for frame_id in self.evictable
        )
        self.evictable.remove(frame_id)
        del self.nodes[frame_id]
        return frame_id

    def record_frame_access(self, frame_id: int) -> None:
        # Should I care that dict() isn't thread safe as the GIL should stop
        # anything bad happening here?
        self.nodes[frame_id].add_history(datetime.now())

    def add_node(self, frame_id: int) -> None:
        if frame_id in self.nodes:
            raise Exception(f"frame: {frame_id} is already in replacer.")
        self.nodes[frame_id] = LRUNode(frame_id, False, self.k)

    def set_evictable(self, frame_id: int, set_evictale: bool) -> None:
        node = self.nodes[frame_id]
        node.evictable = set_evictale

        if set_evictale:
            self.evictable.append(frame_id)
