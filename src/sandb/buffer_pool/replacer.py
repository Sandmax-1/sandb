from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque
from queue import PriorityQueue


@dataclass
class LRUNode:

    frame_id: int
    evictable: bool
    k: int
    frame_id: int
    k_distance: timedelta | None = None
    history: deque[datetime] = field(default_factory=deque)

    def add_history(self, time_to_add: datetime) -> None:
        self.history.append(time_to_add)
        if len(self.history) == self.k + 1:
            self.history.popleft()

    def update_k_distance(self) -> None:
        if len(self.history) == self.k:
            self.k_distance = self.history[0] - self.history[-1]


@dataclass
class LRUKReplcer:

    k: int
    max_number_frames: int
    nodes: dict[int, LRUNode] = field(default_factory=dict)
    evictable: PriorityQueue[tuple[timedelta, int]] = field(
        default_factory=PriorityQueue
    )

    def evict(self) -> int:
        k_distance, frame_id = self.evictable.get()
        if (
            k_distance != self.nodes[frame_id].k_distance
            or not self.nodes[frame_id].evictable
        ):
            return self.evict()

        else:
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
            self.evictable.put(
                (-node.k_distance if node.k_distance else -timedelta.max, node.frame_id)
            )
