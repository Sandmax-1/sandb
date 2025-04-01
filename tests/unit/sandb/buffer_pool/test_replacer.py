from datetime import datetime, timedelta

import pytest

from sandb.buffer_pool.replacer import LRUKReplacer, LRUNode


class TestLRUNodePytest:
    def test_add_history(self) -> None:
        node = LRUNode(frame_id=1, evictable=False, k=3)
        now = datetime.now()
        node.add_history(now - timedelta(seconds=5))
        assert len(node.history) == 1
        node.add_history(now - timedelta(seconds=3))
        assert len(node.history) == 2
        node.add_history(now - timedelta(seconds=1))
        assert len(node.history) == 3
        node.add_history(now)
        assert len(node.history) == 3
        assert node.history[0] == now - timedelta(seconds=3)
        assert node.history[2] == now


class TestLRUKReplcerPytest:
    def test_add_node(self) -> None:
        replacer = LRUKReplacer(k=2)
        replacer.add_node(1)
        assert 1 in replacer.nodes
        assert not replacer.nodes[1].evictable
        assert not replacer.evictable

    def test_add_existing_node(self) -> None:
        replacer = LRUKReplacer(k=2)
        replacer.add_node(1)
        with pytest.raises(Exception, match="frame: 1 is already in replacer."):
            replacer.add_node(1)

    def test_set_evictable(self) -> None:
        replacer = LRUKReplacer(k=2)
        replacer.add_node(1)
        replacer.set_evictable(1, True)
        assert replacer.nodes[1].evictable
        assert 1 in replacer.evictable

        replacer.set_evictable(1, False)
        assert not replacer.nodes[1].evictable
        assert 1 not in replacer.evictable

    def test_record_frame_access(self) -> None:
        replacer = LRUKReplacer(k=2)
        replacer.add_node(1)
        now1 = datetime.now()
        now2 = datetime.now()
        replacer.record_frame_access(1, now1)
        assert len(replacer.nodes[1].history) == 1

        replacer.record_frame_access(1, now2)
        assert len(replacer.nodes[1].history) == 2
        assert replacer.nodes[1].history[1] == now2
        assert replacer.nodes[1].history[0] == now1

    def test_evict_of_nodes_with_k_and_less_than_k_accesses(self) -> None:
        replacer = LRUKReplacer(k=2)
        replacer.add_node(1)
        replacer.add_node(2)
        replacer.set_evictable(1, True)
        replacer.set_evictable(2, True)

        now = datetime.now()
        replacer.record_frame_access(1, now - timedelta(seconds=5))
        replacer.record_frame_access(1, now - timedelta(seconds=4))
        replacer.record_frame_access(2, now - timedelta(seconds=1))

        evicted_frame = replacer.evict()
        assert evicted_frame == 2
        assert 2 not in replacer.nodes
        assert 2 not in replacer.evictable

    def test_evict_with_less_than_k_accesses(self) -> None:
        now = datetime.now()
        replacer = LRUKReplacer(k=3)
        replacer.add_node(1)
        replacer.add_node(2)
        replacer.set_evictable(1, True)
        replacer.set_evictable(2, True)

        replacer.record_frame_access(1, now - timedelta(seconds=11))
        replacer.record_frame_access(2, now - timedelta(seconds=10))

        evicted_frame = replacer.evict()
        assert evicted_frame == 2
        assert 2 not in replacer.nodes
        assert 2 not in replacer.evictable

    def test_evict_non_evictable(self) -> None:
        now = datetime.now()
        replacer = LRUKReplacer(k=2)
        replacer.add_node(1)
        replacer.add_node(2)
        replacer.set_evictable(1, True)
        replacer.record_frame_access(1, now - timedelta(seconds=10))

        replacer.record_frame_access(2, now - timedelta(seconds=5))

        evicted_frame = replacer.evict()
        assert evicted_frame == 1
        assert 1 not in replacer.nodes
        assert 1 not in replacer.evictable
        assert 2 in replacer.nodes

    def test_evict_empty_evictable(self) -> None:
        replacer = LRUKReplacer(k=2)
        replacer.add_node(1)
        with pytest.raises(Exception, match="no evictable nodes"):
            replacer.evict()
