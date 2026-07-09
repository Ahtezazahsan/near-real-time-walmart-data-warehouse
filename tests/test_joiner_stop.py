import unittest
from hybridjoin import StreamBuffer, MasterData, Joiner


class DummyLoader:
    def insert_enriched(self, rows):
        pass

    def flush_all(self):
        pass


class TestJoinerStop(unittest.TestCase):
    def test_stop_sets_event(self):
        sb = StreamBuffer()
        # Use the CSVs in project root for master data (they exist in the workspace)
        cm = MasterData('customer_master_data.csv', 'Customer_ID')
        pm = MasterData('product_master_data.csv', 'Product_ID')

        joiner = Joiner(sb, cm, pm, DummyLoader(), hS=10, vP=2)
        # Ensure stop method exists and sets the internal event flag
        self.assertTrue(hasattr(joiner, 'stop'))
        # Initially the event should be clear
        self.assertFalse(joiner._stop.is_set())
        # Calling stop should set it
        joiner.stop()
        self.assertTrue(joiner._stop.is_set())


if __name__ == '__main__':
    unittest.main()
