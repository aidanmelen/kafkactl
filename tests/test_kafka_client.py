from unittest.mock import patch
from kafkactl import KafkaClient

import mock
import logging
import unittest
import json


class TestKafkaClient(unittest.TestCase):
    def setUp(self):
        self.kafkactl = KafkaClient()

    def test_init(self):
        # Test default initialization
        kc = KafkaClient()
    
    


if __name__ == "__main__":
    unittest.main()
