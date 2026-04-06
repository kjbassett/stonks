import torch
import unittest

class TestCudaIsAvailable(unittest.TestCase):
    def test_cuda_available(self):
        assert torch.cuda.is_available()