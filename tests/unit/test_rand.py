'''
Test sorbic.utils.rand
'''
# import sorbic libs
import sorbic.utils.rand
# Import python libs
import unittest


class TestRand(unittest.TestCase):
    '''
    Cover db funcs
    '''
    def test_rand_hex_strs(self):
        '''
        Test rand hex strings
        '''
        rands = []
        for _ in range(0, 100):
            r_1 = sorbic.utils.rand.rand_hex_str(24)
            self.assertEqual(24, len(r_1))
            rands.append(r_1)
        for n_1 in range(0, 100):
            for n_2 in range(0, 100):
                if n_1 == n_2:
                    continue
                self.assertNotEqual(rands[n_1], rands[n_2])

    def test_rand_raw_strs(self):
        '''
        Test rand raw strings
        '''
        rands = []
        for _ in range(0, 100):
            r_1 = sorbic.utils.rand.rand_raw_str(24)
            self.assertEqual(24, len(r_1))
            rands.append(r_1)
        for n_1 in range(0, 100):
            for n_2 in range(0, 100):
                if n_1 == n_2:
                    continue
                self.assertNotEqual(rands[n_1], rands[n_2])

    def test_id(self):
        '''
        Test id creation
        '''
        rands = []
        for _ in range(0, 1000):
            r_1 = sorbic.utils.rand.gen_id()
            rands.append(r_1)
        for n_1 in range(0, 1000):
            for n_2 in range(0, 1000):
                if n_1 == n_2:
                    continue
                self.assertNotEqual(rands[n_1], rands[n_2])

    def _test_id_too_many(self):
        '''
        Test way too much id creation
        '''
        rands = []
        for _ in range(0, 10000000):
            r_1 = sorbic.utils.rand.gen_id()
            rands.append(r_1)
        for n_1 in range(0, 10000000):
            for n_2 in range(0, 10000000):
                if n_1 == n_2:
                    continue
                self.assertNotEqual(rands[n_1], rands[n_2])
