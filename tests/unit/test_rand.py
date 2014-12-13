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
        Test database creation
        '''
        rands = []
        for _ in range(0,100):
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
        Test database creation
        '''
        rands = []
        for _ in range(0,100):
            r_1 = sorbic.utils.rand.rand_raw_str(24)
            self.assertEqual(24, len(r_1))
            rands.append(r_1)
        for n_1 in range(0, 100):
            for n_2 in range(0, 100):
                if n_1 == n_2:
                    continue
                self.assertNotEqual(rands[n_1], rands[n_2])
