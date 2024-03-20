def test_sum():
    assert sum([1, 2, 3]) == 6, "Should be 6"

def test_sum_tuple():
    assert sum((10, 20, 30)) == 60, "Should be 60"

def test_all_sum_unit_tests():
    test_sum()
    test_sum_tuple()
