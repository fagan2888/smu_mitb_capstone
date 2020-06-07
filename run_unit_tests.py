from unittest import TextTestRunner, TestLoader
import unit_tests as unit_tests

def suite():
    suite = TestLoader().loadTestsFromModule(module=unit_tests)

    return suite

if __name__ == '__main__':
    runner = TextTestRunner()
    runner.run(suite())