"""
Test runner script that discovers and runs all test files in the project.
Usage: python run_tests.py
"""
import unittest
import sys
import os
from io import StringIO


def run_all_tests():
    """
    Discover and run all test files in the current directory.
    
    Returns:
        bool: True if all tests passed, False otherwise
    """
    # Get the current directory
    test_dir = os.path.dirname(os.path.abspath(__file__))
    
    print("=" * 80)
    print("Running All Tests")
    print("=" * 80)
    print(f"Test directory: {test_dir}")
    print()
    
    # Discover and load all test files
    # Pattern matches files starting with 'test_' and ending with '.py'
    loader = unittest.TestLoader()
    suite = loader.discover(
        start_dir=test_dir,
        pattern='test_*.py',
        top_level_dir=test_dir
    )
    
    # Count tests before running
    test_count = suite.countTestCases()
    print(f"Found {test_count} test(s) to run")
    print()
    
    # Run the tests with verbose output
    runner = unittest.TextTestRunner(
        verbosity=2,  # Verbose output
        buffer=True,  # Capture output
        stream=sys.stdout
    )
    
    # Run the test suite
    result = runner.run(suite)
    
    # Print summary
    print()
    print("=" * 80)
    print("Test Summary")
    print("=" * 80)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  - {test}")
    
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  - {test}")
    
    print("=" * 80)
    
    # Return True if all tests passed
    return result.wasSuccessful()


def list_test_files():
    """
    List all test files found in the current directory.
    """
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_files = []
    
    for file in os.listdir(test_dir):
        if file.startswith('test_') and file.endswith('.py'):
            test_files.append(file)
    
    if test_files:
        print("Test files found:")
        for test_file in sorted(test_files):
            print(f"  - {test_file}")
    else:
        print("No test files found (looking for files matching 'test_*.py')")
    
    return test_files


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Run all test files in the project',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_tests.py           # Run all tests
  python run_tests.py --list     # List all test files
        """
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all test files without running them'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Run tests with minimal output'
    )
    
    args = parser.parse_args()
    
    if args.list:
        list_test_files()
    else:
        if args.quiet:
            # Override verbosity for quiet mode
            unittest.TextTestRunner.verbosity = 0
        
        success = run_all_tests()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)

