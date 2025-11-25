"""
Test coverage runner script that measures code coverage for all test files.
Usage: python run_coverage.py
"""
import subprocess
import sys
import os


def check_coverage_installed():
    """Check if coverage.py is installed"""
    try:
        import coverage
        return True
    except ImportError:
        return False


def install_coverage():
    """Install coverage.py if not available"""
    print("coverage.py is not installed. Installing...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "coverage"], 
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("coverage.py installed successfully!")
        return True
    except subprocess.CalledProcessError:
        print("Failed to install coverage.py. Please install manually: pip install coverage")
        return False


def run_coverage(html_report=True, show_missing=True, min_percent=None):
    """
    Run test coverage analysis.
    
    Args:
        html_report: Generate HTML coverage report
        show_missing: Show lines that are missing coverage
        min_percent: Minimum coverage percentage to require (None = no requirement)
    """
    # Check if coverage is installed
    if not check_coverage_installed():
        if not install_coverage():
            return False
    
    print("=" * 80)
    print("Running Test Coverage Analysis")
    print("=" * 80)
    print()
    
    # Get current directory
    test_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Find all Python source files (excluding test files and __pycache__)
    source_files = []
    for root, dirs, files in os.walk(test_dir):
        # Skip __pycache__ directories
        dirs[:] = [d for d in dirs if d != '__pycache__']
        
        for file in files:
            if file.endswith('.py') and not file.startswith('test_') and file != 'run_tests.py' and file != 'run_coverage.py':
                filepath = os.path.join(root, file)
                source_files.append(filepath)
    
    print(f"Source files to analyze: {len(source_files)}")
    for f in source_files:
        print(f"  - {os.path.basename(f)}")
    print()
    
    # Find all test files
    test_files = []
    for file in os.listdir(test_dir):
        if file.startswith('test_') and file.endswith('.py'):
            test_files.append(file)
    
    print(f"Test files found: {len(test_files)}")
    for f in test_files:
        print(f"  - {f}")
    print()
    
    # Run coverage
    print("Running tests with coverage...")
    print("-" * 80)
    
    # Build coverage command
    coverage_cmd = [
        sys.executable, "-m", "coverage", "run",
        "--source", test_dir,
        "-m", "unittest", "discover",
        "-s", test_dir,
        "-p", "test_*.py"
    ]
    
    try:
        result = subprocess.run(coverage_cmd, cwd=test_dir, capture_output=True, text=True)
        
        if result.returncode != 0:
            print("Tests failed during coverage run:")
            print(result.stdout)
            print(result.stderr)
            return False
        
        print("Tests completed successfully!")
        print()
        
        # Generate report
        print("=" * 80)
        print("Coverage Report")
        print("=" * 80)
        print()
        
        # Terminal report
        report_cmd = [sys.executable, "-m", "coverage", "report"]
        if show_missing:
            report_cmd.append("--show-missing")
        
        result = subprocess.run(report_cmd, cwd=test_dir, capture_output=True, text=True)
        print(result.stdout)
        
        if result.stderr:
            print(result.stderr)
        
        # Check coverage percentage
        if min_percent is not None:
            # Extract total coverage from report
            lines = result.stdout.split('\n')
            for line in lines:
                if 'TOTAL' in line:
                    try:
                        # Parse percentage from line like "TOTAL    1234    567    54%"
                        parts = line.split()
                        for i, part in enumerate(parts):
                            if part.endswith('%'):
                                coverage_pct = float(part.rstrip('%'))
                                if coverage_pct < min_percent:
                                    print(f"\n⚠️  Coverage {coverage_pct:.1f}% is below required {min_percent}%")
                                    return False
                                else:
                                    print(f"\n✅ Coverage {coverage_pct:.1f}% meets requirement of {min_percent}%")
                                break
                    except (ValueError, IndexError):
                        pass
                    break
        
        # HTML report
        if html_report:
            print()
            print("=" * 80)
            print("Generating HTML Coverage Report")
            print("=" * 80)
            
            html_cmd = [sys.executable, "-m", "coverage", "html"]
            result = subprocess.run(html_cmd, cwd=test_dir, capture_output=True, text=True)
            
            html_dir = os.path.join(test_dir, "htmlcov")
            if os.path.exists(html_dir):
                index_file = os.path.join(html_dir, "index.html")
                print(f"\nHTML report generated: {index_file}")
                print(f"Open in browser: file://{os.path.abspath(index_file)}")
            else:
                print("HTML report generation may have failed")
        
        print()
        print("=" * 80)
        print("Coverage analysis complete!")
        print("=" * 80)
        
        return True
        
    except Exception as e:
        print(f"Error running coverage: {e}")
        return False


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Run test coverage analysis for all test files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_coverage.py              # Run coverage with HTML report
  python run_coverage.py --no-html    # Run coverage without HTML report
  python run_coverage.py --min 80     # Require minimum 80% coverage
        """
    )
    parser.add_argument(
        '--no-html',
        action='store_true',
        help='Skip HTML report generation'
    )
    parser.add_argument(
        '--no-missing',
        action='store_true',
        help='Do not show missing lines in report'
    )
    parser.add_argument(
        '--min',
        type=float,
        help='Minimum coverage percentage required (e.g., 80 for 80%%)'
    )
    
    args = parser.parse_args()
    
    success = run_coverage(
        html_report=not args.no_html,
        show_missing=not args.no_missing,
        min_percent=args.min
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

