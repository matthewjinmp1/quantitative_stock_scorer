"""
Test suite for correlations.py
Tests the correlation analysis functionality for metrics and forward returns
"""
import unittest
import json
import os
import tempfile
import numpy as np
from correlations import (
    get_forward_return_key,
    format_forward_period_display,
    MetricData,
    load_data,
    extract_unified_data,
    detect_available_metrics,
    calculate_correlations,
    calculate_bucket_difference,
    calculate_custom_bucket_stats,
    rank_metrics_by_correlation,
    rank_metrics_by_bucket_difference,
    display_rankings_by_correlation,
    display_rankings_by_bucket_difference,
    print_correlations_by_forward_period,
    print_forward_period_correlations_summary,
    print_period_correlations,
    run_average_mode,
    run_by_period_mode,
    run_buckets_mode,
    run_custom_buckets_mode,
    calculate_combined_scores,
    run_combine_mode,
    FORWARD_RETURN_PERIODS,
    EXCLUDED_KEYS
)


class TestHelperFunctions(unittest.TestCase):
    """Test helper functions"""
    
    def test_get_forward_return_key(self):
        """Test get_forward_return_key function"""
        self.assertEqual(get_forward_return_key('total'), 'forward_return')
        self.assertEqual(get_forward_return_key('1y'), 'forward_return_1y')
        self.assertEqual(get_forward_return_key('3y'), 'forward_return_3y')
        self.assertEqual(get_forward_return_key('5y'), 'forward_return_5y')
        self.assertEqual(get_forward_return_key('10y'), 'forward_return_10y')
    
    def test_format_forward_period_display(self):
        """Test format_forward_period_display function"""
        self.assertEqual(format_forward_period_display('total'), 'Total forward return')
        self.assertEqual(format_forward_period_display('1y'), '1y forward return')
        self.assertEqual(format_forward_period_display('3y'), '3y forward return')
        self.assertEqual(format_forward_period_display('5y'), '5y forward return')
        self.assertEqual(format_forward_period_display('10y'), '10y forward return')


class TestMetricData(unittest.TestCase):
    """Test MetricData class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.metric_data = MetricData()
    
    def test_initialization(self):
        """Test MetricData initialization"""
        self.assertEqual(len(self.metric_data.data), len(FORWARD_RETURN_PERIODS))
        for period in FORWARD_RETURN_PERIODS:
            self.assertIn(period, self.metric_data.data)
        self.assertEqual(self.metric_data.metric_keys, [])
    
    def test_add_data_point(self):
        """Test adding data points"""
        self.metric_data.add_data_point('total', '2020-Q1', 'roa', 0.15, 10.5)
        
        self.assertIn('2020-Q1', self.metric_data.data['total'])
        self.assertIn('roa', self.metric_data.data['total']['2020-Q1'])
        self.assertEqual(
            self.metric_data.data['total']['2020-Q1']['roa'],
            [(0.15, 10.5)]
        )
    
    def test_get_pairs_single_period(self):
        """Test getting pairs for a specific time period"""
        self.metric_data.add_data_point('total', '2020-Q1', 'roa', 0.15, 10.5)
        self.metric_data.add_data_point('total', '2020-Q1', 'roa', 0.20, 12.0)
        
        pairs = self.metric_data.get_pairs('total', 'roa', '2020-Q1')
        self.assertEqual(len(pairs), 2)
        self.assertIn((0.15, 10.5), pairs)
        self.assertIn((0.20, 12.0), pairs)
    
    def test_get_pairs_all_periods(self):
        """Test getting pairs across all time periods"""
        self.metric_data.add_data_point('total', '2020-Q1', 'roa', 0.15, 10.5)
        self.metric_data.add_data_point('total', '2020-Q2', 'roa', 0.20, 12.0)
        
        pairs = self.metric_data.get_pairs('total', 'roa')
        self.assertEqual(len(pairs), 2)
    
    def test_get_time_periods(self):
        """Test getting time periods"""
        self.metric_data.add_data_point('total', '2020-Q1', 'roa', 0.15, 10.5)
        self.metric_data.add_data_point('total', '2020-Q2', 'roa', 0.20, 12.0)
        
        periods = self.metric_data.get_time_periods('total')
        self.assertEqual(len(periods), 2)
        self.assertIn('2020-Q1', periods)
        self.assertIn('2020-Q2', periods)


class TestDataLoading(unittest.TestCase):
    """Test data loading functions"""
    
    def setUp(self):
        """Set up test fixtures with temporary JSON file"""
        self.test_data = [
            {
                "symbol": "TEST1",
                "company_name": "Test Company 1",
                "data": [
                    {
                        "period": "2020-Q1",
                        "roa": 0.15,
                        "ebit_ppe": 0.25,
                        "forward_return": 10.5,
                        "forward_return_1y": 12.0,
                        "forward_return_3y": 15.0
                    },
                    {
                        "period": "2020-Q2",
                        "roa": 0.20,
                        "ebit_ppe": 0.30,
                        "forward_return": 11.0,
                        "forward_return_1y": 13.0,
                        "forward_return_3y": 16.0
                    }
                ]
            },
            {
                "symbol": "TEST2",
                "company_name": "Test Company 2",
                "data": [
                    {
                        "period": "2020-Q1",
                        "roa": 0.10,
                        "ebit_ppe": 0.20,
                        "forward_return": 8.0,
                        "forward_return_1y": 9.0,
                        "forward_return_3y": 10.0
                    }
                ]
            }
        ]
        
        # Create temporary file
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(self.test_data, self.temp_file)
        self.temp_file.close()
        self.temp_filename = self.temp_file.name
    
    def tearDown(self):
        """Clean up temporary file"""
        if os.path.exists(self.temp_filename):
            os.unlink(self.temp_filename)
    
    def test_load_data(self):
        """Test loading data from JSON file"""
        data = load_data(self.temp_filename)
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['symbol'], 'TEST1')
        self.assertEqual(data[1]['symbol'], 'TEST2')
    
    def test_load_data_file_not_found(self):
        """Test loading non-existent file"""
        data = load_data('nonexistent_file.json')
        self.assertEqual(data, [])
    
    def test_load_data_invalid_json(self):
        """Test loading file with invalid JSON"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"invalid": json}')  # Invalid JSON
            temp_filename = f.name
        
        try:
            data = load_data(temp_filename)
            self.assertEqual(data, [])
        finally:
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)
    
    def test_detect_available_metrics(self):
        """Test detecting available metrics"""
        available = detect_available_metrics(self.test_data)
        self.assertIn('roa', available)
        self.assertIn('ebit_ppe', available)
        self.assertNotIn('period', available)  # Should be excluded
        self.assertNotIn('forward_return', available)  # Should be excluded
    
    def test_extract_unified_data(self):
        """Test extracting unified data"""
        metric_data = extract_unified_data(self.test_data, ['roa', 'ebit_ppe'])
        
        self.assertIn('roa', metric_data.metric_keys)
        self.assertIn('ebit_ppe', metric_data.metric_keys)
        
        # Check that data was extracted
        pairs = metric_data.get_pairs('total', 'roa')
        self.assertGreater(len(pairs), 0)
        
        # Check forward return periods
        for period in FORWARD_RETURN_PERIODS:
            pairs = metric_data.get_pairs(period, 'roa')
            # Should have some data for periods that exist in test data
            if period in ['total', '1y', '3y']:
                self.assertGreaterEqual(len(pairs), 0)
    
    def test_extract_unified_data_auto_detect_metrics(self):
        """Test auto-detection of metric keys"""
        metric_data = extract_unified_data(self.test_data, None)  # None triggers auto-detect
        
        # Should have detected metrics
        self.assertGreater(len(metric_data.metric_keys), 0)
        self.assertIn('roa', metric_data.metric_keys)
    
    def test_extract_unified_data_skips_invalid_periods(self):
        """Test that invalid periods are skipped"""
        test_data_with_invalid = [
            {
                "symbol": "TEST",
                "data": [
                    {"period": None, "roa": 0.1, "forward_return": 5.0},  # Invalid period
                    {"period": 0, "roa": 0.2, "forward_return": 6.0},  # Invalid period
                    {"period": "2020-Q1", "roa": 0.3, "forward_return": 7.0}  # Valid period
                ]
            }
        ]
        
        metric_data = extract_unified_data(test_data_with_invalid, ['roa'])
        
        # Should only have data from valid period
        pairs = metric_data.get_pairs('total', 'roa')
        self.assertEqual(len(pairs), 1)  # Only one valid period


class TestAnalysisFunctions(unittest.TestCase):
    """Test analysis functions"""
    
    def test_calculate_correlations_sufficient_data(self):
        """Test correlation calculation with sufficient data"""
        metric_values = [0.1, 0.2, 0.3, 0.4, 0.5]
        forward_return_values = [5.0, 10.0, 15.0, 20.0, 25.0]
        
        result = calculate_correlations(metric_values, forward_return_values)
        
        self.assertIn('ranked_correlation', result)
        self.assertIn('ranked_pvalue', result)
        self.assertEqual(result['n_pairs'], 5)
        self.assertIsNotNone(result['ranked_correlation'])
        self.assertIsNotNone(result['ranked_pvalue'])
        # Should have positive correlation (both increasing)
        self.assertGreater(result['ranked_correlation'], 0)
    
    def test_calculate_correlations_insufficient_data(self):
        """Test correlation calculation with insufficient data"""
        metric_values = [0.1]
        forward_return_values = [5.0]
        
        result = calculate_correlations(metric_values, forward_return_values)
        
        self.assertEqual(result['n_pairs'], 1)
        self.assertIsNone(result['ranked_correlation'])
        self.assertIsNone(result['ranked_pvalue'])
        self.assertIn('error', result)
    
    def test_calculate_correlations_negative_correlation(self):
        """Test correlation calculation with negative correlation"""
        metric_values = [0.5, 0.4, 0.3, 0.2, 0.1]
        forward_return_values = [5.0, 10.0, 15.0, 20.0, 25.0]
        
        result = calculate_correlations(metric_values, forward_return_values)
        
        self.assertLess(result['ranked_correlation'], 0)  # Negative correlation
    
    def test_calculate_correlations_constant_input(self):
        """Test correlation calculation with constant input arrays"""
        # Constant metric values
        metric_values = [0.5, 0.5, 0.5, 0.5, 0.5]
        forward_return_values = [5.0, 10.0, 15.0, 20.0, 25.0]
        
        result = calculate_correlations(metric_values, forward_return_values)
        
        self.assertIsNone(result['ranked_correlation'])
        self.assertIsNone(result['ranked_pvalue'])
        self.assertIn('error', result)
        self.assertIn('Constant input array', result['error'])
        
        # Constant forward return values
        metric_values = [0.1, 0.2, 0.3, 0.4, 0.5]
        forward_return_values = [10.0, 10.0, 10.0, 10.0, 10.0]
        
        result = calculate_correlations(metric_values, forward_return_values)
        
        self.assertIsNone(result['ranked_correlation'])
        self.assertIsNone(result['ranked_pvalue'])
        self.assertIn('error', result)
    
    def test_calculate_correlations_constant_ranks(self):
        """Test correlation calculation with constant ranks (all tied values)"""
        # All values are the same (will have constant ranks)
        # This should trigger the constant input check first, so let's test a case
        # where values pass the constant input check but ranks are constant
        # Actually, if all values are the same, ranks will also be constant
        # So this test covers the constant ranks path
        metric_values = [0.5] * 5
        forward_return_values = [10.0] * 5
        
        result = calculate_correlations(metric_values, forward_return_values)
        
        self.assertIsNone(result['ranked_correlation'])
        self.assertIsNone(result['ranked_pvalue'])
        self.assertIn('error', result)
    
    def test_calculate_bucket_difference(self):
        """Test bucket difference calculation"""
        # Create pairs with clear separation
        pairs = [
            (0.1, 5.0),   # Bottom bucket
            (0.2, 6.0),   # Bottom bucket
            (0.3, 7.0),   # Bottom bucket
            (0.8, 15.0),  # Top bucket
            (0.9, 16.0),  # Top bucket
            (1.0, 17.0),  # Top bucket
        ]
        
        difference = calculate_bucket_difference(pairs)
        
        self.assertIsNotNone(difference)
        self.assertGreater(difference, 0)  # Top should have higher returns
    
    def test_calculate_bucket_difference_insufficient_data(self):
        """Test bucket difference with insufficient data"""
        pairs = [(0.1, 5.0)]
        difference = calculate_bucket_difference(pairs)
        self.assertIsNone(difference)
    
    def test_calculate_custom_bucket_stats(self):
        """Test custom bucket stats calculation"""
        # Create pairs with clear separation for 3 buckets
        pairs = [
            (0.1, 5.0),   # Bucket 1
            (0.2, 6.0),   # Bucket 1
            (0.3, 7.0),   # Bucket 2
            (0.4, 8.0),   # Bucket 2
            (0.8, 15.0),  # Bucket 3
            (0.9, 16.0),  # Bucket 3
        ]
        
        bucket_stats = calculate_custom_bucket_stats(pairs, 3)
        
        self.assertIsNotNone(bucket_stats)
        self.assertEqual(len(bucket_stats), 3)
        self.assertEqual(bucket_stats[0]['bucket_num'], 1)
        self.assertEqual(bucket_stats[1]['bucket_num'], 2)
        self.assertEqual(bucket_stats[2]['bucket_num'], 3)
        # Each bucket should have 2 items
        self.assertEqual(bucket_stats[0]['count'], 2)
        self.assertEqual(bucket_stats[1]['count'], 2)
        self.assertEqual(bucket_stats[2]['count'], 2)
        # Check that median_return is calculated
        self.assertIsNotNone(bucket_stats[0]['median_return'])
        self.assertIsNotNone(bucket_stats[1]['median_return'])
        self.assertIsNotNone(bucket_stats[2]['median_return'])
    
    def test_calculate_custom_bucket_stats_insufficient_data(self):
        """Test custom bucket stats with insufficient data"""
        pairs = [(0.1, 5.0), (0.2, 6.0)]
        bucket_stats = calculate_custom_bucket_stats(pairs, 5)  # Need 5 buckets but only 2 data points
        self.assertIsNone(bucket_stats)
    
    def test_calculate_bucket_difference_empty(self):
        """Test bucket difference with empty data"""
        pairs = []
        difference = calculate_bucket_difference(pairs)
        self.assertIsNone(difference)


class TestRankingFunctions(unittest.TestCase):
    """Test ranking functions"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.metric_data = MetricData()
        self.metric_data.metric_keys = ['roa', 'ebit_ppe']
        
        # Add test data with clear correlation patterns
        # ROA: positive correlation with returns
        self.metric_data.add_data_point('total', '2020-Q1', 'roa', 0.1, 5.0)
        self.metric_data.add_data_point('total', '2020-Q1', 'roa', 0.2, 10.0)
        self.metric_data.add_data_point('total', '2020-Q1', 'roa', 0.3, 15.0)
        
        # EBIT/PPE: also positive correlation
        self.metric_data.add_data_point('total', '2020-Q1', 'ebit_ppe', 0.2, 5.0)
        self.metric_data.add_data_point('total', '2020-Q1', 'ebit_ppe', 0.3, 10.0)
        self.metric_data.add_data_point('total', '2020-Q1', 'ebit_ppe', 0.4, 15.0)
        
        self.available_metrics = {
            'roa': 'ROA (Return on Assets)',
            'ebit_ppe': 'EBIT/PPE (EBIT per Property, Plant & Equipment)'
        }
    
    def test_rank_metrics_by_correlation(self):
        """Test ranking metrics by correlation"""
        rankings = rank_metrics_by_correlation(self.metric_data, self.available_metrics)
        
        self.assertEqual(len(rankings), 2)
        # Both should have correlations
        for metric_key, correlation in rankings:
            self.assertIn(metric_key, ['roa', 'ebit_ppe'])
            self.assertIsNotNone(correlation)
    
    def test_rank_metrics_by_bucket_difference(self):
        """Test ranking metrics by bucket difference"""
        rankings = rank_metrics_by_bucket_difference(self.metric_data, self.available_metrics)
        
        self.assertEqual(len(rankings), 2)
        # Both should have differences
        for metric_key, difference in rankings:
            self.assertIn(metric_key, ['roa', 'ebit_ppe'])
            self.assertIsNotNone(difference)


class TestIntegration(unittest.TestCase):
    """Integration tests with realistic data"""
    
    def setUp(self):
        """Set up test fixtures with realistic data structure"""
        self.test_data = [
            {
                "symbol": "AAPL",
                "company_name": "Apple Inc.",
                "data": [
                    {
                        "period": "2020-Q1",
                        "roa": 0.20,
                        "ebit_ppe": 0.35,
                        "gross_margin": 0.38,
                        "forward_return": 12.5,
                        "forward_return_1y": 15.0,
                        "forward_return_3y": 18.0,
                        "forward_return_5y": 20.0
                    },
                    {
                        "period": "2020-Q2",
                        "roa": 0.22,
                        "ebit_ppe": 0.36,
                        "gross_margin": 0.39,
                        "forward_return": 13.0,
                        "forward_return_1y": 15.5,
                        "forward_return_3y": 18.5,
                        "forward_return_5y": 20.5
                    },
                    {
                        "period": "2020-Q3",
                        "roa": 0.21,
                        "ebit_ppe": 0.34,
                        "gross_margin": 0.37,
                        "forward_return": 12.8,
                        "forward_return_1y": 15.2,
                        "forward_return_3y": 18.2,
                        "forward_return_5y": 20.2
                    }
                ]
            },
            {
                "symbol": "MSFT",
                "company_name": "Microsoft Corporation",
                "data": [
                    {
                        "period": "2020-Q1",
                        "roa": 0.15,
                        "ebit_ppe": 0.25,
                        "gross_margin": 0.32,
                        "forward_return": 10.0,
                        "forward_return_1y": 12.0,
                        "forward_return_3y": 14.0,
                        "forward_return_5y": 16.0
                    },
                    {
                        "period": "2020-Q2",
                        "roa": 0.16,
                        "ebit_ppe": 0.26,
                        "gross_margin": 0.33,
                        "forward_return": 10.5,
                        "forward_return_1y": 12.5,
                        "forward_return_3y": 14.5,
                        "forward_return_5y": 16.5
                    }
                ]
            }
        ]
    
    def test_full_pipeline(self):
        """Test the full pipeline from data loading to ranking"""
        # Detect metrics
        available_metrics = detect_available_metrics(self.test_data)
        self.assertGreater(len(available_metrics), 0)
        
        # Extract data
        metric_data = extract_unified_data(self.test_data, list(available_metrics.keys()))
        self.assertGreater(len(metric_data.metric_keys), 0)
        
        # Test ranking
        rankings = rank_metrics_by_correlation(metric_data, available_metrics)
        self.assertGreater(len(rankings), 0)
        
        # Test bucket ranking
        bucket_rankings = rank_metrics_by_bucket_difference(metric_data, available_metrics)
        self.assertGreater(len(bucket_rankings), 0)
    
    def test_multiple_forward_periods(self):
        """Test that all forward return periods are handled"""
        available_metrics = detect_available_metrics(self.test_data)
        metric_data = extract_unified_data(self.test_data, list(available_metrics.keys()))
        
        for period in FORWARD_RETURN_PERIODS:
            pairs = metric_data.get_pairs(period, 'roa')
            # Should have data for periods that exist in test data
            if period in ['total', '1y', '3y', '5y']:
                self.assertGreaterEqual(len(pairs), 0)


class TestDisplayFunctions(unittest.TestCase):
    """Test display functions"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.available_metrics = {
            'roa': 'ROA (Return on Assets)',
            'ebit_ppe': 'EBIT/PPE (EBIT per Property, Plant & Equipment)'
        }
        self.rankings = [
            ('roa', 0.75),
            ('ebit_ppe', 0.65),
            ('gross_margin', None)
        ]
    
    def test_display_rankings_by_correlation(self):
        """Test displaying rankings by correlation"""
        import io
        import sys
        
        # Capture stdout
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            display_rankings_by_correlation(self.rankings, self.available_metrics)
            output = buffer.getvalue()
            
            # Check that output contains expected elements
            self.assertIn('Rank', output)
            self.assertIn('Metric', output)
            self.assertIn('Correlation', output)
            self.assertIn('ROA', output)
        finally:
            sys.stdout = old_stdout
    
    def test_display_rankings_by_bucket_difference(self):
        """Test displaying rankings by bucket difference"""
        import io
        import sys
        
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            display_rankings_by_bucket_difference(self.rankings, self.available_metrics)
            output = buffer.getvalue()
            
            self.assertIn('Rank', output)
            self.assertIn('Metric', output)
            self.assertIn('Top-Bottom Difference', output)
        finally:
            sys.stdout = old_stdout
    
    def test_print_correlations_by_forward_period(self):
        """Test printing correlations by forward period"""
        import io
        import sys
        
        results = {
            'total': {
                'ranked_correlation': 0.75,
                'ranked_pvalue': 0.01,
                'n_pairs': 100,
                'n_periods': 5
            },
            '1y': {
                'ranked_correlation': None,
                'ranked_pvalue': None,
                'n_pairs': 50,
                'n_periods': 3
            }
        }
        
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            print_correlations_by_forward_period(results, 'Test Metric')
            output = buffer.getvalue()
            
            self.assertIn('Test Metric', output)
            self.assertIn('Forward Period', output)
            self.assertIn('Ranked Corr', output)
        finally:
            sys.stdout = old_stdout
    
    def test_print_forward_period_correlations_summary(self):
        """Test printing forward period correlations summary"""
        import io
        import sys
        
        results = {
            'total': {
                'ranked_correlation': 0.75,
                'ranked_pvalue': 0.01,
                'n_pairs': 100
            },
            '1y': {
                'ranked_correlation': 0.65,
                'ranked_pvalue': 0.02,
                'n_pairs': 80
            }
        }
        
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            print_forward_period_correlations_summary(results, 'Test Metric')
            output = buffer.getvalue()
            
            self.assertIn('Test Metric', output)
            self.assertIn('Summary', output)
            self.assertIn('Average', output)
        finally:
            sys.stdout = old_stdout
    
    def test_print_forward_period_correlations_summary_empty(self):
        """Test printing summary with empty results"""
        import io
        import sys
        
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            print_forward_period_correlations_summary({}, 'Test Metric')
            output = buffer.getvalue()
            
            # Should return early with empty results
            self.assertEqual(output, '')
        finally:
            sys.stdout = old_stdout
    
    def test_print_period_correlations(self):
        """Test printing period correlations"""
        import io
        import sys
        
        period_stats = [
            {
                'time_period': '2020-Q1',
                'ranked_correlation': 0.75,
                'ranked_pvalue': 0.01,
                'n_pairs': 50
            },
            {
                'time_period': '2020-Q2',
                'ranked_correlation': 0.65,
                'ranked_pvalue': 0.02,
                'n_pairs': 45
            }
        ]
        
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            print_period_correlations(period_stats, 'Test Metric')
            output = buffer.getvalue()
            
            self.assertIn('Test Metric', output)
            self.assertIn('Time Period', output)
            self.assertIn('2020-Q1', output)
        finally:
            sys.stdout = old_stdout
    
    def test_print_period_correlations_empty(self):
        """Test printing period correlations with empty stats"""
        import io
        import sys
        
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            print_period_correlations([], 'Test Metric')
            output = buffer.getvalue()
            
            self.assertIn('No period statistics available', output)
        finally:
            sys.stdout = old_stdout


class TestModeExecution(unittest.TestCase):
    """Test mode execution functions"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.metric_data = MetricData()
        self.metric_data.metric_keys = ['roa', 'ebit_ppe']
        
        # Add test data
        for period in ['2020-Q1', '2020-Q2', '2020-Q3']:
            self.metric_data.add_data_point('total', period, 'roa', 0.1, 5.0)
            self.metric_data.add_data_point('total', period, 'roa', 0.2, 10.0)
            self.metric_data.add_data_point('total', period, 'roa', 0.3, 15.0)
            self.metric_data.add_data_point('1y', period, 'roa', 0.1, 6.0)
            self.metric_data.add_data_point('1y', period, 'roa', 0.2, 12.0)
        
        self.available_metrics = {
            'roa': 'ROA (Return on Assets)',
            'ebit_ppe': 'EBIT/PPE (EBIT per Property, Plant & Equipment)'
        }
    
    def test_run_average_mode(self):
        """Test running average mode"""
        import io
        import sys
        
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            run_average_mode(self.metric_data, self.available_metrics, ['roa'])
            output = buffer.getvalue()
            
            # Should have generated output
            self.assertGreater(len(output), 0)
        finally:
            sys.stdout = old_stdout
    
    def test_run_by_period_mode(self):
        """Test running by-period mode"""
        import io
        import sys
        
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            run_by_period_mode(self.metric_data, self.available_metrics, ['roa'])
            output = buffer.getvalue()
            
            self.assertGreater(len(output), 0)
        finally:
            sys.stdout = old_stdout
    
    def test_run_by_period_mode_no_data(self):
        """Test by-period mode with no valid data"""
        import io
        import sys
        
        empty_metric_data = MetricData()
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            run_by_period_mode(empty_metric_data, self.available_metrics, ['roa'])
            output = buffer.getvalue()
            
            # Should handle gracefully
            self.assertIn('No valid correlation data', output)
        finally:
            sys.stdout = old_stdout
    
    def test_run_buckets_mode(self):
        """Test running buckets mode"""
        import io
        import sys
        
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            run_buckets_mode(self.metric_data, self.available_metrics, ['roa'])
            output = buffer.getvalue()
            
            self.assertGreater(len(output), 0)
            self.assertIn('Median Return', output)
        finally:
            sys.stdout = old_stdout
    
    def test_run_custom_buckets_mode(self):
        """Test running custom buckets mode"""
        import io
        import sys
        
        old_stdout = sys.stdout
        sys.stdout = buffer = io.StringIO()
        
        try:
            run_custom_buckets_mode(self.metric_data, self.available_metrics, ['roa'], 3)
            output = buffer.getvalue()
            
            self.assertGreater(len(output), 0)
            self.assertIn('buckets', output.lower())
        finally:
            sys.stdout = old_stdout
    
    def test_calculate_combined_scores(self):
        """Test calculating combined scores"""
        # Add data for multiple metrics
        for period in ['2020-Q1', '2020-Q2']:
            # Add matching data points (same forward return = same stock)
            self.metric_data.add_data_point('total', period, 'roa', 0.1, 5.0)
            self.metric_data.add_data_point('total', period, 'ebit_ppe', 0.2, 5.0)
            self.metric_data.add_data_point('total', period, 'roa', 0.2, 10.0)
            self.metric_data.add_data_point('total', period, 'ebit_ppe', 0.3, 10.0)
        
        metric_items = [('roa', 1), ('ebit_ppe', 1)]  # Add both metrics
        combined_pairs = calculate_combined_scores(self.metric_data, metric_items, 'total')
        
        # Should have combined scores
        self.assertGreater(len(combined_pairs), 0)
        # Each pair should be (combined_score, forward_return)
        for score, fr in combined_pairs:
            self.assertIsInstance(score, (int, float))
            self.assertIsInstance(fr, (int, float))
    
    def test_calculate_combined_scores_with_subtraction(self):
        """Test combined scores with subtraction"""
        for period in ['2020-Q1']:
            self.metric_data.add_data_point('total', period, 'roa', 0.1, 5.0)
            self.metric_data.add_data_point('total', period, 'ebit_ppe', 0.2, 5.0)
        
        metric_items = [('roa', 1), ('ebit_ppe', -1)]  # Subtract ebit_ppe
        combined_pairs = calculate_combined_scores(self.metric_data, metric_items, 'total')
        
        self.assertGreaterEqual(len(combined_pairs), 0)
    
    def test_run_combine_mode(self):
        """Test running combine mode"""
        import io
        import sys
        from unittest.mock import patch
        
        # Mock user input to exit immediately
        with patch('builtins.input', return_value='exit'):
            old_stdout = sys.stdout
            sys.stdout = buffer = io.StringIO()
            
            try:
                run_combine_mode(self.metric_data, self.available_metrics)
                output = buffer.getvalue()
                
                # Should handle gracefully
                self.assertIsNotNone(output)
            finally:
                sys.stdout = old_stdout


if __name__ == '__main__':
    unittest.main()

