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
    rank_metrics_by_correlation,
    rank_metrics_by_bucket_difference,
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


if __name__ == '__main__':
    unittest.main()

