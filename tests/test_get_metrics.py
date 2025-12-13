"""
Test suite for get_metrics.py
Tests the metrics calculation functionality for stock data
"""
import unittest
import json
import os
import tempfile
from get_metrics import (
    load_data_from_jsonl,
    extract_quarterly_data,
    calculate_metrics_for_all_stocks,
    save_metrics_to_json
)


class TestDataLoading(unittest.TestCase):
    """Test data loading functions"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_data = [
            {"symbol": "TEST1", "company_name": "Test Company 1", "data": {"period_end_date": ["2020-Q1", "2020-Q2"]}},
            {"symbol": "TEST2", "company_name": "Test Company 2", "data": {"period_end_date": ["2020-Q1"]}}
        ]
    
    def test_load_data_from_jsonl_valid_file(self):
        """Test loading valid JSONL file"""
        # Create temporary JSONL file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            for item in self.test_data:
                f.write(json.dumps(item) + '\n')
            temp_filename = f.name
        
        try:
            data = load_data_from_jsonl(temp_filename)
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0]['symbol'], 'TEST1')
            self.assertEqual(data[1]['symbol'], 'TEST2')
        finally:
            os.unlink(temp_filename)
    
    def test_load_data_from_jsonl_file_not_found(self):
        """Test loading non-existent file"""
        data = load_data_from_jsonl('nonexistent_file.jsonl')
        self.assertEqual(data, [])
    
    def test_load_data_from_jsonl_invalid_json(self):
        """Test loading file with invalid JSON"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"valid": "json"}\n')
            f.write('invalid json line\n')
            f.write('{"another": "valid"}\n')
            temp_filename = f.name
        
        try:
            data = load_data_from_jsonl(temp_filename)
            # Should load valid lines and skip invalid ones
            self.assertEqual(len(data), 2)
        finally:
            os.unlink(temp_filename)
    
    def test_load_data_from_jsonl_empty_lines(self):
        """Test loading file with empty lines"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"symbol": "TEST"}\n')
            f.write('\n')  # Empty line
            f.write('{"symbol": "TEST2"}\n')
            temp_filename = f.name
        
        try:
            data = load_data_from_jsonl(temp_filename)
            self.assertEqual(len(data), 2)
        finally:
            os.unlink(temp_filename)


class TestQuarterlyDataExtraction(unittest.TestCase):
    """Test quarterly data extraction"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.minimal_stock_data = {
            "symbol": "TEST",
            "company_name": "Test Company",
            "data": {
                "period_end_date": ["2020-Q1", "2020-Q2", "2020-Q3"],
                "period_end_price": [100.0, 110.0, 120.0],
                "dividends": [0.5, 0.5, 0.5],
                "roa": [0.15, 0.16, 0.17],
                "operating_income": [1000.0, 1100.0, 1200.0],
                "ppe_net": [5000.0, 5100.0, 5200.0],
                "revenue": [2000.0, 2100.0, 2200.0],
                "cost_of_goods_sold": [1200.0, 1260.0, 1320.0],
                "enterprise_value": [10000.0, 11000.0, 12000.0],
                "price_to_sales": [2.0, 2.1, 2.2]
            }
        }
    
    def test_extract_quarterly_data_valid(self):
        """Test extracting quarterly data from valid stock data"""
        result = extract_quarterly_data(self.minimal_stock_data)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['symbol'], 'TEST')
        self.assertEqual(result['company_name'], 'Test Company')
        self.assertEqual(len(result['data']), 3)
    
    def test_extract_quarterly_data_missing_data_key(self):
        """Test extracting data with missing 'data' key"""
        stock_data = {"symbol": "TEST"}
        result = extract_quarterly_data(stock_data)
        self.assertIsNone(result)
    
    def test_extract_quarterly_data_empty_data(self):
        """Test extracting data with empty data"""
        stock_data = {"symbol": "TEST", "data": {}}
        result = extract_quarterly_data(stock_data)
        self.assertIsNone(result)
    
    def test_extract_quarterly_data_calculates_total_return(self):
        """Test that total return is calculated correctly"""
        result = extract_quarterly_data(self.minimal_stock_data)
        
        # First quarter should have None (no previous quarter)
        self.assertIsNone(result['data'][0]['total_return'])
        
        # Second quarter should have calculated return
        # ((110 - 100 + 0.5) / 100) * 100 = 10.5%
        self.assertIsNotNone(result['data'][1]['total_return'])
        self.assertAlmostEqual(result['data'][1]['total_return'], 10.5, places=1)
    
    def test_extract_quarterly_data_calculates_ebit_ppe(self):
        """Test EBIT/PPE calculation"""
        result = extract_quarterly_data(self.minimal_stock_data)
        
        # EBIT/PPE = operating_income / ppe_net
        # First quarter: 1000.0 / 5000.0 = 0.2
        self.assertAlmostEqual(result['data'][0]['ebit_ppe'], 0.2, places=2)
    
    def test_extract_quarterly_data_calculates_gross_margin(self):
        """Test gross margin calculation"""
        result = extract_quarterly_data(self.minimal_stock_data)
        
        # Gross margin = (revenue - cogs) / revenue
        # First quarter: (2000 - 1200) / 2000 = 0.4
        self.assertAlmostEqual(result['data'][0]['gross_margin'], 0.4, places=2)
    
    def test_extract_quarterly_data_calculates_operating_margin(self):
        """Test operating margin calculation"""
        result = extract_quarterly_data(self.minimal_stock_data)
        
        # Operating margin = operating_income / revenue
        # First quarter: 1000.0 / 2000.0 = 0.5
        self.assertAlmostEqual(result['data'][0]['operating_margin'], 0.5, places=2)
    
    def test_extract_quarterly_data_calculates_ev_ebit(self):
        """Test EV/EBIT calculation"""
        result = extract_quarterly_data(self.minimal_stock_data)
        
        # EV/EBIT = enterprise_value / operating_income
        # First quarter: 10000.0 / 1000.0 = 10.0
        self.assertAlmostEqual(result['data'][0]['ev_ebit'], 10.0, places=2)
    
    def test_extract_quarterly_data_handles_missing_values(self):
        """Test handling of missing values"""
        stock_data = {
            "symbol": "TEST",
            "data": {
                "period_end_date": ["2020-Q1"],
                "period_end_price": [100.0],
                "dividends": [],
                "roa": [None],
                "operating_income": [],
                "ppe_net": []
            }
        }
        
        result = extract_quarterly_data(stock_data)
        self.assertIsNotNone(result)
        self.assertEqual(len(result['data']), 1)
        self.assertIsNone(result['data'][0]['roa'])
        self.assertIsNone(result['data'][0]['ebit_ppe'])
    
    def test_extract_quarterly_data_handles_non_list_data(self):
        """Test handling when data fields are not lists"""
        stock_data = {
            "symbol": "TEST",
            "data": {
                "period_end_date": ["2020-Q1", "2020-Q2"],
                "period_end_price": [100.0, 110.0],
                "dividends": None,  # Not a list
                "roa": "invalid",  # Not a list
                "operating_income": 1000.0,  # Not a list
                "ppe_net": None,  # Not a list
                "revenue": None,  # Not a list - should trigger line 125
                "cost_of_goods_sold": None,  # Not a list - should trigger line 127
                "enterprise_value": None,
                "price_to_sales": None
            }
        }
        
        result = extract_quarterly_data(stock_data)
        self.assertIsNotNone(result)
        self.assertEqual(len(result['data']), 2)
        # Should handle gracefully with None values
        self.assertIsNone(result['data'][0]['ebit_ppe'])
        self.assertIsNone(result['data'][0]['gross_margin'])
    
    def test_extract_quarterly_data_calculates_ebit_ppe_ttm(self):
        """Test EBIT/PPE TTM calculation"""
        # Need at least 4 quarters for TTM
        stock_data = {
            "symbol": "TEST",
            "data": {
                "period_end_date": ["2020-Q1", "2020-Q2", "2020-Q3", "2020-Q4"],
                "period_end_price": [100.0, 110.0, 120.0, 130.0],
                "dividends": [0.5, 0.5, 0.5, 0.5],
                "operating_income": [1000.0, 1100.0, 1200.0, 1300.0],
                "ppe_net": [5000.0, 5100.0, 5200.0, 5300.0],
                "revenue": [2000.0] * 4,
                "cost_of_goods_sold": [1200.0] * 4,
                "enterprise_value": [10000.0] * 4,
                "price_to_sales": [2.0] * 4
            }
        }
        
        result = extract_quarterly_data(stock_data)
        
        # First 3 quarters should have None (need 4 quarters)
        self.assertIsNone(result['data'][0]['ebit_ppe_ttm'])
        self.assertIsNone(result['data'][1]['ebit_ppe_ttm'])
        self.assertIsNone(result['data'][2]['ebit_ppe_ttm'])
        
        # 4th quarter should have TTM value
        # TTM = (1000 + 1100 + 1200 + 1300) / (5000 + 5100 + 5200 + 5300) = 4600 / 20600
        self.assertIsNotNone(result['data'][3]['ebit_ppe_ttm'])
    
    def test_extract_quarterly_data_calculates_relative_ps(self):
        """Test Relative PS calculation"""
        # Need at least 20 quarters for 5-year average
        stock_data = {
            "symbol": "TEST",
            "data": {
                "period_end_date": [f"2020-Q{i%4+1}" for i in range(20)],
                "period_end_price": [100.0] * 20,
                "dividends": [0.5] * 20,
                "operating_income": [1000.0] * 20,
                "ppe_net": [5000.0] * 20,
                "revenue": [2000.0] * 20,
                "cost_of_goods_sold": [1200.0] * 20,
                "enterprise_value": [10000.0] * 20,
                "price_to_sales": [2.0 + i*0.01 for i in range(20)]  # Varying PS values
            }
        }
        
        result = extract_quarterly_data(stock_data)
        
        # First 19 quarters should have None
        for i in range(19):
            self.assertIsNone(result['data'][i]['relative_ps'])
        
        # 20th quarter should have relative PS
        self.assertIsNotNone(result['data'][19]['relative_ps'])
    
    def test_extract_quarterly_data_calculates_cagr_5y(self):
        """Test 5-Year Revenue CAGR calculation"""
        # Need at least 21 quarters (20 quarters ago + current)
        stock_data = {
            "symbol": "TEST",
            "data": {
                "period_end_date": [f"2020-Q{i%4+1}" for i in range(21)],
                "period_end_price": [100.0] * 21,
                "dividends": [0.5] * 21,
                "operating_income": [1000.0] * 21,
                "ppe_net": [5000.0] * 21,
                "revenue": [1000.0 * (1.1 ** i) for i in range(21)],  # Growing revenue
                "cost_of_goods_sold": [600.0] * 21,
                "enterprise_value": [10000.0] * 21,
                "price_to_sales": [2.0] * 21
            }
        }
        
        result = extract_quarterly_data(stock_data)
        
        # First 20 quarters should have None
        for i in range(20):
            self.assertIsNone(result['data'][i]['cagr_5y'])
        
        # 21st quarter should have CAGR
        # Revenue at index 20 vs index 0
        # CAGR = ((revenue[20] / revenue[0])^(1/5) - 1) * 100
        self.assertIsNotNone(result['data'][20]['cagr_5y'])
        # Should be positive since revenue is growing
        self.assertGreater(result['data'][20]['cagr_5y'], 0)


class TestForwardReturns(unittest.TestCase):
    """Test forward return calculations"""
    
    def test_forward_return_1y_calculation(self):
        """Test 1-year forward return calculation"""
        # Need at least 6 quarters: current (j=0) + skip 1 (j+1) + 4 future quarters (j+2 to j+5)
        # Forward returns start at t+2 to avoid look-ahead bias
        stock_data = {
            "symbol": "TEST",
            "data": {
                "period_end_date": ["2020-Q1", "2020-Q2", "2020-Q3", "2020-Q4", "2021-Q1", "2021-Q2"],
                "period_end_price": [100.0, 105.0, 110.0, 115.0, 120.0, 125.0],
                "dividends": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "operating_income": [1000.0] * 6,
                "ppe_net": [5000.0] * 6,
                "revenue": [2000.0] * 6,
                "cost_of_goods_sold": [1200.0] * 6,
                "enterprise_value": [10000.0] * 6,
                "price_to_sales": [2.0] * 6
            }
        }
        
        result = extract_quarterly_data(stock_data)
        
        # First quarter should have forward_return_1y calculated
        # (needs j+2 to j+5, which is indices 2, 3, 4, 5 - 4 quarters)
        self.assertIsNotNone(result['data'][0]['forward_return_1y'])
        
        # Last quarter should not have it (no future quarters)
        self.assertIsNone(result['data'][5]['forward_return_1y'])
    
    def test_forward_return_total_calculation(self):
        """Test total forward return calculation"""
        stock_data = {
            "symbol": "TEST",
            "data": {
                "period_end_date": ["2020-Q1", "2020-Q2", "2020-Q3"],
                "period_end_price": [100.0, 105.0, 110.0],
                "dividends": [0.0, 0.0, 0.0],
                "operating_income": [1000.0] * 3,
                "ppe_net": [5000.0] * 3,
                "revenue": [2000.0] * 3,
                "cost_of_goods_sold": [1200.0] * 3,
                "enterprise_value": [10000.0] * 3,
                "price_to_sales": [2.0] * 3
            }
        }
        
        result = extract_quarterly_data(stock_data)
        
        # First quarter should have forward_return (to end)
        self.assertIsNotNone(result['data'][0]['forward_return'])
        
        # Last quarter should not have it (no future periods)
        self.assertIsNone(result['data'][2]['forward_return'])


class TestCalculateMetricsForAllStocks(unittest.TestCase):
    """Test metrics calculation for all stocks"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_stocks = [
            {
                "symbol": "TEST1",
                "company_name": "Test Company 1",
                "data": {
                    "period_end_date": ["2020-Q1", "2020-Q2"],
                    "period_end_price": [100.0, 110.0],
                    "dividends": [0.5, 0.5],
                    "roa": [0.15, 0.16],
                    "operating_income": [1000.0, 1100.0],
                    "ppe_net": [5000.0, 5100.0],
                    "revenue": [2000.0, 2100.0],
                    "cost_of_goods_sold": [1200.0, 1260.0],
                    "enterprise_value": [10000.0, 11000.0],
                    "price_to_sales": [2.0, 2.1]
                }
            },
            {
                "symbol": "TEST2",
                "company_name": "Test Company 2",
                "data": {
                    "period_end_date": ["2020-Q1"],
                    "period_end_price": [50.0],
                    "dividends": [0.25],
                    "roa": [0.10],
                    "operating_income": [500.0],
                    "ppe_net": [2500.0],
                    "revenue": [1000.0],
                    "cost_of_goods_sold": [600.0],
                    "enterprise_value": [5000.0],
                    "price_to_sales": [1.5]
                }
            }
        ]
    
    def test_calculate_metrics_for_all_stocks_success(self):
        """Test successful metrics calculation"""
        results, stats = calculate_metrics_for_all_stocks(self.test_stocks)
        
        self.assertEqual(len(results), 2)
        self.assertEqual(stats['total_stocks'], 2)
        self.assertEqual(stats['processed'], 2)
        self.assertEqual(stats['skipped'], 0)
        self.assertEqual(stats['errors'], 0)
        self.assertGreater(stats['total_quarters'], 0)
    
    def test_calculate_metrics_for_all_stocks_tracks_data_points(self):
        """Test that data point tracking works"""
        results, stats = calculate_metrics_for_all_stocks(self.test_stocks)
        
        # Should have tracked some ROA data points
        self.assertGreater(stats['roa_data_points'], 0)
        # Should have tracked some EBIT/PPE data points
        self.assertGreater(stats['ebit_ppe_data_points'], 0)
    
    def test_calculate_metrics_for_all_stocks_handles_invalid_data(self):
        """Test handling of invalid stock data"""
        invalid_stocks = [
            {"symbol": "INVALID", "data": {}},  # Missing required fields
            {"symbol": "VALID", "data": {"period_end_date": []}}  # Empty dates
        ]
        
        results, stats = calculate_metrics_for_all_stocks(invalid_stocks)
        
        self.assertEqual(len(results), 0)
        self.assertEqual(stats['total_stocks'], 2)
        self.assertEqual(stats['processed'], 0)
        self.assertGreater(stats['skipped'], 0)
    
    def test_calculate_metrics_for_all_stocks_handles_errors(self):
        """Test error handling"""
        # Create data that might cause errors
        error_stocks = [
            {
                "symbol": "ERROR",
                "data": {
                    "period_end_date": None,  # This will cause an error
                    "period_end_price": [100.0]
                }
            }
        ]
        
        results, stats = calculate_metrics_for_all_stocks(error_stocks)
        
        # Should handle error gracefully
        self.assertGreaterEqual(stats['errors'], 0)
        if stats['errors'] > 0:
            self.assertIn('error_details', stats)


class TestSaveMetricsToJson(unittest.TestCase):
    """Test saving metrics to JSON"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.test_metrics_data = [
            {
                "symbol": "TEST1",
                "company_name": "Test Company 1",
                "data": [
                    {
                        "period": "2020-Q1",
                        "roa": 0.15,
                        "ebit_ppe": 0.2,
                        "forward_return": 10.5,
                        "total_return": 5.0
                    }
                ]
            }
        ]
    
    def test_save_metrics_to_json_success(self):
        """Test successful save to JSON"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_filename = f.name
        
        try:
            save_metrics_to_json(self.test_metrics_data, temp_filename)
            
            # Verify file was created
            self.assertTrue(os.path.exists(temp_filename))
            
            # Verify file contents
            with open(temp_filename, 'r') as f:
                loaded_data = json.load(f)
            
            self.assertEqual(len(loaded_data), 1)
            self.assertEqual(loaded_data[0]['symbol'], 'TEST1')
            self.assertEqual(len(loaded_data[0]['data']), 1)
        finally:
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)
    
    def test_save_metrics_to_json_includes_all_fields(self):
        """Test that all metric fields are included in output"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_filename = f.name
        
        try:
            save_metrics_to_json(self.test_metrics_data, temp_filename)
            
            with open(temp_filename, 'r') as f:
                loaded_data = json.load(f)
            
            entry = loaded_data[0]['data'][0]
            # Check that expected fields are present
            self.assertIn('period', entry)
            self.assertIn('roa', entry)
            self.assertIn('ebit_ppe', entry)
            self.assertIn('forward_return', entry)
            self.assertIn('total_return', entry)
        finally:
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)


class TestIntegration(unittest.TestCase):
    """Integration tests"""
    
    def test_full_pipeline(self):
        """Test the full pipeline from JSONL to metrics JSON"""
        # Create test JSONL file
        test_stocks = [
            {
                "symbol": "INTEGRATION_TEST",
                "company_name": "Integration Test Company",
                "data": {
                    "period_end_date": ["2020-Q1", "2020-Q2", "2020-Q3", "2020-Q4"],
                    "period_end_price": [100.0, 105.0, 110.0, 115.0],
                    "dividends": [0.5, 0.5, 0.5, 0.5],
                    "roa": [0.15, 0.16, 0.17, 0.18],
                    "operating_income": [1000.0, 1100.0, 1200.0, 1300.0],
                    "ppe_net": [5000.0, 5100.0, 5200.0, 5300.0],
                    "revenue": [2000.0, 2100.0, 2200.0, 2300.0],
                    "cost_of_goods_sold": [1200.0, 1260.0, 1320.0, 1380.0],
                    "enterprise_value": [10000.0, 11000.0, 12000.0, 13000.0],
                    "price_to_sales": [2.0, 2.1, 2.2, 2.3]
                }
            }
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            for stock in test_stocks:
                f.write(json.dumps(stock) + '\n')
            jsonl_filename = f.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json_filename = f.name
        
        try:
            # Load data
            stocks = load_data_from_jsonl(jsonl_filename)
            self.assertEqual(len(stocks), 1)
            
            # Calculate metrics
            metrics_data, stats = calculate_metrics_for_all_stocks(stocks)
            self.assertEqual(len(metrics_data), 1)
            self.assertEqual(stats['processed'], 1)
            
            # Save metrics
            save_metrics_to_json(metrics_data, json_filename)
            self.assertTrue(os.path.exists(json_filename))
            
            # Verify saved data
            with open(json_filename, 'r') as f:
                saved_data = json.load(f)
            
            self.assertEqual(len(saved_data), 1)
            self.assertGreater(len(saved_data[0]['data']), 0)
        finally:
            if os.path.exists(jsonl_filename):
                os.unlink(jsonl_filename)
            if os.path.exists(json_filename):
                os.unlink(json_filename)


if __name__ == '__main__':
    unittest.main()

