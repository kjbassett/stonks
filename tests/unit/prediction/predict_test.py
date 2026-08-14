"""Unit tests for src/prediction/predict.py."""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd

from src.prediction.predict import _resolve_symbols, prepare_message


class TestResolveSymbols(unittest.IsolatedAsyncioTestCase):
    async def test_watchlist_keyword_resolves_via_watchlist_module(self):
        with patch(
            "src.data_sources.watchlist.get_watchlist_and_held_symbols",
            new=AsyncMock(return_value=["AAPL", "MSFT"]),
        ):
            result = await _resolve_symbols("watchlist")
        self.assertEqual(result, ["AAPL", "MSFT"])

    async def test_comma_separated_symbols_uppercased_and_stripped(self):
        result = await _resolve_symbols(" aapl, msft ,goog")
        self.assertEqual(result, ["AAPL", "MSFT", "GOOG"])

    async def test_empty_string_returns_empty_list(self):
        result = await _resolve_symbols("")
        self.assertEqual(result, [])

    async def test_whitespace_only_returns_empty_list(self):
        result = await _resolve_symbols("   ")
        self.assertEqual(result, [])


class TestPrepareMessage(unittest.TestCase):
    def test_formats_symbol_and_timestamp(self):
        df = pd.DataFrame([{"symbol": "AAPL", "timestamp": 1700000000}])
        result = prepare_message(df)
        self.assertTrue(result.startswith("AAPL, "))

    def test_joins_multiple_rows_with_newline(self):
        df = pd.DataFrame([
            {"symbol": "AAPL", "timestamp": 1700000000},
            {"symbol": "MSFT", "timestamp": 1700000000},
        ])
        result = prepare_message(df)
        self.assertEqual(len(result.split("\n")), 2)


class TestPredictLatestDataSendResults(unittest.IsolatedAsyncioTestCase):
    """Regression test: send_results=True used to reference a nonexistent
    'uncertainty' column and would raise KeyError; must use variance instead."""

    async def _run(self, predictions, recipients="a@b.com"):
        mock_model = MagicMock()
        mock_model.folder = "organisms/stonk/v1"
        mock_model.run = AsyncMock(return_value=predictions)

        mock_model_dao = MagicMock()
        mock_model_dao.get_id = AsyncMock(return_value=1)
        mock_prediction_dao = MagicMock()
        mock_prediction_dao.save = AsyncMock()

        with patch(
            "src.prediction.predict.Organism.load", return_value=mock_model
        ), patch(
            "src.prediction.predict.config", {"organism_folder": "organisms"}
        ), patch(
            "src.prediction.predict.dao_manager"
        ) as mock_dm, patch(
            "src.prediction.predict.send_email"
        ) as mock_send_email:
            mock_dm.get_dao.side_effect = lambda name: (
                mock_model_dao if name == "Model" else mock_prediction_dao
            )
            from src.prediction.predict import predict_latest_data
            await predict_latest_data("stonk", send_results=True, recipients=recipients)
        return mock_send_email

    async def test_uses_variance_not_uncertainty_column(self):
        # High prediction, tiny variance -> prediction / sqrt(variance) > 2.5 -> included.
        # (Would previously raise KeyError: 'uncertainty' before this fix.)
        predictions = pd.DataFrame([
            {"symbol": "AAPL", "timestamp": 1700000000, "prediction": 0.1, "variance": 0.0001},
        ])
        mock_send_email = await self._run(predictions)
        mock_send_email.assert_called_once()
        _subject, body, _recipients = mock_send_email.call_args.args
        self.assertIn("AAPL", body)

    async def test_filters_out_low_confidence_predictions(self):
        # Low prediction, high variance -> ratio well under 2.5 -> excluded.
        predictions = pd.DataFrame([
            {"symbol": "AAPL", "timestamp": 1700000000, "prediction": 0.001, "variance": 1.0},
        ])
        mock_send_email = await self._run(predictions)
        _subject, body, _recipients = mock_send_email.call_args.args
        self.assertEqual(body, "")

    async def test_raises_when_no_recipients(self):
        predictions = pd.DataFrame([
            {"symbol": "AAPL", "timestamp": 1700000000, "prediction": 0.1, "variance": 0.0001},
        ])
        with self.assertRaises(ValueError):
            await self._run(predictions, recipients=None)


if __name__ == "__main__":
    unittest.main()
