"""Test period parsing utilities."""
import pytest
from datawarp.utils.period import parse_period, parse_period_range


class TestParsePeriodExisting:
    """Existing behavior - must continue working."""

    def test_standard_month_year(self):
        assert parse_period("december-2025") == "2025-12"
        assert parse_period("nov 2024") == "2024-11"
        assert parse_period("2024-11") == "2024-11"

    def test_iso_formats(self):
        assert parse_period("2024_11") == "2024-11"
        assert parse_period("2024/11") == "2024-11"

    def test_with_day(self):
        assert parse_period("31-december-2025") == "2025-12"

    def test_compact_formats(self):
        assert parse_period("202411") == "2024-11"
        assert parse_period("122025") == "2025-12"

    def test_abbreviated(self):
        assert parse_period("nov25") == "2025-11"
        assert parse_period("aug2025") == "2025-08"

    def test_quarterly(self):
        assert parse_period("q2-2526") == "2025-07"  # FY Q2 = July
        assert parse_period("q1-25") == "2025-04"    # FY Q1 = April
        assert parse_period("q4-2526") == "2026-01"  # FY Q4 = Jan next year

    def test_no_period(self):
        assert parse_period("") is None
        assert parse_period("no date here") is None
        assert parse_period("hello world") is None


class TestParsePeriodDateRanges:
    """NEW: Date range handling - return END date."""

    def test_date_range_returns_end(self):
        assert parse_period("October 2019 - September 2025") == "2025-09"
        assert parse_period("Referrals October 2019 - September 2025") == "2025-09"

    def test_partial_year_range(self):
        assert parse_period("Jan-Sep 2025") == "2025-09"
        assert parse_period("eRS dashboard data Jan-Sep 2025") == "2025-09"

    def test_range_with_boundaries(self):
        assert parse_period("eRS dashboard data October 2019 - March 2021") == "2021-03"
        assert parse_period("Bookings October 2019 - September 2025") == "2025-09"


class TestParsePeriodYearOnly:
    """NEW: Year-only handling - return January."""

    def test_year_only(self):
        assert parse_period("2020") == "2020-01"
        assert parse_period("eRS dashboard data 2020") == "2020-01"
        assert parse_period("eRS dashboard data 2021") == "2021-01"
        assert parse_period("data_2023") == "2023-01"

    def test_year_only_not_triggered_when_month_present(self):
        # Should still return the month-year, not just year
        assert parse_period("December 2025") == "2025-12"
        assert parse_period("2025-09") == "2025-09"


class TestParsePeriodRange:
    """Test parse_period_range() function."""

    def test_date_range(self):
        assert parse_period_range("October 2019 - September 2025") == ("2019-10", "2025-09")
        assert parse_period_range("Referrals October 2019 - September 2025") == ("2019-10", "2025-09")

    def test_single_date(self):
        assert parse_period_range("December 2025") == ("2025-12", "2025-12")
        assert parse_period_range("2024-11") == ("2024-11", "2024-11")

    def test_year_only(self):
        assert parse_period_range("2020") == ("2020-01", "2020-01")
        assert parse_period_range("eRS dashboard data 2020") == ("2020-01", "2020-01")

    def test_no_date(self):
        assert parse_period_range("no date here") == (None, None)
        assert parse_period_range("") == (None, None)


class TestNHSeReferralFilenames:
    """Real NHS e-Referral Service filenames."""

    def test_cumulative_files(self):
        # These are the actual filenames from the NHS page
        assert parse_period("Referrals October 2019 - September 2025") == "2025-09"
        assert parse_period("Appointment Slot Issues October 2019 - September 2025") == "2025-09"
        assert parse_period("Bookings October 2019 - September 2025") == "2025-09"

    def test_annual_files(self):
        assert parse_period("eRS dashboard data 2020 (using 2022 ICB - sub group boundaries)") == "2020-01"
        assert parse_period("eRS dashboard data 2021 (using 2022 ICB - sub group boundaries)") == "2021-01"
        assert parse_period("eRS dashboard data 2022 (using 2022 ICB - sub group boundaries)") == "2022-01"
        assert parse_period("eRS dashboard data 2023 (using 2022 ICB - sub group boundaries)") == "2023-01"
        assert parse_period("eRS dashboard data 2024 (using 2022 ICB - sub group boundaries)") == "2024-01"

    def test_partial_year_file(self):
        assert parse_period("eRS dashboard data Jan-Sep 2025 (using 2022 ICB - sub group boundaries)") == "2025-09"

    def test_legacy_boundary_files(self):
        assert parse_period("eRS dashboard data October 2019 - March 2021 (using 2020 CCG and LA boundaries)") == "2021-03"
        assert parse_period("eRS dashboard data October 2019 - June 2022 (using 2021 CCG and LA boundaries)") == "2022-06"
