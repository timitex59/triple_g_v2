import unittest
from datetime import datetime

from fios.cross_check import (
    PARIS,
    _append_section_pip_report,
    _render_memory_sections,
    _update_section_pip_tracker,
)


def pair_row(pair: str, day: float) -> dict:
    return {"cur": pair, "day": day}


def clock(hour: int) -> datetime:
    return PARIS.localize(datetime(2026, 8, 10, hour, 15))


class MomentumSectionPersistenceTests(unittest.TestCase):
    def test_non_qualifying_pair_stays_in_its_section_with_warning(self):
        rows = {"EURUSD": pair_row("EURUSD", 0.04)}

        conv, top, state = _render_memory_sections(
            {}, {}, rows, {"EURUSD": {"section": "conv", "dir": 1}}
        )

        self.assertIn("🟢 EURUSD ⚠️", conv)
        self.assertEqual(top, [])
        self.assertEqual(state["EURUSD"], {"section": "conv", "dir": 1})

    def test_pair_migrates_to_new_section_without_warning(self):
        rows = {"EURUSD": pair_row("EURUSD", 0.35)}

        conv, top, state = _render_memory_sections(
            {},
            {"EURUSD": {"dir": 1, "aligned3": False, "chg_abs": 0.35}},
            rows,
            {"EURUSD": {"section": "conv", "dir": 1}},
        )

        self.assertEqual(conv, [])
        self.assertIn("🟢 EURUSD", top)
        self.assertNotIn("⚠️", "\n".join(top))
        self.assertEqual(state["EURUSD"], {"section": "top", "dir": 1})

    def test_strong_reversal_removes_pair(self):
        rows = {"EURUSD": pair_row("EURUSD", -0.11)}

        conv, top, state = _render_memory_sections(
            {}, {}, rows, {"EURUSD": {"section": "conv", "dir": 1}}
        )

        self.assertEqual(conv, [])
        self.assertEqual(top, [])
        self.assertNotIn("EURUSD", state)

    def test_small_counter_move_keeps_pair_with_warning(self):
        rows = {"EURUSD": pair_row("EURUSD", -0.10)}

        conv, _, state = _render_memory_sections(
            {}, {}, rows, {"EURUSD": {"section": "conv", "dir": 1}}
        )

        self.assertIn("🟢 EURUSD ⚠️", conv)
        self.assertIn("EURUSD", state)

    def test_current_qualification_restores_flame_and_direction(self):
        rows = {"EURUSD": pair_row("EURUSD", -0.42)}

        conv, _, state = _render_memory_sections(
            {"EURUSD": {"dir": -1, "tier": 3, "chg_abs": 0.42}},
            {},
            rows,
            {"EURUSD": {"section": "top", "dir": 1}},
        )

        self.assertIn("🔴 EURUSD 🔥", conv)
        self.assertEqual(state["EURUSD"], {"section": "conv", "dir": -1})

    def test_section_books_follow_persistence_and_migration(self):
        conv = {"EURUSD": {"section": "conv", "dir": 1}}
        state, report = _update_section_pip_tracker(
            {}, conv, {"EURUSD": {"level": 1.1000}}, clock(7)
        )
        self.assertIsNone(report)

        # Une paire persistante (y compris marquee warning par le rendu) garde
        # le meme segment et son prix d'entree initial.
        state, _ = _update_section_pip_tracker(
            state, conv, {"EURUSD": {"level": 1.1020}}, clock(10)
        )
        self.assertAlmostEqual(state["open"]["EURUSD"]["current_pips"], 20.0)

        top = {"EURUSD": {"section": "top", "dir": 1}}
        state, _ = _update_section_pip_tracker(
            state, top, {"EURUSD": {"level": 1.1030}}, clock(11)
        )
        segments = state["days"]["2026-08-10"]["segments"]
        self.assertEqual(segments[0]["section"], "conv")
        self.assertEqual(segments[0]["close_reason"], "MIGRATION")
        self.assertAlmostEqual(segments[0]["pips"], 30.0)
        self.assertEqual(state["open"]["EURUSD"]["section"], "top")
        self.assertAlmostEqual(state["open"]["EURUSD"]["start_price"], 1.1030)

        state, report = _update_section_pip_tracker(
            state, top, {"EURUSD": {"level": 1.1050}}, clock(23)
        )
        self.assertEqual(state["open"], {})
        self.assertAlmostEqual(report["conv"]["daily"], 30.0)
        self.assertAlmostEqual(report["top"]["daily"], 20.0)
        self.assertAlmostEqual(report["conv"]["weekly"], 30.0)
        self.assertAlmostEqual(report["top"]["monthly"], 20.0)
        self.assertAlmostEqual(report["conv"]["yearly"], 30.0)

        # Le bilan du meme jour n'est produit qu'une fois.
        state, repeated = _update_section_pip_tracker(
            state, top, {"EURUSD": {"level": 1.1050}},
            PARIS.localize(datetime(2026, 8, 10, 23, 30)),
        )
        self.assertIsNone(repeated)

    def test_section_report_format_is_appended_only_at_daily_close(self):
        item = {
            "daily": 19.0,
            "weekly": 19.0,
            "monthly": 25.7,
            "yearly": 25.7,
            "year": 2026,
        }
        lines = _append_section_pip_report(
            ["🎯 PAIRES CONVERGENTES", "", "🟢 EURUSD"],
            "🎯 PAIRES CONVERGENTES",
            item,
        )

        self.assertIn("📈 Daily : +19.0", lines)
        self.assertIn("📊 Weekly : +19.0", lines)
        self.assertIn("Monthly : +25.7", lines)
        self.assertIn("🗓 YTD 2026 : +25.7", lines)


if __name__ == "__main__":
    unittest.main()
