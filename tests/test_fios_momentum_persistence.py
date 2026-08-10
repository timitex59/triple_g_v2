import unittest

from fios.cross_check import _render_memory_sections


def pair_row(pair: str, day: float) -> dict:
    return {"cur": pair, "day": day}


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


if __name__ == "__main__":
    unittest.main()
