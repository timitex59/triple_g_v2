import unittest

import pandas as pd

import datetime as dt
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo

from imp_triangle_verdict import (
    aligned_verdict,
    build_telegram_message,
    classify,
    count_since,
    daily_chg_pct,
    h1_cross_state,
    load_state,
    save_state,
    timeframe_verdict,
    trading_day,
    update_selection,
)


class ClassifyTests(unittest.TestCase):
    def test_bull_when_price_is_above_the_bull_level(self):
        # niveau bull 100, niveau bear 90 : prix 105 > 100 ; 105 < 90 faux
        self.assertEqual(classify(105, bull_above=100, bear_below=90, latest_kind="BEAR"), "BULL")

    def test_bear_when_price_is_below_the_bear_level(self):
        self.assertEqual(classify(95, bull_above=110, bear_below=100, latest_kind="BULL"), "BEAR")

    def test_neutral_when_price_sits_between_the_two_levels(self):
        self.assertEqual(classify(100, bull_above=110, bear_below=90, latest_kind="BULL"), "NEUTRE")

    def test_both_true_is_settled_by_the_most_recent_triangle(self):
        self.assertEqual(classify(105, bull_above=100, bear_below=110, latest_kind="BULL"), "BULL")
        self.assertEqual(classify(105, bull_above=100, bear_below=110, latest_kind="BEAR"), "BEAR")

    def test_price_exactly_on_a_level_does_not_qualify(self):
        self.assertEqual(classify(100, bull_above=100, bear_below=None, latest_kind="BEAR"), "NEUTRE")
        self.assertEqual(classify(100, bull_above=None, bear_below=100, latest_kind="BULL"), "NEUTRE")

    def test_missing_level_cannot_qualify_that_side(self):
        self.assertEqual(classify(105, bull_above=None, bear_below=None, latest_kind=None), "NEUTRE")
        self.assertEqual(classify(105, bull_above=100, bear_below=None, latest_kind="BEAR"), "BULL")
        self.assertEqual(classify(95, bull_above=None, bear_below=100, latest_kind="BULL"), "BEAR")


def sig(kind, index):
    return dict(kind=kind, index=index, cross_index=index)


def computed_for(signals, opens, closes):
    times = list(pd.date_range("2026-01-04 21:00", periods=len(opens), freq="D", tz="UTC"))
    return dict(times=times, opens=opens, closes=closes, signals=signals)


class CountSinceTests(unittest.TestCase):
    def test_counts_same_color_after_the_last_opposite(self):
        signals = [sig("BULL", 0), sig("BEAR", 1), sig("BULL", 2), sig("BULL", 3)]
        self.assertEqual(count_since(signals, "BEAR", "BULL"), 2)
        self.assertEqual(count_since(signals, "BULL", "BEAR"), 0)

    def test_counts_all_when_there_was_never_an_opposite(self):
        signals = [sig("BULL", 0), sig("BULL", 1), sig("BULL", 2)]
        self.assertEqual(count_since(signals, "BEAR", "BULL"), 3)


class TimeframeVerdictTests(unittest.TestCase):
    def test_default_reference_is_the_open_of_the_last_triangle_of_each_color(self):
        # [BULL 0, BEAR 1, BULL 2] : 1 seul vert depuis le dernier rouge (< far_count 2)
        c = computed_for([sig("BULL", 0), sig("BEAR", 1), sig("BULL", 2)],
                         opens=[10.0, 20.0, 30.0, 40.0], closes=[11.0, 19.0, 31.0, 41.0])
        reading = timeframe_verdict(c, "D", price=25.0)
        self.assertEqual(reading["bull_ref"]["basis"], "OPEN_RED")
        self.assertEqual(reading["bull_ref"]["level"], 20.0)
        self.assertEqual(reading["bear_ref"]["basis"], "OPEN_GREEN")
        self.assertEqual(reading["bear_ref"]["level"], 30.0)  # dernier vert = index 2
        # 25 > 20 (BULL vrai) et 25 < 30 (BEAR vrai) -> le plus recent (vert) departage
        self.assertEqual(reading["verdict"], "BULL")

    def test_far_red_triangle_moves_the_bull_reference_to_the_last_green_close(self):
        # [BEAR 0, BULL 1, BULL 2, BULL 3] : 3 verts depuis le rouge >= far_count 2
        c = computed_for([sig("BEAR", 0), sig("BULL", 1), sig("BULL", 2), sig("BULL", 3)],
                         opens=[100.0, 60.0, 70.0, 80.0], closes=[99.0, 62.0, 72.0, 85.0])
        reading = timeframe_verdict(c, "D", price=90.0)
        self.assertEqual(reading["bull_ref"]["basis"], "CLOSE_GREEN")
        self.assertEqual(reading["bull_ref"]["level"], 85.0)   # close du dernier vert, pas l'open du rouge (100)
        self.assertEqual(reading["verdict"], "BULL")           # 90 > 85 (avec l'open du rouge 100 : NEUTRE)
        self.assertEqual(reading["greens_since_red"], 3)

    def test_price_inside_the_last_green_body_is_neutral_once_the_reference_moved(self):
        c = computed_for([sig("BEAR", 0), sig("BULL", 1), sig("BULL", 2)],
                         opens=[100.0, 60.0, 80.0], closes=[99.0, 62.0, 85.0])
        # bull > close 85 ; bear < open du dernier vert 80 : 82 n'est ni l'un ni l'autre
        self.assertEqual(timeframe_verdict(c, "D", price=82.0)["verdict"], "NEUTRE")
        self.assertEqual(timeframe_verdict(c, "D", price=79.0)["verdict"], "BEAR")

    def test_far_green_triangle_moves_the_bear_reference_to_the_last_red_close(self):
        # [BULL 0, BEAR 1, BEAR 2, BEAR 3] : 3 rouges depuis le vert >= far_count 2
        c = computed_for([sig("BULL", 0), sig("BEAR", 1), sig("BEAR", 2), sig("BEAR", 3)],
                         opens=[10.0, 60.0, 50.0, 45.0], closes=[11.0, 58.0, 48.0, 40.0])
        reading = timeframe_verdict(c, "D", price=35.0)
        self.assertEqual(reading["bear_ref"]["basis"], "CLOSE_RED")
        self.assertEqual(reading["bear_ref"]["level"], 40.0)   # close du dernier rouge, pas l'open du vert (10)
        self.assertEqual(reading["verdict"], "BEAR")           # 35 < 40 (avec l'open du vert 10 : NEUTRE)
        self.assertEqual(reading["reds_since_green"], 3)

    def test_below_the_threshold_the_original_rule_applies(self):
        c = computed_for([sig("BEAR", 0), sig("BULL", 1), sig("BULL", 2)],
                         opens=[100.0, 60.0, 80.0], closes=[99.0, 62.0, 85.0])
        reading = timeframe_verdict(c, "D", price=90.0, far_count=3)  # 2 verts < 3
        self.assertEqual(reading["bull_ref"]["basis"], "OPEN_RED")
        self.assertEqual(reading["verdict"], "NEUTRE")               # 90 < open du rouge 100

    def test_no_red_at_all_with_enough_greens_uses_the_green_close(self):
        c = computed_for([sig("BULL", 0), sig("BULL", 1)], opens=[10.0, 20.0], closes=[12.0, 25.0])
        reading = timeframe_verdict(c, "D", price=30.0)
        self.assertEqual(reading["bull_ref"]["basis"], "CLOSE_GREEN")
        self.assertEqual(reading["verdict"], "BULL")

    def test_no_triangle_at_all_is_neutral(self):
        c = computed_for([], opens=[10.0, 20.0], closes=[11.0, 21.0])
        reading = timeframe_verdict(c, "D", price=25.0)
        self.assertEqual(reading["verdict"], "NEUTRE")
        self.assertIsNone(reading["bull_ref"])
        self.assertIsNone(reading["bear_ref"])


class AlignedVerdictTests(unittest.TestCase):
    def result(self, **verdicts):
        return dict(timeframes={tf: dict(verdict=v) for tf, v in verdicts.items()})

    def test_aligned_when_all_timeframes_agree(self):
        self.assertEqual(aligned_verdict(self.result(D="BULL", W="BULL", M="BULL")), "BULL")
        self.assertEqual(aligned_verdict(self.result(D="BEAR", W="BEAR")), "BEAR")

    def test_not_aligned_on_disagreement_or_all_neutral(self):
        self.assertIsNone(aligned_verdict(self.result(D="BULL", W="BEAR", M="BULL")))
        self.assertIsNone(aligned_verdict(self.result(D="BULL", W="NEUTRE", M="BULL")))
        self.assertIsNone(aligned_verdict(self.result(D="NEUTRE", W="NEUTRE")))


TODAY = "2026-09-20"
YESTERDAY = "2026-09-19"


def selected(pair, verdict, warning=False, lost=False):
    return dict(pair=pair, verdict=verdict, warning=warning, lost=lost, chg=None)


class TelegramMessageTests(unittest.TestCase):
    NOW = dt.datetime(2026, 9, 20, 3, 45, tzinfo=ZoneInfo("Europe/Paris"))

    def test_one_icon_per_pair_bull_first_then_bear_alphabetical(self):
        selection = [
            selected("NZDUSD", "BEAR"), selected("GBPNZD", "BULL"),
            selected("EURAUD", "BEAR"), selected("AUDCAD", "BULL"),
        ]
        self.assertEqual(
            build_telegram_message(selection, now=self.NOW),
            "\U0001f53a EARLY IMP\n\n"
            "AUDCAD\t\U0001f7e2\nGBPNZD\t\U0001f7e2\n"
            "EURAUD\t\U0001f534\nNZDUSD\t\U0001f534\n\n"
            "⏰ 2026-09-20 03:45 Paris",
        )

    def test_warning_is_glued_to_the_icon(self):
        message = build_telegram_message([selected("AUDCAD", "BULL", warning=True)], now=self.NOW)
        self.assertIn("AUDCAD\t\U0001f7e2⚠️\n", message)

    def test_lost_pair_shows_a_double_warning_in_place_of_the_colored_ball(self):
        selection = [selected("AUDCAD", "BULL"), selected("GBPNZD", "BULL", warning=True, lost=True),
                     selected("EURAUD", "BEAR", warning=True)]
        message = build_telegram_message(selection, now=self.NOW)
        self.assertIn("GBPNZD\t⚠️⚠️\n", message)
        self.assertNotIn("GBPNZD\t\U0001f7e2", message)
        # la paire perdue garde sa place dans son groupe d'origine (BULL avant BEAR)
        self.assertLess(message.index("GBPNZD"), message.index("EURAUD"))
        self.assertIn("EURAUD\t\U0001f534⚠️\n", message)

    def test_silent_when_selection_is_empty(self):
        self.assertIsNone(build_telegram_message([], now=self.NOW))


class TradingDayTests(unittest.TestCase):
    def test_rolls_over_at_17h_new_york(self):
        paris = ZoneInfo("Europe/Paris")
        before = dt.datetime(2026, 9, 16, 22, 59, tzinfo=paris)  # 16h59 NY
        after = dt.datetime(2026, 9, 16, 23, 1, tzinfo=paris)    # 17h01 NY
        self.assertEqual(trading_day(before), "2026-09-16")
        self.assertEqual(trading_day(after), "2026-09-17")


class UpdateSelectionTests(unittest.TestCase):
    def result(self, pair, verdict, chg, event="cross", side="right"):
        """`event` : "cross" = cross H1 dans le sens du verdict, "against" = cross inverse,
        None = pas de cross. `side` : "right"/"wrong" = cote du prix vs SAR H1 (bon/mauvais
        pour ce verdict), None = indefini."""
        opposite = "BEAR" if verdict == "BULL" else "BULL"
        good_side = "above" if verdict == "BULL" else "below"
        bad_side = "below" if verdict == "BULL" else "above"
        h1 = dict(event={"cross": verdict, "against": opposite, None: None}[event],
                  side={"right": good_side, "wrong": bad_side, None: None}[side])
        tfs = {tf: dict(verdict=verdict) for tf in ("D", "W", "M")}
        return dict(pair=pair, chg=chg, h1=h1, timeframes=tfs)

    def not_aligned(self, pair, chg):
        return dict(pair=pair, chg=chg, timeframes=dict(D=dict(verdict="BULL"), W=dict(verdict="BEAR")))

    def names(self, selection):
        return {s["pair"]: s["warning"] for s in selection}

    def test_aligned_pair_above_the_threshold_is_selected_without_warning(self):
        selection, state = update_selection([self.result("AUDCAD", "BULL", 0.25)], {}, 0.1, TODAY)
        self.assertEqual(self.names(selection), {"AUDCAD": False})
        self.assertEqual(state, {"AUDCAD": dict(verdict="BULL", warning=False)})

    def test_threshold_is_strict_and_uses_the_absolute_value(self):
        results = [self.result("AUDCAD", "BULL", 0.1), self.result("EURAUD", "BEAR", -0.3)]
        selection, _ = update_selection(results, {}, 0.1, TODAY)
        # 0.1 pile : pas au-dessus du seuil -> warning ; -0.3 : au-dessus en valeur absolue -> pas de warning
        self.assertEqual(self.names(selection), {"AUDCAD": True, "EURAUD": False})

    def test_chg_below_the_threshold_does_not_block_the_entry_but_adds_a_warning(self):
        selection, state = update_selection([self.result("AUDCAD", "BULL", 0.05)], {}, 0.1, TODAY)
        self.assertEqual(self.names(selection), {"AUDCAD": True})
        self.assertFalse(selection[0]["lost"])  # 1 seul warning, la boule reste
        self.assertEqual(state["AUDCAD"], dict(verdict="BULL", warning=True))

    def test_unknown_chg_does_not_block_the_entry_either(self):
        selection, _ = update_selection([self.result("AUDCAD", "BULL", None)], {}, 0.1, TODAY)
        self.assertEqual(self.names(selection), {"AUDCAD": True})

    def test_previously_selected_pair_falling_below_the_threshold_stays_with_a_warning(self):
        previous = {"AUDCAD": dict(verdict="BULL", warning=False)}
        selection, state = update_selection([self.result("AUDCAD", "BULL", 0.04)], previous, 0.1, TODAY)
        self.assertEqual(self.names(selection), {"AUDCAD": True})
        self.assertEqual(state["AUDCAD"], dict(verdict="BULL", warning=True))

    def test_warning_stays_while_it_remains_below_and_clears_when_back_above(self):
        previous = {"AUDCAD": dict(verdict="BULL", warning=True)}
        selection, _ = update_selection([self.result("AUDCAD", "BULL", 0.02)], previous, 0.1, TODAY)
        self.assertEqual(self.names(selection), {"AUDCAD": True})
        selection, state = update_selection([self.result("AUDCAD", "BULL", 0.3)], previous, 0.1, TODAY)
        self.assertEqual(self.names(selection), {"AUDCAD": False})
        self.assertEqual(state["AUDCAD"]["warning"], False)

    def test_pair_that_loses_alignment_stays_with_a_double_warning_for_the_day(self):
        previous = {"AUDCAD": dict(verdict="BULL", warning=False)}
        selection, state = update_selection([self.not_aligned("AUDCAD", 0.5)], previous, 0.1, TODAY)
        self.assertEqual(len(selection), 1)
        self.assertEqual((selection[0]["pair"], selection[0]["verdict"]), ("AUDCAD", "BULL"))  # sens d'origine
        self.assertTrue(selection[0]["lost"])
        self.assertTrue(selection[0]["warning"])  # double warning meme si le CHG% est au-dessus du seuil
        self.assertEqual(state["AUDCAD"], dict(verdict="BULL", warning=True, lost_day=TODAY))

    def test_lost_pair_is_kept_on_later_runs_of_the_same_day(self):
        previous = {"AUDCAD": dict(verdict="BULL", warning=True, lost_day=TODAY)}
        selection, state = update_selection([self.not_aligned("AUDCAD", 0.0)], previous, 0.1, TODAY)
        self.assertEqual([(s["pair"], s["lost"]) for s in selection], [("AUDCAD", True)])
        self.assertEqual(state["AUDCAD"]["lost_day"], TODAY)  # le jour de perte ne glisse pas

    def test_lost_pair_leaves_the_list_once_the_trading_day_is_over(self):
        previous = {"AUDCAD": dict(verdict="BULL", warning=True, lost_day=YESTERDAY)}
        selection, state = update_selection([self.not_aligned("AUDCAD", 0.5)], previous, 0.1, TODAY)
        self.assertEqual(selection, [])
        self.assertEqual(state, {})

    def test_unaligned_pair_that_was_never_selected_stays_out(self):
        selection, state = update_selection([self.not_aligned("AUDCAD", 0.5)], {}, 0.1, TODAY)
        self.assertEqual((selection, state), ([], {}))

    def test_lost_pair_that_realigns_in_the_same_direction_resumes_normal_handling(self):
        previous = {"AUDCAD": dict(verdict="BULL", warning=True, lost_day=TODAY)}
        selection, state = update_selection([self.result("AUDCAD", "BULL", 0.3)], previous, 0.1, TODAY)
        self.assertEqual([(s["lost"], s["warning"]) for s in selection], [(False, False)])
        self.assertEqual(state["AUDCAD"], dict(verdict="BULL", warning=False))  # plus de lost_day
        selection, state = update_selection([self.result("AUDCAD", "BULL", 0.02)], previous, 0.1, TODAY)
        self.assertEqual([(s["lost"], s["warning"]) for s in selection], [(False, True)])  # 1 seul warning

    def test_lost_pair_that_realigns_the_other_way_must_requalify(self):
        previous = {"AUDCAD": dict(verdict="BULL", warning=True, lost_day=TODAY)}
        # sens oppose sans cross H1 dans ce sens : pas d'entree
        selection, _ = update_selection([self.result("AUDCAD", "BEAR", -0.4, event=None)], previous, 0.1, TODAY)
        self.assertEqual(selection, [])
        selection, state = update_selection([self.result("AUDCAD", "BEAR", -0.4)], previous, 0.1, TODAY)
        self.assertEqual(state["AUDCAD"], dict(verdict="BEAR", warning=False))

    def test_direction_flip_must_requalify_like_a_new_pair(self):
        previous = {"AUDCAD": dict(verdict="BULL", warning=False)}
        # nouveau sens sans cross H1 dans ce sens : l'ancien BULL ne compte pas, pas d'entree
        no_cross, _ = update_selection([self.result("AUDCAD", "BEAR", -0.4, event=None)], previous, 0.1, TODAY)
        self.assertEqual(no_cross, [])
        entered, state = update_selection([self.result("AUDCAD", "BEAR", -0.4)], previous, 0.1, TODAY)
        self.assertEqual(state["AUDCAD"], dict(verdict="BEAR", warning=False))
        self.assertEqual(self.names(entered), {"AUDCAD": False})

    def test_pair_missing_from_results_keeps_its_previous_state(self):
        # fetch en erreur ce run-la : ne doit pas faire sortir la paire
        previous = {"GBPNZD": dict(verdict="BULL", warning=True)}
        selection, state = update_selection([self.result("AUDCAD", "BULL", 0.3)], previous, 0.1, TODAY)
        self.assertEqual(self.names(selection), {"AUDCAD": False, "GBPNZD": True})
        self.assertEqual(state["GBPNZD"], dict(verdict="BULL", warning=True))

    def test_missing_pair_with_an_expired_loss_day_is_dropped_but_a_current_one_is_kept(self):
        previous = {"OLD": dict(verdict="BULL", warning=True, lost_day=YESTERDAY),
                    "CUR": dict(verdict="BEAR", warning=True, lost_day=TODAY)}
        selection, state = update_selection([self.result("AUDCAD", "BULL", 0.3)], previous, 0.1, TODAY)
        self.assertEqual(sorted(s["pair"] for s in selection), ["AUDCAD", "CUR"])
        self.assertEqual([s["lost"] for s in selection if s["pair"] == "CUR"], [True])
        self.assertNotIn("OLD", state)

    def test_selection_is_ordered_bull_first_then_alphabetical(self):
        results = [self.result("NZDUSD", "BEAR", 0.5), self.result("GBPNZD", "BULL", 0.5),
                   self.result("AUDCAD", "BULL", 0.5)]
        selection, _ = update_selection(results, {}, 0.1, TODAY)
        self.assertEqual([s["pair"] for s in selection], ["AUDCAD", "GBPNZD", "NZDUSD"])

    # --- declencheur H1 : entree stricte sur le cross, jamais de retrait mais un warning ---

    def test_entry_requires_an_h1_cross_in_the_verdict_direction(self):
        # deja du bon cote du SAR H1 mais aucun cross depuis le dernier run : on attend le prochain cross
        selection, state = update_selection([self.result("AUDCAD", "BULL", 0.5, event=None, side="right")], {}, 0.1, TODAY)
        self.assertEqual((selection, state), ([], {}))
        # cross dans le SENS OPPOSE : pas d'entree non plus
        selection, _ = update_selection([self.result("AUDCAD", "BULL", 0.5, event="against", side="wrong")], {}, 0.1, TODAY)
        self.assertEqual(selection, [])
        selection, _ = update_selection([self.result("EURAUD", "BEAR", -0.5, event="cross")], {}, 0.1, TODAY)
        self.assertEqual([s["pair"] for s in selection], ["EURAUD"])

    def test_pair_without_h1_data_cannot_enter(self):
        legacy = self.result("AUDCAD", "BULL", 0.5)
        del legacy["h1"]
        selection, _ = update_selection([legacy], {}, 0.1, TODAY)
        self.assertEqual(selection, [])

    def test_listed_pair_is_kept_with_a_warning_when_the_h1_sar_turns_against_it(self):
        previous = {"AUDCAD": dict(verdict="BULL", warning=False)}
        selection, state = update_selection(
            [self.result("AUDCAD", "BULL", 0.5, event="against", side="wrong")], previous, 0.1, TODAY)
        self.assertEqual(self.names(selection), {"AUDCAD": True})   # jamais retiree, warning
        self.assertFalse(selection[0]["lost"])                       # 1 seul warning, la boule reste
        self.assertEqual(state["AUDCAD"], dict(verdict="BULL", warning=True))

    def test_h1_warning_applies_to_bear_pairs_when_price_is_above_the_sar(self):
        previous = {"EURAUD": dict(verdict="BEAR", warning=False)}
        selection, _ = update_selection([self.result("EURAUD", "BEAR", -0.5, event=None, side="wrong")], previous, 0.1, TODAY)
        self.assertEqual(self.names(selection), {"EURAUD": True})

    def test_h1_warning_clears_once_price_is_back_on_the_right_side(self):
        previous = {"AUDCAD": dict(verdict="BULL", warning=True)}
        selection, state = update_selection(
            [self.result("AUDCAD", "BULL", 0.5, event="cross", side="right")], previous, 0.1, TODAY)
        self.assertEqual(self.names(selection), {"AUDCAD": False})
        self.assertEqual(state["AUDCAD"]["warning"], False)

    def test_h1_and_chg_warnings_share_the_single_warning_icon(self):
        previous = {"AUDCAD": dict(verdict="BULL", warning=False)}
        # CHG% sous le seuil MAIS H1 du bon cote -> warning ; CHG% ok mais H1 du mauvais cote -> warning
        below = update_selection([self.result("AUDCAD", "BULL", 0.02, event=None, side="right")], previous, 0.1, TODAY)[0]
        against = update_selection([self.result("AUDCAD", "BULL", 0.5, event=None, side="wrong")], previous, 0.1, TODAY)[0]
        both = update_selection([self.result("AUDCAD", "BULL", 0.02, event=None, side="wrong")], previous, 0.1, TODAY)[0]
        self.assertEqual([self.names(x) for x in (below, against, both)], [{"AUDCAD": True}] * 3)

    def test_listed_pair_from_the_previous_rules_is_grandfathered(self):
        # entree d'avant le declencheur H1 : pas d'info H1 dans l'etat, on la garde sans cross
        previous = {"AUDCAD": dict(verdict="BULL", warning=False)}
        selection, _ = update_selection([self.result("AUDCAD", "BULL", 0.5, event=None, side="right")], previous, 0.1, TODAY)
        self.assertEqual(self.names(selection), {"AUDCAD": False})


class DailyChgTests(unittest.TestCase):
    def test_change_versus_previous_daily_close(self):
        self.assertAlmostEqual(daily_chg_pct(101.0, 100.0), 1.0)
        self.assertAlmostEqual(daily_chg_pct(99.0, 100.0), -1.0)

    def test_unknown_or_zero_previous_close_gives_none(self):
        self.assertIsNone(daily_chg_pct(100.0, None))
        self.assertIsNone(daily_chg_pct(100.0, 0.0))


class H1CrossStateTests(unittest.TestCase):
    TIMES = list(pd.date_range("2026-09-18 10:00", periods=4, freq="h", tz="UTC"))

    def test_cross_after_the_mark_is_reported_with_its_direction(self):
        # close passe de 9 a 11 au-dessus d'un SAR a 10 sur la bougie d'index 2
        state = h1_cross_state(self.TIMES, [9, 9, 11, 11], [10, 10, 10, 10], mark=self.TIMES[1])
        self.assertEqual((state["event"], state["side"]), ("BULL", "above"))
        self.assertEqual(state["last_bar"], self.TIMES[3].isoformat())

    def test_cross_at_or_before_the_mark_was_already_seen(self):
        state = h1_cross_state(self.TIMES, [9, 9, 11, 11], [10, 10, 10, 10], mark=self.TIMES[2])
        self.assertIsNone(state["event"])
        self.assertEqual(state["side"], "above")

    def test_no_mark_means_no_event_the_first_time_a_pair_is_seen(self):
        state = h1_cross_state(self.TIMES, [9, 9, 11, 11], [10, 10, 10, 10], mark=None)
        self.assertIsNone(state["event"])

    def test_only_the_last_cross_of_the_window_counts(self):
        # haussier en 1 puis baissier en 2 : la paire finit du mauvais cote
        state = h1_cross_state(self.TIMES, [9, 11, 9, 9], [10, 10, 10, 10], mark=self.TIMES[0])
        self.assertEqual((state["event"], state["side"]), ("BEAR", "below"))

    def test_bear_cross(self):
        state = h1_cross_state(self.TIMES, [11, 11, 9, 9], [10, 10, 10, 10], mark=self.TIMES[0])
        self.assertEqual((state["event"], state["side"]), ("BEAR", "below"))

    def test_side_is_none_when_close_equals_sar(self):
        state = h1_cross_state(self.TIMES[:2], [10, 10], [10, 10], mark=None)
        self.assertIsNone(state["side"])


class StatePersistenceTests(unittest.TestCase):
    def test_roundtrip_and_missing_or_corrupt_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            self.assertEqual(load_state(path), {})
            save_state(path, {"AUDCAD": dict(verdict="BULL", warning=True)})
            self.assertEqual(load_state(path)["pairs"], {"AUDCAD": dict(verdict="BULL", warning=True)})
            path.write_text("{corrompu", encoding="utf-8")
            self.assertEqual(load_state(path), {})

    def test_h1_marks_are_persisted_per_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            marks = {"AUDCAD": "2026-09-18T21:00:00+00:00", "EURAUD": "2026-09-18T20:00:00+00:00"}
            save_state(path, {}, marks)
            self.assertEqual(load_state(path)["h1_marks"], marks)
            save_state(path, {})  # sans repere : le champ existe quand meme, vide
            self.assertEqual(load_state(path)["h1_marks"], {})


if __name__ == "__main__":
    unittest.main()
