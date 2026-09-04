import datetime as dt
import unittest
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from paire_check import (
    DEFAULT_PAIRS,
    aligned_currency_names,
    best_pair_lines,
    best_pair_names,
    build_message,
    currency_consensus_ball,
    index_chg_lines,
    index_trend_lines,
    needed_currencies,
    needed_helper_pairs,
    pair_check_compact_line,
    pair_index_icon,
    update_index_trend_state,
)

PARIS = ZoneInfo("Europe/Paris")


def index_row(pair, currency, m, w, d, daily_chg):
    """Ligne d'indice factice, meme structure que le helper equivalent de
    tests/test_renko_full_alignment_29pairs.py."""
    px = {"M": m, "W": w, "D": d}
    bias = dict(px)
    states = {
        tf: SimpleNamespace(
            green_streak=1 if bias[tf] == 1 else 0,
            red_streak=1 if bias[tf] == -1 else 0,
        )
        for tf in ("M", "W", "D")
    }
    return {
        "pair": pair, "asset_type": "INDEX", "currency": currency,
        "px": px, "bias": bias, "states": states, "daily_chg": daily_chg,
        "live_price": 1.0,
    }


def make_currency_row(
    renko=("BULL", "BULL", "BULL"),
    d1=("BULL", 10.0, 5.0, -2.0),
    h1=("BEAR", -3.0, 0.0, 1.0),
):
    """Ligne factice `compute_currency_imp` (une devise): `d1`/`h1` =
    (imp_status, all_avg_pips, bull_avg_pips, bear_avg_pips)."""
    m, w, d = renko

    def chart(status, all_pips, bull_pips, bear_pips):
        return {
            "imp_status": status,
            "all_avg_pips": all_pips,
            "bull_avg_pips": bull_pips,
            "bear_avg_pips": bear_pips,
        }

    return {
        "pair": "DXY",
        "currency": "USD",
        "renko": {"M": {"status": m}, "W": {"status": w}, "D": {"status": d}},
        "D1": chart(*d1),
        "H1": chart(*h1),
    }


class PaireCheckTests(unittest.TestCase):
    def test_needed_currencies_unions_base_and_quote_of_every_pair(self):
        self.assertEqual(
            needed_currencies(["EURUSD", "CHFJPY", "USDJPY"]),
            {"EUR", "USD", "CHF", "JPY"},
        )

    def test_needed_currencies_ignores_unrecognized_pairs(self):
        self.assertEqual(needed_currencies(["EURUSD", "NOTAPAIR"]), {"EUR", "USD"})
        self.assertEqual(needed_currencies([]), set())

    def test_needed_helper_pairs_covers_all_7_pairs_of_each_currency(self):
        helper_pairs = needed_helper_pairs({"EUR"})

        self.assertEqual(len(helper_pairs), 7)
        self.assertIn("EURUSD", helper_pairs)
        self.assertIn("EURJPY", helper_pairs)
        self.assertNotIn("AUDNZD", helper_pairs)  # ni EUR ni la devise cherchee

    def test_needed_helper_pairs_dedupes_pairs_shared_by_two_currencies(self):
        # CHFJPY implique CHF et JPY a la fois: ne doit apparaitre qu'une fois.
        helper_pairs = needed_helper_pairs({"CHF", "JPY"})

        self.assertEqual(helper_pairs.count("CHFJPY"), 1)

    def test_needed_helper_pairs_for_default_pairs_matches_manual_union(self):
        # EURUSD -> devises EUR/USD -> 13 des 28 paires (les 7 de EUR + les 7
        # de USD, EURUSD dedupe une fois).
        helper_pairs = needed_helper_pairs(needed_currencies(DEFAULT_PAIRS))

        self.assertEqual(len(helper_pairs), 13)
        self.assertEqual(helper_pairs.count("EURUSD"), 1)

    def test_pair_check_compact_line_concatenates_icons_with_no_label(self):
        rows_by_pair = {
            "EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"},
            "GBPUSD": {"pair": "GBPUSD", "asset_type": "PAIR"},
        }
        price_trends = {
            "EURUSD": {"pips_vs_06h": 20.0, "pips_vs_previous_run": 3.0},
            "GBPUSD": {"pips_vs_06h": -5.0, "pips_vs_previous_run": -1.0},
        }

        line = pair_check_compact_line("EURUSD", rows_by_pair, price_trends, {})

        self.assertEqual(line, "EURUSD 🟢🟢")

    def test_pair_check_compact_line_none_without_any_exploitable_signal(self):
        self.assertIsNone(pair_check_compact_line("EURUSD", {}, {}, {}))

    def test_index_chg_lines_matches_full_momentum_rendering(self):
        rows = [
            index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
            index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
        ]

        lines = index_chg_lines(rows)

        self.assertEqual(lines[0], "💱 INDEX CHG%D")
        self.assertTrue(lines[1].startswith("🟢 EUR ("))  # score plus fort en tete
        self.assertTrue(lines[2].startswith("🔴 USD ("))

    def test_index_chg_lines_empty_without_index_rows(self):
        self.assertEqual(index_chg_lines([]), [])

    def test_currency_consensus_ball_bull_when_four_of_five_real_votes_agree(self):
        # Renko M/W/D + D1_IMP21 = 4 BULL, H1_IMP21 dissident -> reste BULL
        # (>= CURRENCY_CONSENSUS_THRESHOLD malgre le dissident).
        row = make_currency_row(
            renko=("BULL", "BULL", "BULL"),
            d1=("BULL", 10.0, 5.0, -2.0),
            h1=("BEAR", -3.0, 0.0, 1.0),
        )
        self.assertEqual(currency_consensus_ball(row), "🟢")

    def test_currency_consensus_ball_bear_when_four_of_five_real_votes_agree(self):
        row = make_currency_row(
            renko=("BEAR", "BEAR", "BEAR"),
            d1=("BEAR", -10.0, -5.0, 2.0),
            h1=("BULL", 3.0, 0.0, -1.0),
        )
        self.assertEqual(currency_consensus_ball(row), "🔴")

    def test_currency_consensus_ball_neutral_without_a_clear_majority(self):
        # 3 BULL (RENKO_M, RENKO_W, D1_IMP21) / 2 BEAR (RENKO_D, H1_IMP21) --
        # sous CURRENCY_CONSENSUS_THRESHOLD (4), pas de consensus net.
        row = make_currency_row(
            renko=("BULL", "BULL", "BEAR"),
            d1=("BULL", 10.0, 5.0, -2.0),
            h1=("BEAR", -3.0, 0.0, 1.0),
        )
        self.assertEqual(currency_consensus_ball(row), "⚪")

    def test_currency_consensus_ball_ignores_the_six_averages(self):
        # Memes 5 votes reels (Renko M/W/D + D1/H1 IMP21), moyennes opposees
        # -> meme boule: les moyennes ne comptent pas dans le consensus.
        confirming_averages = make_currency_row(
            renko=("BULL", "BULL", "BULL"), d1=("BULL", 50.0, 40.0, 30.0), h1=("BULL", 20.0, 15.0, 10.0),
        )
        contradicting_averages = make_currency_row(
            renko=("BULL", "BULL", "BULL"), d1=("BULL", -50.0, -40.0, -30.0), h1=("BULL", -20.0, -15.0, -10.0),
        )
        self.assertEqual(
            currency_consensus_ball(confirming_averages), currency_consensus_ball(contradicting_averages),
        )

    def test_index_chg_lines_appends_currency_consensus_ball_when_provided(self):
        rows = [
            index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
            index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
        ]
        imp_by_currency = {"EUR": make_currency_row(renko=("BULL", "BULL", "BULL"))}

        with_balls = index_chg_lines(rows, imp_by_currency)
        without_balls = index_chg_lines(rows)

        eur_line = next(line for line in with_balls if line.startswith("🟢 EUR ("))
        usd_line = next(line for line in with_balls if line.startswith("🔴 USD ("))
        base_usd_line = next(line for line in without_balls if line.startswith("🔴 USD ("))

        self.assertTrue(eur_line.endswith(" 🟢"))  # boule de consensus ajoutee pour EUR
        self.assertEqual(usd_line, base_usd_line)  # USD absent de imp_by_currency -> inchangee

    def test_index_chg_lines_without_imp_by_currency_is_unchanged(self):
        rows = [index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57)]
        self.assertEqual(index_chg_lines(rows), index_chg_lines(rows, None))

    def test_aligned_currency_names_keeps_only_double_confirmations(self):
        # JPY: jour BEAR + consensus BEAR -> doublement BEAR.
        # USD: jour BULL + consensus BULL -> doublement BULL.
        # AUD: jour BEAR mais pas de donnee consensus (absente de
        # imp_by_currency) -> ignoree, pas assez d'information.
        rows = [
            index_row("JYX", "JPY", 1, 1, 1, daily_chg=-1.66),
            index_row("AUX", "AUD", 1, 1, 1, daily_chg=-0.43),
            index_row("USX", "USD", 1, 1, 1, daily_chg=0.28),
        ]
        imp_by_currency = {
            "JPY": make_currency_row(
                renko=("BEAR", "BEAR", "BEAR"), d1=("BEAR", -10.0, -5.0, 2.0), h1=("BULL", 3.0, 0.0, -1.0),
            ),
            "USD": make_currency_row(),  # BULL par defaut
        }

        bull, bear = aligned_currency_names(rows, imp_by_currency)

        self.assertEqual(bull, ["USD"])
        self.assertEqual(bear, ["JPY"])

    def test_aligned_currency_names_excludes_contradictory_currency(self):
        # GBP: jour BEAR mais consensus BULL -- contradictoire, meme avec un
        # daily_chg extreme, ne doit jamais ressortir comme "faible".
        rows = [index_row("GBX", "GBP", 1, 1, 1, daily_chg=-5.0)]
        imp_by_currency = {"GBP": make_currency_row()}  # consensus BULL par defaut

        bull, bear = aligned_currency_names(rows, imp_by_currency)

        self.assertEqual((bull, bear), ([], []))

    def test_aligned_currency_names_excludes_neutral_consensus(self):
        rows = [index_row("EUX", "EUR", 1, 1, 1, daily_chg=0.50)]
        imp_by_currency = {"EUR": make_currency_row(
            renko=("BULL", "BULL", "BEAR"), d1=("BULL", 10.0, 5.0, -2.0), h1=("BEAR", -3.0, 0.0, 1.0),
        )}  # NEUTRAL (3/5 seulement, cf. imp_trend tests)

        bull, bear = aligned_currency_names(rows, imp_by_currency)

        self.assertEqual((bull, bear), ([], []))

    def test_best_pair_names_pairs_a_bull_with_a_bear_currency(self):
        rows = [
            index_row("JYX", "JPY", 1, 1, 1, daily_chg=-1.66),
            index_row("USX", "USD", 1, 1, 1, daily_chg=0.28),
        ]
        imp_by_currency = {
            "JPY": make_currency_row(
                renko=("BEAR", "BEAR", "BEAR"), d1=("BEAR", -10.0, -5.0, 2.0), h1=("BULL", 3.0, 0.0, -1.0),
            ),
            "USD": make_currency_row(),
        }

        self.assertEqual(best_pair_names(rows, imp_by_currency), ["USDJPY"])

    def test_best_pair_names_reverses_to_the_real_pair_when_forward_does_not_exist(self):
        # AUD doublement forte, GBP doublement faible -> "AUDGBP" n'existe pas
        # parmi les 28 paires OANDA (seul GBPAUD existe) -> doit renvoyer
        # GBPAUD, pas fabriquer un symbole inexistant.
        rows = [
            index_row("AUX", "AUD", 1, 1, 1, daily_chg=0.31),
            index_row("GBX", "GBP", 1, 1, 1, daily_chg=-1.25),
        ]
        imp_by_currency = {
            "AUD": make_currency_row(),
            "GBP": make_currency_row(
                renko=("BEAR", "BEAR", "BEAR"), d1=("BEAR", -10.0, -5.0, 2.0), h1=("BULL", 3.0, 0.0, -1.0),
            ),
        }

        self.assertEqual(best_pair_names(rows, imp_by_currency), ["GBPAUD"])

    def test_best_pair_names_one_per_combination_when_several_qualify(self):
        # 2 devises doublement BULL x 1 doublement BEAR -> 2 BEST PAIRE.
        rows = [
            index_row("USX", "USD", 1, 1, 1, daily_chg=0.28),
            index_row("AUX", "AUD", 1, 1, 1, daily_chg=0.31),
            index_row("JYX", "JPY", 1, 1, 1, daily_chg=-1.66),
        ]
        bear_jpy = make_currency_row(
            renko=("BEAR", "BEAR", "BEAR"), d1=("BEAR", -10.0, -5.0, 2.0), h1=("BULL", 3.0, 0.0, -1.0),
        )
        imp_by_currency = {"USD": make_currency_row(), "AUD": make_currency_row(), "JPY": bear_jpy}

        pairs = best_pair_names(rows, imp_by_currency)

        self.assertEqual(set(pairs), {"USDJPY", "AUDJPY"})

    def test_best_pair_names_empty_without_both_sides(self):
        rows = [index_row("USX", "USD", 1, 1, 1, daily_chg=0.28)]
        imp_by_currency = {"USD": make_currency_row()}

        self.assertEqual(best_pair_names(rows, imp_by_currency), [])

    def test_best_pair_names_empty_without_rows(self):
        self.assertEqual(best_pair_names([], {}), [])

    def test_best_pair_names_only_currency_restricts_to_that_currency_as_weak(self):
        # JPY doublement BEAR, USD et AUD doublement BULL -> avec
        # only_currency="JPY", seules les combinaisons impliquant JPY
        # ressortent (pas AUDUSD, qui ne touche pas JPY).
        rows = [
            index_row("USX", "USD", 1, 1, 1, daily_chg=0.28),
            index_row("AUX", "AUD", 1, 1, 1, daily_chg=0.31),
            index_row("JYX", "JPY", 1, 1, 1, daily_chg=-1.66),
        ]
        bear_jpy = make_currency_row(
            renko=("BEAR", "BEAR", "BEAR"), d1=("BEAR", -10.0, -5.0, 2.0), h1=("BULL", 3.0, 0.0, -1.0),
        )
        imp_by_currency = {"USD": make_currency_row(), "AUD": make_currency_row(), "JPY": bear_jpy}

        pairs = best_pair_names(rows, imp_by_currency, only_currency="JPY")

        self.assertEqual(set(pairs), {"USDJPY", "AUDJPY"})
        self.assertNotIn("AUDUSD", pairs)

    def test_best_pair_names_only_currency_restricts_to_that_currency_as_strong(self):
        # JPY doublement BULL, GBP doublement BEAR -> only_currency="JPY"
        # associe JPY (forte) a GBP (faible) -- "JPYGBP" n'existe pas, doit
        # utiliser l'ordre reel GBPJPY.
        rows = [
            index_row("JYX", "JPY", 1, 1, 1, daily_chg=0.50),
            index_row("GBX", "GBP", 1, 1, 1, daily_chg=-0.80),
        ]
        imp_by_currency = {
            "JPY": make_currency_row(),  # BULL par defaut
            "GBP": make_currency_row(
                renko=("BEAR", "BEAR", "BEAR"), d1=("BEAR", -10.0, -5.0, 2.0), h1=("BULL", 3.0, 0.0, -1.0),
            ),
        }

        self.assertEqual(best_pair_names(rows, imp_by_currency, only_currency="JPY"), ["GBPJPY"])

    def test_best_pair_names_only_currency_empty_when_not_aligned(self):
        # JPY contradictoire (jour BEAR, consensus BULL) -> ni doublement
        # BULL ni doublement BEAR, aucune combinaison ne le concerne.
        rows = [
            index_row("JYX", "JPY", 1, 1, 1, daily_chg=-0.50),
            index_row("USX", "USD", 1, 1, 1, daily_chg=0.28),
        ]
        imp_by_currency = {"JPY": make_currency_row(), "USD": make_currency_row()}  # BULL/BULL -> contradictoire pour JPY

        self.assertEqual(best_pair_names(rows, imp_by_currency, only_currency="JPY"), [])

    def test_best_pair_lines_renders_the_same_billes_as_a_paires_line(self):
        rows_by_pair = {"EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"}}
        price_trends = {"EURUSD": {"pips_vs_06h": 20.0}}

        lines = best_pair_lines(["EURUSD"], rows_by_pair, price_trends, {})

        self.assertEqual(
            lines, ["🏆 BEST PAIRE", pair_check_compact_line(
                "EURUSD", rows_by_pair, price_trends, {},
            )],
        )

    def test_best_pair_lines_empty_without_any_pair(self):
        self.assertEqual(best_pair_lines([], {}, {}, {}), [])

    def test_best_pair_lines_empty_without_exploitable_signal(self):
        self.assertEqual(best_pair_lines(["EURUSD"], {}, {}, {}), [])

    def test_best_pair_lines_uses_the_custom_label(self):
        rows_by_pair = {"EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"}}
        price_trends = {"EURUSD": {"pips_vs_06h": 20.0}}

        lines = best_pair_lines(["EURUSD"], rows_by_pair, price_trends, {}, label="🏆 BEST PAIRE JPY")

        self.assertEqual(lines[0], "🏆 BEST PAIRE JPY")

    def test_build_message_lists_one_compact_line_per_pair_under_a_paires_label(self):
        rows_by_pair = {
            "EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"},
            "GBPUSD": {"pair": "GBPUSD", "asset_type": "PAIR"},
        }
        price_trends = {
            "EURUSD": {"pips_vs_06h": 20.0},
            "GBPUSD": {"pips_vs_06h": -5.0},
        }
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, has_content = build_message(
            ["EURUSD"], rows_by_pair, price_trends, {}, now,
        )

        self.assertTrue(has_content)
        self.assertEqual(message, "\n".join([
            "📐 PAIRE_CHECK", "",
            "PAIRES",
            "EURUSD 🟢",
            "", "⏰ 2026-07-16 10:00 Paris",
        ]))

    def test_build_message_adds_index_chg_block_before_paires(self):
        rows_by_pair = {"EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"}}
        price_trends = {"EURUSD": {"pips_vs_06h": 20.0}}
        index_by_currency = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
            "USD": index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
        }
        # EUR doublement BULL, USD doublement BEAR -> BEST PAIRE = EURUSD.
        imp_by_currency = {
            "EUR": make_currency_row(),
            "USD": make_currency_row(
                renko=("BEAR", "BEAR", "BEAR"), d1=("BEAR", -10.0, -5.0, 2.0), h1=("BULL", 3.0, 0.0, -1.0),
            ),
        }
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, has_content = build_message(
            ["EURUSD"], rows_by_pair, price_trends, index_by_currency, now, imp_by_currency,
        )

        self.assertTrue(has_content)
        self.assertIn("💱 INDEX CHG%D", message)
        self.assertIn("🏆 BEST PAIRE", message)
        self.assertIn("PAIRES", message)
        self.assertLess(message.index("💱 INDEX CHG%D"), message.index("🏆 BEST PAIRE"))
        self.assertLess(message.index("🏆 BEST PAIRE"), message.index("PAIRES"))
        # EUR verte, USD rouge ici -> BEST PAIRE = EURUSD, memes billes que la
        # ligne PAIRES (qui suit) puisque c'est la meme paire dans ce test.
        after_paires = message[message.index("PAIRES"):]
        self.assertIn("EURUSD 🟢", after_paires)

    def test_build_message_without_imp_by_currency_has_no_best_pair_section(self):
        # Sans donnee de consensus, aligned_currency_names ne peut rien
        # valider -> pas de BEST PAIRE, meme avec des devises INDEX fournies.
        rows_by_pair = {"EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"}}
        price_trends = {"EURUSD": {"pips_vs_06h": 20.0}}
        index_by_currency = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
            "USD": index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
        }
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, _ = build_message(
            ["EURUSD"], rows_by_pair, price_trends, index_by_currency, now,
        )

        self.assertNotIn("🏆 BEST PAIRE", message)

    def test_build_message_adds_a_focus_currency_section_after_the_general_one(self):
        rows_by_pair = {
            "EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"},
            "EURJPY": {"pair": "EURJPY", "asset_type": "PAIR"},
        }
        price_trends = {
            "EURUSD": {"pips_vs_06h": 20.0},
            "EURJPY": {"pips_vs_06h": 15.0},
        }
        index_by_currency = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
            "USD": index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
            "JPY": index_row("JYX", "JPY", -1, -1, -1, daily_chg=-1.66),
        }
        # EUR doublement BULL, USD et JPY doublement BEAR -> general = EURUSD
        # + EURJPY, focus JPY = EURJPY seulement (pas EURUSD, USD n'est pas
        # la devise ciblee).
        imp_by_currency = {
            "EUR": make_currency_row(),
            "USD": make_currency_row(
                renko=("BEAR", "BEAR", "BEAR"), d1=("BEAR", -10.0, -5.0, 2.0), h1=("BULL", 3.0, 0.0, -1.0),
            ),
            "JPY": make_currency_row(
                renko=("BEAR", "BEAR", "BEAR"), d1=("BEAR", -10.0, -5.0, 2.0), h1=("BULL", 3.0, 0.0, -1.0),
            ),
        }
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, _ = build_message(
            ["EURUSD"], rows_by_pair, price_trends, index_by_currency, now, imp_by_currency, ["JPY"],
        )

        self.assertIn("🏆 BEST PAIRE", message)
        self.assertIn("🏆 BEST PAIRE JPY", message)
        self.assertLess(message.index("🏆 BEST PAIRE\n"), message.index("🏆 BEST PAIRE JPY"))
        self.assertLess(message.index("🏆 BEST PAIRE JPY"), message.index("PAIRES"))
        general_section = message[message.index("🏆 BEST PAIRE\n"):message.index("🏆 BEST PAIRE JPY")]
        focus_section = message[message.index("🏆 BEST PAIRE JPY"):message.index("PAIRES")]
        self.assertIn("EURUSD", general_section)
        self.assertIn("EURJPY", general_section)  # les deux dans la section generale
        self.assertIn("EURJPY", focus_section)
        self.assertNotIn("EURUSD", focus_section)  # focus JPY: pas de paire hors JPY

    def test_build_message_without_focus_currencies_has_no_focus_section(self):
        rows_by_pair = {"EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"}}
        price_trends = {"EURUSD": {"pips_vs_06h": 20.0}}
        index_by_currency = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
            "USD": index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
        }
        imp_by_currency = {
            "EUR": make_currency_row(),
            "USD": make_currency_row(
                renko=("BEAR", "BEAR", "BEAR"), d1=("BEAR", -10.0, -5.0, 2.0), h1=("BULL", 3.0, 0.0, -1.0),
            ),
        }
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, _ = build_message(
            ["EURUSD"], rows_by_pair, price_trends, index_by_currency, now, imp_by_currency,
        )

        self.assertNotIn("BEST PAIRE JPY", message)

    def test_build_message_forwards_imp_by_currency_to_index_chg_lines(self):
        rows_by_pair = {"EURUSD": {"pair": "EURUSD", "asset_type": "PAIR"}}
        price_trends = {"EURUSD": {"pips_vs_06h": 20.0}}
        index_by_currency = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
            "USD": index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
        }
        imp_by_currency = {"EUR": make_currency_row(renko=("BULL", "BULL", "BULL"))}
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, _ = build_message(
            ["EURUSD"], rows_by_pair, price_trends, index_by_currency, now, imp_by_currency,
        )

        eur_line = next(line for line in message.splitlines() if line.startswith("🟢 EUR ("))
        self.assertTrue(eur_line.endswith(" 🟢"))  # boule de consensus, cf. make_currency_row par defaut

    def test_build_message_flags_no_content_when_no_pair_scores(self):
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, has_content = build_message(["EURUSD"], {}, {}, {}, now)

        self.assertFalse(has_content)
        self.assertIn("📐 PAIRE_CHECK", message)
        self.assertNotIn("PAIRES", message)
        self.assertNotIn("EURUSD", message)

    def test_build_message_has_content_from_trend_lines_alone(self):
        # Meme sans aucune paire exploitable (avant 06h Paris par ex.),
        # trend_lines suffit a considerer le message exploitable.
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        message, has_content = build_message(
            ["EURUSD"], {}, {}, {}, now, trend_lines=["📈 TENDANCE", "🟢 EURUSD (100.00%)"],
        )

        self.assertTrue(has_content)
        self.assertIn("📈 TENDANCE", message)
        self.assertIn("🟢 EURUSD (100.00%)", message)
        self.assertLess(message.index("📈 TENDANCE"), message.index("⏰"))

    def test_pair_index_icon_compares_signed_strength_of_the_two_currencies(self):
        # EUR rouge de -0.08 (le moins baissier), USD rouge de -0.16: EUR est
        # relativement plus fort -> EURUSD monte, meme si les 2 sont rouges.
        index_by_currency = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=-0.08),
            "USD": index_row("DXY", "USD", 1, 1, 1, daily_chg=-0.16),
        }

        self.assertEqual(pair_index_icon("EURUSD", index_by_currency), "🟢")

    def test_pair_index_icon_none_when_a_currency_index_is_missing(self):
        index_by_currency = {"EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.0)}

        self.assertIsNone(pair_index_icon("EURUSD", index_by_currency))

    def test_pair_index_icon_none_for_unrecognized_pair(self):
        self.assertIsNone(pair_index_icon("NOTAPAIR", {}))

    def test_update_index_trend_state_accumulates_up_and_down_across_runs(self):
        bull_index = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
            "USD": index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
        }
        bear_index = {
            "EUR": index_row("EXY", "EUR", -1, -1, -1, daily_chg=-1.57),
            "USD": index_row("DXY", "USD", 1, 1, 1, daily_chg=1.61),
        }
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        state = {}
        for index_by_currency in (bull_index, bull_index, bull_index, bull_index, bull_index, bear_index, bear_index):
            state = update_index_trend_state(state, ["EURUSD"], index_by_currency, now)

        self.assertEqual(state["pairs"]["EURUSD"], {"up": 5, "down": 2})

    def test_update_index_trend_state_keeps_accumulating_across_midnight(self):
        # Le jour de comptage va de 5h a 5h Paris (pas minuit a minuit): un
        # run juste apres minuit continue d'alimenter le meme compteur.
        index_by_currency = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
            "USD": index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
        }
        state = update_index_trend_state(
            {}, ["EURUSD"], index_by_currency, dt.datetime(2026, 7, 16, 23, 0, tzinfo=PARIS),
        )
        self.assertEqual(state["pairs"]["EURUSD"], {"up": 1, "down": 0})

        state = update_index_trend_state(
            state, ["EURUSD"], index_by_currency, dt.datetime(2026, 7, 17, 0, 5, tzinfo=PARIS),
        )

        self.assertEqual(state["pairs"]["EURUSD"], {"up": 2, "down": 0})

    def test_update_index_trend_state_resets_at_5am_paris(self):
        index_by_currency = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.61),
            "USD": index_row("DXY", "USD", -1, -1, -1, daily_chg=-1.57),
        }
        state = update_index_trend_state(
            {}, ["EURUSD"], index_by_currency, dt.datetime(2026, 7, 17, 4, 59, tzinfo=PARIS),
        )
        self.assertEqual(state["pairs"]["EURUSD"], {"up": 1, "down": 0})

        state = update_index_trend_state(
            state, ["EURUSD"], index_by_currency, dt.datetime(2026, 7, 17, 5, 1, tzinfo=PARIS),
        )

        self.assertEqual(state["pairs"]["EURUSD"], {"up": 1, "down": 0})

    def test_update_index_trend_state_keeps_counts_on_a_tie_or_missing_data(self):
        tied_index = {
            "EUR": index_row("EXY", "EUR", 1, 1, 1, daily_chg=1.0),
            "USD": index_row("DXY", "USD", 1, 1, 1, daily_chg=1.0),
        }
        now = dt.datetime(2026, 7, 16, 10, 0, tzinfo=PARIS)

        state = {"date": "2026-07-16", "pairs": {"EURUSD": {"up": 3, "down": 1}}}
        state = update_index_trend_state(state, ["EURUSD"], tied_index, now)
        self.assertEqual(state["pairs"]["EURUSD"], {"up": 3, "down": 1})

        state = update_index_trend_state(state, ["EURUSD"], {}, now)
        self.assertEqual(state["pairs"]["EURUSD"], {"up": 3, "down": 1})

    def test_index_trend_lines_reports_majority_direction_and_percentage(self):
        state = {"date": "2026-07-16", "pairs": {"EURUSD": {"up": 5, "down": 2}}}

        self.assertEqual(
            index_trend_lines(["EURUSD"], state),
            ["📈 TENDANCE", "🟢 EURUSD (71.43%)"],
        )

    def test_index_trend_lines_flips_ball_when_down_is_majority(self):
        state = {"date": "2026-07-16", "pairs": {"EURUSD": {"up": 2, "down": 5}}}

        self.assertEqual(
            index_trend_lines(["EURUSD"], state),
            ["📈 TENDANCE", "🔴 EURUSD (71.43%)"],
        )

    def test_index_trend_lines_empty_without_any_decisive_run_yet(self):
        state = {"date": "2026-07-16", "pairs": {"EURUSD": {"up": 0, "down": 0}}}

        self.assertEqual(index_trend_lines(["EURUSD"], state), [])
        self.assertEqual(index_trend_lines(["EURUSD"], {}), [])


if __name__ == "__main__":
    unittest.main()
