"""Phase 6: the numbers the dashboard derives, and the page it renders."""
import json

import build_dashboard
from build_dashboard import as_dicts, headline, render, sale_effects


def test_athena_text_becomes_numbers():
    assert as_dicts([["category", "n", "avg", "title"], ["Video_Games", "12", "4.5", ""]]) == [
        {"category": "Video_Games", "n": 12, "avg": 4.5, "title": None}]


def test_sale_effect_compares_the_days_before_with_the_days_after_and_skips_the_sale():
    # 10 reviews a day before, 1000 on the sale days, 30 a day after.
    daily = []
    for n in range(1, 32):
        count = 10 if n < 15 else 1000 if n in (15, 16) else 30
        daily.append({"category": "Toys_and_Games", "review_date": f"2021-03-{n:02d}", "review_count": count})
    [row] = sale_effects(daily, sales=[("Test sale", "2021-03-15", "2021-03-16")], days=7)
    assert (row["before_per_day"], row["after_per_day"], row["ratio"]) == (10.0, 30.0, 3.0)


def test_days_with_no_reviews_count_as_zero():
    daily = [{"category": "Video_Games", "review_date": "2021-03-01", "review_count": 70}]
    [row] = sale_effects(daily, sales=[("S", "2021-03-08", "2021-03-08")], days=7)
    assert row["before_per_day"] == 10.0 and row["after_per_day"] == 0.0 and row["ratio"] == 0.0


def test_headline_weights_averages_by_review_count():
    monthly = [
        {"category": "Electronics", "review_month": "2020-01", "review_count": 3, "rating_sum": 15, "verified_count": 3},
        {"category": "Electronics", "review_month": "2020-02", "review_count": 1, "rating_sum": 1, "verified_count": 0},
    ]
    h = headline(monthly)
    # (15 + 1) / 4 = 4.0, not the average of the monthly averages (5 + 1) / 2 = 3.0
    assert h["avg_rating"] == 4.0 and h["categories"]["Electronics"]["verified_share"] == 0.75
    assert (h["first_month"], h["last_month"]) == ("2020-01", "2020-02")


def test_render_embeds_the_data_and_cannot_be_broken_by_a_title(tmp_path):
    template = tmp_path / "t.html"
    template.write_text("<script>const DATA = /*DASHBOARD_DATA*/null;</script>")
    page = render({"falling": [{"product_title": "Best toy</script><script>alert(1)"}]}, str(template))
    assert page.count("</script>") == 1  # only the template's own
    payload = page[len("<script>const DATA = "):-len(";</script>")]
    assert json.loads(payload)["falling"][0]["product_title"] == "Best toy</script><script>alert(1)"


def test_the_real_template_has_the_placeholder():
    with open(build_dashboard.TEMPLATE, encoding="utf-8") as handle:
        assert "/*DASHBOARD_DATA*/null" in handle.read()
