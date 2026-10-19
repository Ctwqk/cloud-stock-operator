"""Web control panel: rendering, API calls, outcome handling, plot proxy."""

from __future__ import annotations

from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import responses

import web_app

ACCOUNT = "123456789012"
PK = f"ACCOUNT#{ACCOUNT}"
API = "https://api.example.test/prod"
PLOT_API = "https://plots.example.test/prod"


@pytest.fixture
def client(cloud, monkeypatch):
    monkeypatch.setenv("MGMT_API_BASE_URL", API + "/")
    monkeypatch.setenv("NETWORTH_API_BASE_URL", PLOT_API)
    monkeypatch.setenv("MGMT_API_KEY", "k-123")
    monkeypatch.setattr(web_app, "OPERATION_POLL_S", 0.6)
    monkeypatch.setattr(web_app.time, "sleep", lambda s: None)
    web_app.app.config["TESTING"] = True
    return web_app.app.test_client()


def flash(response):
    """(message, is_error, extra query args) carried by the redirect back to the index."""
    query = parse_qs(urlparse(response.headers["Location"]).query)
    return query["m"][0], query["e"][0] == "1", query


class TestIndex:
    def test_empty_state(self, client):
        html = client.get("/").get_data(as_text=True)
        assert "Trading Control Panel" in html
        assert "No watchlist entries yet." in html
        assert "No trades yet." in html
        assert API in html  # trailing slash normalised away

    def test_renders_watchlist_summary_and_trades(self, client, table):
        table.put_item(
            Item={
                "PK": PK,
                "SK": "SUMMARY",
                "current_cash_managed": Decimal("1234.5"),
                "positions_value": Decimal("200"),
                "current_net_worth": Decimal("1434.5"),
            }
        )
        table.put_item(
            Item={
                "PK": PK,
                "SK": "STOCK#AAPL",
                "symbol": "AAPL",
                "shares_managed": 4,
                "current_level_score": 2,
                "threshold_abs": 3,
                "last_fetched_price": Decimal("232.45"),
                "last_headline": "Apple surges",
                "last_sentiment": 1,
                "is_deleted": False,
            }
        )
        table.put_item(
            Item={
                "PK": PK,
                "SK": "STOCK#OLD",
                "symbol": "OLD",
                "is_deleted": True,
            }
        )
        for i, status in enumerate(["EXECUTED_VIRTUAL", "REJECTED_NO_PRICE"]):
            table.put_item(
                Item={
                    "PK": PK,
                    "SK": f"TRADE#2025-12-0{i + 1}T10:00:00.000000Z#d{i}",
                    "symbol": "AAPL",
                    "side": "BUY",
                    "status": status,
                    "quantity": 1,
                    "created_at": f"2025-12-0{i + 1}T10:00:00.000000Z",
                    "last_price": Decimal("232.45"),
                    "score": 3,
                }
            )

        html = client.get("/").get_data(as_text=True)

        assert "1,234.50" in html and "1,434.50" in html
        assert "232.45" in html and "Apple surges" in html
        assert "deleted" in html  # OLD is shown, flagged
        assert html.index("rejected no price") < html.index("executed virtual")  # newest first

    def test_data_errors_are_shown_not_raised(self, client, monkeypatch):
        monkeypatch.setenv("TABLE_NAME", "does-not-exist")
        response = client.get("/")
        assert response.status_code == 200
        assert "could not load data" in response.get_data(as_text=True)

    def test_message_is_displayed(self, client):
        html = client.get("/?m=Hello+there&e=1").get_data(as_text=True)
        assert 'class="msg-err"' in html and "Hello there" in html

    def test_healthz(self, client):
        assert client.get("/healthz").get_json() == {"status": "ok"}

    def test_money_formatting(self):
        assert web_app.money(Decimal("1234.567")) == "1,234.57"
        assert web_app.money(None) == "-" and web_app.money("x") == "-"


class TestCommands:
    @responses.activate
    def test_async_command_waits_for_the_outcome(self, client):
        responses.post(API + "/watchlist", json={"op_id": "op1", "status": "PENDING"}, status=202)
        responses.get(API + "/operations/op1", json={"op_id": "op1", "status": "PENDING"})
        responses.get(API + "/operations/op1", json={"op_id": "op1", "status": "APPLIED"})

        response = client.post("/watchlist/add", data={"symbol": "aapl", "shares_managed": "3"})

        message, is_error, _ = flash(response)
        assert message == "Add AAPL: applied" and not is_error
        sent = responses.calls[0].request
        assert sent.headers["x-api-key"] == "k-123"
        assert sent.body == b'{"symbol": "AAPL", "shares_managed": 3}'

    @responses.activate
    def test_rejection_reason_reaches_the_user(self, client):
        responses.post(API + "/account/cash/adjust", json={"op_id": "op2"}, status=202)
        responses.get(
            API + "/operations/op2",
            json={"status": "REJECTED", "reason": "managed cash would exceed the allowed maximum"},
        )
        response = client.post(
            "/account/cash/adjust", data={"delta_cash": "5000", "max_allowed_cash": "1000"}
        )
        message, is_error, _ = flash(response)
        assert is_error and "rejected" in message and "exceed the allowed maximum" in message

    @responses.activate
    def test_still_pending_after_timeout(self, client):
        responses.post(API + "/watchlist", json={"op_id": "op3"}, status=202)
        responses.get(API + "/operations/op3", json={"status": "PENDING"})
        message, is_error, _ = flash(client.post("/watchlist/add", data={"symbol": "A"}))
        assert not is_error and "still processing" in message

    @responses.activate
    def test_api_error_is_surfaced(self, client):
        responses.post(API + "/watchlist", json={"error": "invalid symbol: '!!'"}, status=400)
        message, is_error, _ = flash(client.post("/watchlist/add", data={"symbol": "!!"}))
        assert is_error and "400" in message and "invalid symbol" in message

    @responses.activate
    def test_unreachable_api(self, client):
        responses.post(API + "/watchlist", body=requests.ConnectionError("refused"))
        message, is_error, _ = flash(client.post("/watchlist/add", data={"symbol": "A"}))
        assert is_error and "refused" in message

    def test_api_not_configured(self, client, monkeypatch):
        monkeypatch.delenv("MGMT_API_BASE_URL")
        message, is_error, _ = flash(client.post("/watchlist/add", data={"symbol": "A"}))
        assert is_error and "not configured" in message

    @pytest.mark.parametrize(
        "url, data",
        [
            ("/watchlist/add", {"symbol": "A", "shares_managed": "x"}),
            ("/watchlist/threshold", {"symbol": "A", "threshold_abs": "x"}),
            ("/account/cash/adjust", {"delta_cash": "x"}),
            ("/account/shares/adjust", {"symbol": "A", "delta_shares": "1.5"}),
        ],
    )
    def test_bad_form_input_is_a_message_not_a_500(self, client, url, data):
        response = client.post(url, data=data)
        assert response.status_code == 302 and flash(response)[1] is True

    @responses.activate
    def test_synchronous_endpoints(self, client):
        responses.delete(API + "/watchlist/AAPL", json={"message": "delete accepted"}, status=202)
        responses.post(API + "/watchlist/AAPL/threshold", json={"message": "threshold updated"})
        responses.post(
            API + "/watchlist/refresh", json={"message": "refresh requested"}, status=202
        )
        for url, data in [
            ("/watchlist/delete", {"symbol": "aapl"}),
            ("/watchlist/threshold", {"symbol": "aapl", "threshold_abs": "2"}),
            ("/watchlist/refresh", {}),
        ]:
            message, is_error, _ = flash(client.post(url, data=data))
            assert not is_error, message


class TestPlot:
    @responses.activate
    def test_plot_request_redirects_with_the_key(self, client):
        responses.get(
            PLOT_API + "/networth",
            json={"points": 12, "s3_key": "plots/networth/1/x.png"},
            match=[responses.matchers.query_param_matcher({"start_time": "2025-12-01T00:00:00Z"})],
        )
        response = client.post(
            "/networth/plot", data={"start_time": "2025-12-01T00:00:00Z", "end_time": ""}
        )
        message, is_error, query = flash(response)
        assert not is_error and "12 snapshot" in message
        assert query["plot_key"] == ["plots/networth/1/x.png"]
        page = client.get(response.headers["Location"]).get_data(as_text=True)
        assert 'src="/plot?key=plots/networth/1/x.png"' in page

    def test_plot_proxy_serves_png_from_s3(self, client, cloud, s3):
        s3.put_object(Bucket=cloud.bucket_name, Key="plots/networth/1/x.png", Body=b"\x89PNG-data")
        monkey = client.get("/plot?key=plots/networth/1/x.png")
        assert monkey.status_code == 200 and monkey.mimetype == "image/png"
        assert monkey.data == b"\x89PNG-data"

    @pytest.mark.parametrize(
        "key", ["", "news/AAPL/x.json", "plots/networth/../../secret", "plots/other/x.png"]
    )
    def test_plot_proxy_only_serves_networth_plots(self, client, key):
        assert client.get("/plot", query_string={"key": key}).status_code == 400

    def test_missing_plot_is_404(self, client):
        assert client.get("/plot?key=plots/networth/1/none.png").status_code == 404

    def test_plot_api_not_configured(self, client, monkeypatch):
        monkeypatch.delenv("NETWORTH_API_BASE_URL")
        assert flash(client.post("/networth/plot", data={}))[1] is True
