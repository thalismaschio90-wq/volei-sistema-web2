from __future__ import annotations

from dataclasses import dataclass
import json
from urllib import error, parse, request

from .metrics import MetricCollector


@dataclass
class HttpResult:
    status_code: int
    body: bytes
    headers: dict[str, str]

    def json(self) -> dict:
        try:
            value = json.loads(self.body.decode("utf-8"))
            return value if isinstance(value, dict) else {"data": value}
        except Exception:
            return {}


class LoadHttpClient:
    def __init__(
        self,
        base_url: str,
        metrics: MetricCollector,
        *,
        timeout_seconds: float = 10.0,
        session_cookie: str = "",
        operator_token: str = "",
    ):
        self.base_url = base_url.rstrip("/")
        self.metrics = metrics
        self.timeout_seconds = timeout_seconds
        self.session_cookie = session_cookie
        self.operator_token = operator_token

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {
            "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
            "User-Agent": "VolleyTablePro-LoadTest/1.0",
            "X-VTP-Load-Test": "1",
        }
        if self.session_cookie:
            headers["Cookie"] = self.session_cookie
        if self.operator_token:
            headers["X-Operator-Token"] = self.operator_token
        if extra:
            headers.update(extra)
        return headers

    def request(
        self,
        metric_name: str,
        method: str,
        path: str,
        *,
        json_body: dict | None = None,
        form_body: dict | None = None,
    ) -> HttpResult:
        url = path if path.startswith(("http://", "https://")) else f"{self.base_url}{path}"
        data: bytes | None = None
        headers = self._headers()
        if json_body is not None:
            data = json.dumps(json_body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        elif form_body is not None:
            data = parse.urlencode(form_body).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        req = request.Request(url, data=data, headers=headers, method=method.upper())
        with self.metrics.timer(metric_name) as timer:
            try:
                with request.urlopen(req, timeout=self.timeout_seconds) as response:
                    body = response.read()
                    status = int(response.status)
                    if status >= 400:
                        timer.fail(f"HTTP {status}", status)
                    return HttpResult(status, body, dict(response.headers.items()))
            except error.HTTPError as exc:
                body = exc.read()
                timer.fail(f"HTTP {exc.code}", int(exc.code))
                return HttpResult(int(exc.code), body, dict(exc.headers.items()))
            except Exception as exc:
                timer.fail(repr(exc))
                raise

    def get(self, metric_name: str, path: str) -> HttpResult:
        return self.request(metric_name, "GET", path)

    def post_json(self, metric_name: str, path: str, payload: dict) -> HttpResult:
        return self.request(metric_name, "POST", path, json_body=payload)
