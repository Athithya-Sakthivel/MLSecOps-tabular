from __future__ import annotations

from collections.abc import Iterable
from html import escape

_GOOGLE_SVG = (
    '<svg viewBox="0 0 24 24" width="18" height="18" xmlns="http://www.w3.org/2000/svg">'
    '<path fill="#EA4335" d="M12 10.2v3.6h5.2c-.2 1.2-1.4 3.6-5.2 3.6-3.1 0-5.6-2.6-5.6-5.8S8.9 6.8 12 6.8c1.8 0 2.9.8 3.6 1.5l2.4-2.3C17.2 4 14.8 3 12 3 7.6 3 4 6.6 4 11s3.6 8 8 8c4.6 0 7-3.2 7-7.7 0-.5 0-.9-.1-1.1H12z"/>'
    "</svg>"
)
_MICROSOFT_SVG = (
    '<svg viewBox="0 0 24 24" width="18" height="18" xmlns="http://www.w3.org/2000/svg">'
    '<rect x="2" y="2" width="9" height="9" fill="#F35325"/>'
    '<rect x="13" y="2" width="9" height="9" fill="#81BC06"/>'
    '<rect x="2" y="13" width="9" height="9" fill="#05A6F0"/>'
    '<rect x="13" y="13" width="9" height="9" fill="#FFBA08"/>'
    "</svg>"
)
_GITHUB_SVG = (
    '<svg viewBox="0 0 24 24" width="18" height="18" xmlns="http://www.w3.org/2000/svg">'
    '<path fill="#111" d="M12 .5C5.6.5.5 5.6.5 12c0 5.1 3.3 9.4 7.9 10.9.6.1.8-.3.8-.6v-2.2c-3.2.7-3.9-1.4-3.9-1.4-.5-1.1-1.2-1.4-1.2-1.4-1-.7.1-.7.1-.7 1.1.1 1.7 1.1 1.7 1.1 1 .1 1.6.8 2 .6.1-.8.4-1.4.7-1.8-2.6-.3-5.4-1.3-5.4-5.8 0-1.3.5-2.4 1.3-3.2-.1-.3-.6-1.6.1-3.3 0 0 1-.3 3.3 1.3.9-.3 1.9-.5 2.9-.5s2 .2 2.9.5c2.3-1.6 3.3-1.3 3.3-1.3.7 1.7.2 3 .1 3.3.8.8 1.3 1.9 1.3 3.2 0 4.5-2.8 5.5-5.5 5.8.5.4.8 1.1.8 2.3v3.4c0 .3.2.8.8.6 4.6-1.5 7.9-5.8 7.9-10.9C23.5 5.6 18.4.5 12 .5z"/>'
    "</svg>"
)


def page(title: str, body: str, *, csp: str = "") -> str:
    meta = (
        f'<meta http-equiv="Content-Security-Policy" content="{escape(csp, quote=True)}">'
        if csp
        else ""
    )
    return (
        "<!doctype html>"
        "<html lang='en'>"
        "<head>"
        '<meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{escape(title)}</title>"
        f"{meta}"
        "</head>"
        f"<body>{body}</body>"
        "</html>"
    )


def _policy_section(policy_lines: Iterable[str] | None) -> str:
    lines = [line.strip() for line in (policy_lines or []) if str(line).strip()]
    if not lines:
        return ""

    items = "".join(
        f"<li style='margin:0 0 8px 0'>{escape(line)}</li>"
        for line in lines
    )
    return (
        "<div style='margin-top:18px;padding:14px 16px;border:1px solid #e5e7eb;"
        "border-radius:12px;background:#fafafa'>"
        "<div style='font-size:12px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;"
        "color:#6b7280;margin-bottom:10px;font-family:system-ui'>Access policy</div>"
        f"<ul style='margin:0;padding-left:18px;font-family:system-ui;font-size:14px;color:#374151'>{items}</ul>"
        "</div>"
    )


def login_page(
    app_name: str,
    providers: Iterable[str],
    provider_links: dict[str, str],
    hints: str,
    policy_lines: Iterable[str] | None = None,
) -> str:
    icons = {"google": _GOOGLE_SVG, "microsoft": _MICROSOFT_SVG, "github": _GITHUB_SVG}
    providers = [provider.strip().lower() for provider in providers if str(provider).strip()]

    if not providers:
        buttons = (
            "<div style='font-size:14px;color:#666;font-family:system-ui'>"
            "No providers enabled."
            "</div>"
        )
    else:
        rows = []
        for provider in providers:
            href = escape(provider_links.get(provider, "#"), quote=True)
            rows.append(
                "<a href='{href}' "
                "style='display:flex;align-items:center;justify-content:center;gap:8px;"
                "border:1px solid #d1d5db;border-radius:10px;padding:11px 14px;margin-bottom:12px;"
                "text-decoration:none;color:#111827;font-family:system-ui;background:#fff'>"
                "{icon}<span>Continue with {name}</span>"
                "</a>".format(
                    href=href,
                    icon=icons.get(provider, ""),
                    name=escape(provider.capitalize()),
                )
            )
        buttons = "\n".join(rows)

    body = (
        "<div style='background:linear-gradient(180deg,#f8fafc 0%,#eef2ff 100%);min-height:100vh;"
        "display:flex;align-items:center;justify-content:center;padding:24px'>"
        "<div style='max-width:520px;width:100%'>"
        "<div style='background:#fff;padding:28px;border-radius:16px;"
        "box-shadow:0 10px 30px rgba(15,23,42,.08);border:1px solid #e5e7eb'>"
        f"<div style='font-family:system-ui;font-size:12px;letter-spacing:.08em;text-transform:uppercase;"
        "color:#6b7280;font-weight:700;margin-bottom:10px'>Sign in</div>"
        f"<h1 style='margin:0 0 10px 0;font-family:system-ui;font-size:26px;line-height:1.15;color:#0f172a'>"
        f"{escape(app_name)}"
        "</h1>"
        "<p style='margin:0 0 18px 0;font-family:system-ui;font-size:15px;color:#475569;line-height:1.5'>"
        "Use an enabled provider to continue."
        "</p>"
        f"{buttons}"
        f"{_policy_section(policy_lines)}"
        f"<div style='margin-top:18px;font-family:system-ui;font-size:13px;line-height:1.5;color:#6b7280'>"
        f"{escape(hints)}"
        "</div>"
        "</div></div></div>"
    )
    return page(
        app_name,
        body,
        csp=(
            "default-src 'none'; "
            "style-src 'unsafe-inline'; "
            "img-src 'self' data:; "
            "base-uri 'none'; "
            "frame-ancestors 'none'; "
            "form-action 'self';"
        ),
    )


def redirects_page(redirects: list[tuple[str, str]]) -> str:
    if not redirects:
        body = "<div style='font-family:system-ui;margin:32px'>No providers enabled.</div>"
    else:
        items = "".join(
            f"<li><strong>{escape(provider)}</strong>: <code>{escape(uri)}</code></li>"
            for provider, uri in redirects
        )
        body = (
            "<div style='font-family:system-ui;margin:32px'>"
            "<h2>Redirect URIs to register</h2>"
            f"<ul>{items}</ul>"
            "</div>"
        )
    return page(
        "Redirect URIs",
        body,
        csp="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none';",
    )


def success_page(message: str = "Authentication complete") -> str:
    body = (
        f"<div style='font-family:system-ui;margin:32px'><h2>{escape(message)}</h2>"
        "<p>You can return to the application.</p></div>"
    )
    return page(
        "Success",
        body,
        csp="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none';",
    )


def denied_page(
    title: str,
    message: str,
    details: str | None = None,
    allowed: str | None = None,
) -> str:
    parts = [f"<h2>{escape(title)}</h2>", f"<p>{escape(message)}</p>"]
    if allowed:
        parts.append(f"<p>Allowed: <code>{escape(allowed)}</code></p>")
    if details:
        parts.append(
            f"<div style='margin-top:8px;font-size:90%;color:#666'>{escape(details)}</div>"
        )
    body = "<div style='font-family:system-ui;margin:32px'>" + "".join(parts) + "</div>"
    return page(
        "Access denied",
        body,
        csp="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none';",
    )