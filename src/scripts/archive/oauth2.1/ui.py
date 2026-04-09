from __future__ import annotations

from collections.abc import Iterable
from html import escape

_GOOGLE_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" xmlns="http://www.w3.org/2000/svg"><path fill="#EA4335" d="M12 10.2v3.6h5.2c-.2 1.2-1.4 3.6-5.2 3.6-3.1 0-5.6-2.6-5.6-5.8S8.9 6.8 12 6.8c1.8 0 2.9.8 3.6 1.5l2.4-2.3C17.2 4 14.8 3 12 3 7.6 3 4 6.6 4 11s3.6 8 8 8c4.6 0 7-3.2 7-7.7 0-.5 0-.9-.1-1.1H12z"/></svg>'
_MICROSOFT_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" xmlns="http://www.w3.org/2000/svg"><rect x="2" y="2" width="9" height="9" fill="#F35325"/><rect x="13" y="2" width="9" height="9" fill="#81BC06"/><rect x="2" y="13" width="9" height="9" fill="#05A6F0"/><rect x="13" y="13" width="9" height="9" fill="#FFBA08"/></svg>'
_GITHUB_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" xmlns="http://www.w3.org/2000/svg"><path fill="#111" d="M12 .5C5.6.5.5 5.6.5 12c0 5.1 3.3 9.4 7.9 10.9.6.1.8-.3.8-.6v-2.2c-3.2.7-3.9-1.4-3.9-1.4-.5-1.1-1.2-1.4-1.2-1.4-1-.7.1-.7.1-.7 1.1.1 1.7 1.1 1.7 1.1 1 .1 1.6.8 2 .6.1-.8.4-1.4.7-1.8-2.6-.3-5.4-1.3-5.4-5.8 0-1.3.5-2.4 1.3-3.2-.1-.3-.6-1.6.1-3.3 0 0 1-.3 3.3 1.3.9-.3 1.9-.5 2.9-.5s2 .2 2.9.5c2.3-1.6 3.3-1.3 3.3-1.3.7 1.7.2 3 .1 3.3.8.8 1.3 1.9 1.3 3.2 0 4.5-2.8 5.5-5.5 5.8.5.4.8 1.1.8 2.3v3.4c0 .3.2.8.8.6 4.6-1.5 7.9-5.8 7.9-10.9C23.5 5.6 18.4.5 12 .5z"/></svg>'


def page(title: str, body: str, *, csp: str = '') -> str:
    meta = f'<meta http-equiv="Content-Security-Policy" content="{escape(csp, quote=True)}">' if csp else ''
    return f'<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(title)}</title>{meta}</head><body>{body}</body></html>'


def login_page(app_name: str, providers: Iterable[str], provider_links: dict[str, str], hints: str) -> str:
    icons = {'google': _GOOGLE_SVG, 'microsoft': _MICROSOFT_SVG, 'github': _GITHUB_SVG}
    providers = list(providers)
    if not providers:
        buttons = '<div style="font-size:14px;color:#666;font-family:system-ui">No providers enabled.</div>'
    else:
        rows = []
        for provider in providers:
            href = escape(provider_links[provider], quote=True)
            rows.append(
                f'<a href="{href}" style="display:flex;align-items:center;justify-content:center;border:1px solid #ddd;border-radius:8px;padding:10px 14px;margin-bottom:12px;text-decoration:none;color:#111;font-family:system-ui">{icons.get(provider, "")}<span style="margin-left:8px">Continue with {escape(provider.capitalize())}</span></a>'
            )
        buttons = '\n'.join(rows)
    body = (
        '<div style="background:#f7f7f7;min-height:100vh;display:flex;align-items:center;justify-content:center">'
        '<div style="max-width:460px;width:100%;padding:24px">'
        '<div style="background:#fff;padding:24px;border-radius:12px;box-shadow:0 2px 10px rgba(0,0,0,.06)">'
        f'<h1 style="margin:0 0 14px 0;font-family:system-ui;font-size:22px">{escape(app_name)} sign in</h1>'
        f'{buttons}'
        f'<div style="margin-top:20px;font-family:system-ui;font-size:12px;color:#6b7280">{hints}</div>'
        '</div></div></div>'
    )
    return page(app_name, body, csp="default-src 'none'; style-src 'unsafe-inline'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self';")


def redirects_page(redirects: list[tuple[str, str]]) -> str:
    if not redirects:
        body = '<div style="font-family:system-ui;margin:32px">No providers enabled.</div>'
    else:
        items = ''.join(f'<li><strong>{escape(provider)}</strong>: <code>{escape(uri)}</code></li>' for provider, uri in redirects)
        body = f'<div style="font-family:system-ui;margin:32px"><h2>Redirect URIs to register</h2><ul>{items}</ul></div>'
    return page('Redirect URIs', body, csp="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none';")


def success_page(message: str = 'Authentication complete') -> str:
    body = f'<div style="font-family:system-ui;margin:32px"><h2>{escape(message)}</h2><p>You can return to the application.</p></div>'
    return page('Success', body, csp="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none';")


def denied_page(title: str, message: str, details: str | None = None, allowed: str | None = None) -> str:
    parts = [f'<h2>{escape(title)}</h2>', f'<p>{escape(message)}</p>']
    if allowed:
        parts.append(f'<p>Allowed: <code>{escape(allowed)}</code></p>')
    if details:
        parts.append(f'<div style="margin-top:8px;font-size:90%;color:#666">{escape(details)}</div>')
    body = '<div style="font-family:system-ui;margin:32px">' + ''.join(parts) + '</div>'
    return page('Access denied', body, csp="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none';")