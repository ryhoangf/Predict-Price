import json
import os, sys
from urllib.parse import urlparse, urljoin

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

ROOT = os.path.abspath(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config as config
import requests
from bs4 import BeautifulSoup

# ĐIỀN LINK CẦN TEST VÀO ĐÂY (Mercari, Rakuma, hay Yahoo đều được)
TEST_URL = "https://buyee.jp/item/jdirectitems/auction/b1229563666"

# Giống lệnh PowerShell: GET trực tiếp trang /detail (Referer = trang item không có /detail)
DEBUG_DETAIL_URL = "https://buyee.jp/item/jdirectitems/auction/b1229563666/detail"
DEBUG_ITEM_REFERER = "https://buyee.jp/item/jdirectitems/auction/b1229563666"


def _redact_proxy_url(proxy_url: str) -> str:
    try:
        p = urlparse(proxy_url)
        h, port = p.hostname or "", p.port or ""
        if p.username or p.password:
            return f"{p.scheme}://***:***@{h}:{port}"
        return f"{p.scheme}://{h}:{port}"
    except Exception:
        return "(không in được URL proxy)"


def _debug_buyee_timeout():
    """
    curl-cffi qua HTTP CONNECT tới buyee.jp thường cần connect > 8s.
    Script debug dùng (30, 90) khi curl-cffi; production dùng BUYEE_PROXY_TIMEOUT (mặc định config cũng 30,90).
    """
    if config.prefer_curl_cffi_for_buyee():
        return (30.0, 90.0)
    return getattr(config, "PROXY_TIMEOUT", (10, 45))


def _session_get_safe(sess, url: str, *, headers: dict, proxies: dict, timeout, step: str):
    try:
        return sess.get(url, headers=headers, proxies=proxies, timeout=timeout)
    except Exception as e:
        print(f"   ❌ [{step}] {type(e).__name__}: {e}")
        print(
            "   💡 Qua proxy, TLS handshake lâu: trong .env đặt BUYEE_PROXY_TIMEOUT=25,90 "
            "(hoặc cao hơn), hoặc đổi IP xoay."
        )
        return None


def check_proxy_xoay_like_powershell(
    detail_url: str = None,
    item_referer: str = None,
):
    """
    1) Làm mới + lấy proxy đúng luồng config (_xoay_proxies_dict).
    2) Thử A: GET /detail một lần (giống PS). Thử B: GET trang item (?lang=en) rồi /detail (giống scraper).
    """
    detail_url = detail_url or DEBUG_DETAIL_URL
    item_referer = item_referer or DEBUG_ITEM_REFERER

    print("=== Proxy xoay: kiểm tra lấy + cào Buyee /detail ===\n")
    if not config.PROXY_XOAY_KEY:
        print("❌ Chưa đặt PROXY_XOAY_KEY trong .env — không có proxy xoay.")
        return

    config.invalidate_rotating_proxy()
    proxies = config._xoay_proxies_dict(force_refresh=True)
    if not proxies:
        print("❌ Gọi API proxyxoay không ra proxy (status≠100 / JSON lỗi / parse proxyhttp lỗi).")
        diag = config.proxy_xoay_last_diagnostic()
        if diag:
            try:
                print("   Chi tiết API:", json.dumps(diag, ensure_ascii=False, indent=2))
            except (TypeError, ValueError):
                print("   Chi tiết API:", diag)
        else:
            print("   (Không có chi tiết — có thể PROXY_XOAY_KEY rỗng sau khi load .env.)")
        print("   Thử: PROXY_XOAY_API_PREFER_POST=1, kiểm tra key & whitelist IP máy chạy lệnh này.")
        return

    pu = proxies.get("http") or proxies.get("https") or ""
    print(f"✅ Đã lấy proxy (chuẩn config): {_redact_proxy_url(pu)}")
    print(f"   → Dùng làm: Invoke-WebRequest -Proxy '{_redact_proxy_url(pu)}' ...\n")
    if config.prefer_curl_cffi_for_buyee():
        print(
            f"🌐 Buyee HTTP: curl-cffi ({config.BUYEE_CURL_IMPERSONATE}) — TLS giống Chrome, tránh 202 từ requests thuần.\n"
        )
    else:
        print(
            "🌐 Buyee HTTP: requests (OpenSSL). AWS WAF thường trả 202 vì TLS ≠ Chrome.\n"
            "   → Cài: pip install curl-cffi  (sẽ tự dùng, không cần đổi env)\n"
            "   → Hoặc tắt cố ý: BUYEE_HTTP_CLIENT=requests nếu đã cài curl nhưng muốn thử requests.\n"
        )

    print(
        f"📋 BUYEE_SEND_COOKIE={config.BUYEE_SEND_COOKIE} | "
        f"curl-cffi={config.prefer_curl_cffi_for_buyee()} | "
        f"impersonate={getattr(config, 'BUYEE_CURL_IMPERSONATE', '')}\n"
    )

    timeout = _debug_buyee_timeout()
    if config.prefer_curl_cffi_for_buyee():
        print(
            f"⏱️  Timeout debug: connect {timeout[0]:.0f}s, read {timeout[1]:.0f}s "
            f"(curl qua proxy — mặc định 8s hay quá ngắn).\n"
        )

    def _analyze(label: str, r: requests.Response) -> bool:
        print(f"📊 {label} → HTTP {r.status_code} | {len(r.content)} bytes")
        if config.response_looks_like_buyee_waf_challenge(r):
            print("   🚨 WAF / thách thức (202 hoặc body awswaf).")
            if r.status_code == 202:
                print("   💡 202 từ Buyee thường là AWS WAF; PS (TLS .NET) đôi khi vẫn 200 cùng proxy.")
            if not (config.BUYEE_COOKIE or "").strip():
                print("   💡 Chưa có BUYEE_COOKIE — nếu cần đăng nhập, lấy cookie sau khi mở buyee.jp trên trình duyệt.")
            elif config.BUYEE_SEND_COOKIE:
                print(
                    "   💡 Đang gửi cookie từ .env qua IP proxy — thường không khớp session trình duyệt → WAF challenge.js."
                )
                print("      Thử: BUYEE_SEND_COOKIE=0 (IWR của bạn không gửi Cookie), hoặc A2 tự chạy sau đây.")
            else:
                print("   💡 BUYEE_SEND_COOKIE=0 — request không gửi cookie.")
            return False
        soup = BeautifulSoup(r.text, "html.parser")
        p = soup.select_one("p.m-itemDetail__content")
        if p:
            snippet = p.get_text(separator="\n", strip=True)[:240]
            print(f"   ✅ Có p.m-itemDetail__content. Đoạn đầu:\n   {snippet!r}\n")
            return True
        print("   ⚠️ Không thấy p.m-itemDetail__content.")
        return False

    try:
        try:
            with config.create_buyee_session() as sess:
                if hasattr(sess, "trust_env"):
                    sess.trust_env = False

                print("A0) Chỉ Accept */* + không Referer (rất gần IWR mặc định):\n")
                h0 = config.buyee_bare_headers_like_iwr()
                r0 = _session_get_safe(
                    sess,
                    detail_url,
                    headers=h0,
                    proxies=proxies,
                    timeout=timeout,
                    step="A0 bare IWR-like",
                )
                if r0 and _analyze("A0", r0):
                    try:
                        with open("debug_proxy_detail.html", "w", encoding="utf-8") as f:
                            f.write(r0.text)
                        print("📁 debug_proxy_detail.html\n")
                    except OSError:
                        pass
                    print("=== Xong ===")
                    return
                if r0:
                    try:
                        with open("debug_proxy_detail_A0.html", "w", encoding="utf-8") as f:
                            f.write(r0.text)
                    except OSError:
                        pass
                    print("   📁 Đã ghi debug_proxy_detail_A0.html\n")

                print("A) GET /detail có Referer trang item (header 'trình duyệt'):\n")
                h_direct = config.buyee_page_headers(referer=item_referer)
                r_a = _session_get_safe(
                    sess,
                    detail_url,
                    headers=h_direct,
                    proxies=proxies,
                    timeout=timeout,
                    step="A /detail",
                )
                if r_a and _analyze("A", r_a):
                    try:
                        with open("debug_proxy_detail.html", "w", encoding="utf-8") as f:
                            f.write(r_a.text)
                        print("📁 debug_proxy_detail.html\n")
                    except OSError:
                        pass
                    print("=== Xong ===")
                    return
                if r_a:
                    try:
                        with open("debug_proxy_detail_A.html", "w", encoding="utf-8") as f:
                            f.write(r_a.text)
                    except OSError:
                        pass
                    print("   📁 Đã ghi debug_proxy_detail_A.html\n")

                if (
                    r_a
                    and config.response_looks_like_buyee_waf_challenge(r_a)
                    and (config.BUYEE_COOKIE or "").strip()
                    and config.BUYEE_SEND_COOKIE
                ):
                    print(
                        "A2) Thử lại /detail không gửi Cookie (giống Invoke-WebRequest không -Headers cookie):\n"
                    )
                    h_a2 = config.buyee_page_headers(item_referer, send_cookie=False)
                    r_a2 = _session_get_safe(
                        sess,
                        detail_url,
                        headers=h_a2,
                        proxies=proxies,
                        timeout=timeout,
                        step="A2 /detail no Cookie",
                    )
                    if r_a2 and _analyze("A2", r_a2):
                        try:
                            with open("debug_proxy_detail.html", "w", encoding="utf-8") as f:
                                f.write(r_a2.text)
                            print("📁 debug_proxy_detail.html\n")
                        except OSError:
                            pass
                        print("=== Xong (đặt BUYEE_SEND_COOKIE=0 trong .env để pipeline giống vậy) ===")
                        return
                    if r_a2:
                        try:
                            with open("debug_proxy_detail_A2.html", "w", encoding="utf-8") as f:
                                f.write(r_a2.text)
                        except OSError:
                            pass
                        print("   📁 Đã ghi debug_proxy_detail_A2.html\n")

                print("B) Hai bước giống scraper Yahoo: GET trang item (cookie) → GET /detail:\n")
                item_url = item_referer
                if "lang=en" not in item_url:
                    item_url = item_url + ("&lang=en" if "?" in item_url else "?lang=en")
                h_item = config.buyee_page_headers(referer=config.REFERERS["yahoo"])
                r_item = _session_get_safe(
                    sess,
                    item_url,
                    headers=h_item,
                    proxies=proxies,
                    timeout=timeout,
                    step="B item",
                )
                if not r_item:
                    print("=== Hết ===")
                    return
                print(f"   Item → HTTP {r_item.status_code} | {len(r_item.content)} bytes")
                if config.response_looks_like_buyee_waf_challenge(r_item):
                    print("   🚨 Trang item đã bị WAF — luồng B không cứu được; cần cookie / proxy sạch hơn.")
                    print("=== Hết ===")
                    return

                h_detail = config.buyee_page_headers(referer=item_referer.split("?", 1)[0])
                r_b = _session_get_safe(
                    sess,
                    detail_url,
                    headers=h_detail,
                    proxies=proxies,
                    timeout=timeout,
                    step="B /detail",
                )
                if r_b and _analyze("B", r_b):
                    try:
                        with open("debug_proxy_detail.html", "w", encoding="utf-8") as f:
                            f.write(r_b.text)
                    except OSError:
                        pass
                    print("📁 debug_proxy_detail.html\n")
                elif r_b:
                    try:
                        with open("debug_proxy_detail_B.html", "w", encoding="utf-8") as f:
                            f.write(r_b.text)
                    except OSError:
                        pass
                    print("   📁 Đã ghi debug_proxy_detail_B.html")

        finally:
            if os.getenv("BUYEE_DEBUG_TRY_REQUESTS", "").strip().lower() in (
                "1",
                "true",
                "yes",
            ):
                print(
                    "C) So sánh (BUYEE_DEBUG_TRY_REQUESTS): cùng proxy với requests + header tối giản:\n"
                )
                with requests.Session() as rs:
                    rs.trust_env = False
                    h_c = config.buyee_bare_headers_like_iwr()
                    r_c = _session_get_safe(
                        rs,
                        detail_url,
                        headers=h_c,
                        proxies=proxies,
                        timeout=_debug_buyee_timeout()
                        if not config.prefer_curl_cffi_for_buyee()
                        else (30.0, 90.0),
                        step="C requests bare",
                    )
                    if r_c:
                        _analyze("C", r_c)
                        try:
                            with open(
                                "debug_proxy_detail_C.html", "w", encoding="utf-8"
                            ) as f:
                                f.write(r_c.text)
                        except OSError:
                            pass
                        print("   📁 Đã ghi debug_proxy_detail_C.html\n")

    except requests.RequestException as e:
        print(f"❌ Lỗi mạng qua proxy: {e}")
        return

    print("=== Xong ===")


def debug_with_proxy(url):
    print(f"🔗 Đang tải URL: {url}")

    # 1. Ép ngôn ngữ
    if "lang=en" not in url:
        url += "&lang=en" if "?" in url else "?lang=en"

    # Tận dụng Header cực tốt từ config của bạn
    headers = config.buyee_page_headers(referer="https://buyee.jp/")

    # 2. Dùng Session và Proxy từ Config
    with requests.Session() as session:
        print("⏳ Đang fetch qua Proxy Xoay...")
        resp = config.fetch_with_session(session, url, headers)

        if resp is None:
            print("❌ Không nhận được response! Có thể Proxy lỗi hoặc Timeout.")
            return

        print(f"📊 HTTP Status Code: {resp.status_code}")

        # LƯU HTML TRANG GỐC
        with open("debug_page_goc.html", "w", encoding="utf-8") as f:
            f.write(resp.text)
        print("📁 Đã lưu file HTML gốc -> 'debug_page_goc.html'")

        # Kiểm tra WAF (Anti-Bot)
        if config.response_looks_like_buyee_waf_challenge(resp):
            print("🚨 CẢNH BÁO: Dính WAF Captcha của Buyee! Mở file HTML ra sẽ thấy code JS/Thách thức.")
            print("💡 Hướng giải quyết: Đảm bảo BUYEE_COOKIE trong .env còn sống, hoặc Proxy Xoay cần IP sạch hơn.")
            return

        print("\n✅ Vượt qua WAF thành công! Phân tích DOM...")
        soup = BeautifulSoup(resp.text, "html.parser")

        # TÌM IFRAME (Dành cho Mercari / Yahoo)
        iframe = soup.find("iframe")
        if iframe:
            # Lấy src hoặc data-src
            iframe_src = iframe.get("data-src") or iframe.get("src", "")
            iframe_src = iframe_src.split("#")[0]

            if iframe_src:
                iframe_url = urljoin("https://buyee.jp", iframe_src)
                print(f"🔗 Tìm thấy link Iframe: {iframe_url}")

                # Fetch Iframe (Bắt buộc dùng referer là URL hiện tại)
                iframe_headers = config.buyee_page_headers(referer=url)
                print("⏳ Đang fetch Iframe qua Proxy...")
                iframe_resp = config.fetch_with_session(session, iframe_url, iframe_headers)

                if iframe_resp:
                    with open("debug_iframe.html", "w", encoding="utf-8") as f:
                        f.write(iframe_resp.text)
                    print("📁 Đã lưu HTML của Iframe -> 'debug_iframe.html'")

                    if config.response_looks_like_buyee_waf_challenge(iframe_resp):
                        print("🚨 Iframe bị WAF chặn! Mặc dù trang ngoài vào được.")
                    else:
                        i_soup = BeautifulSoup(iframe_resp.text, "html.parser")
                        text = (
                            i_soup.body.get_text(separator="\n", strip=True)
                            if i_soup.body
                            else i_soup.get_text(separator="\n", strip=True)
                        )
                        print(f"\n✅ TRÍCH XUẤT THÀNH CÔNG (100 ký tự đầu):\n{text[:100]}...")
                else:
                    print("❌ Lỗi khi tải Iframe.")
        else:
            print("ℹ️ Không tìm thấy Iframe nào (Chuyện bình thường nếu bạn đang test Rakuma).")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].strip().lower() in (
        "proxy",
        "xoay",
        "check-proxy",
        "check",
    ):
        check_proxy_xoay_like_powershell()
    else:
        debug_with_proxy(TEST_URL)
