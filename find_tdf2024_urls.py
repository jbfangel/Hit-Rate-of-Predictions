"""
Find Tour de France 2024 stage prediction URLs by walking forward day by day.

Starting from 2024-06-27 / stage 1:
- If the URL is valid → record it, move to next stage and next day
- If not valid → try the next day for the same stage
- Stops when stage 17 is found or date passes 2024-07-31

Then passes all found URLs to add_urls.py.
"""

import subprocess
import sys
import time
from datetime import date, timedelta

from playwright.sync_api import sync_playwright

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

BASES = [
    "https://sport.tv2.dk/cykling/2024-{date}-axelgaards-optakt-til-{stage}-etape",
    "https://sport.tv2.dk/2024-{date}-axelgaards-optakt-til-{stage}-etape",
]
MAX_STAGE = 6
START_DATE = date(2024, 6, 27)
END_DATE = date(2024, 7, 31)


def is_valid(page, url):
    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=10_000)
        if not response or response.status == 404:
            return False
    except Exception:
        return False
    try:
        title = page.locator("h1").first.inner_text(timeout=2_000).strip()
        return "axelgaards optakt til" in title.lower()
    except Exception:
        return False


def main():
    valid_urls = []
    current_date = START_DATE
    stage = 1

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(user_agent=USER_AGENT, locale="da-DK")
        page = context.new_page()

        while stage <= MAX_STAGE and current_date <= END_DATE:
            d = current_date.strftime("%m-%d")
            found_url = None
            for base in BASES:
                url = base.format(date=d, stage=stage)
                print(f"  Trying {current_date} stage {stage} ({url}): ", end="", flush=True)
                if is_valid(page, url):
                    found_url = url
                    break
                print("✗")

            if found_url:
                print("✓")
                valid_urls.append(found_url)
                stage += 1
                current_date += timedelta(days=1)
            else:
                current_date += timedelta(days=1)

            time.sleep(0.3)

        browser.close()

    if not valid_urls:
        print("No valid URLs found.")
        return

    print(f"\nFound {len(valid_urls)} articles. Passing to add_urls.py...")
    subprocess.run([sys.executable, "add_urls.py"] + valid_urls)


if __name__ == "__main__":
    main()
