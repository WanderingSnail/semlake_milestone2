"""Format checks on the built paper: page budget, embedded fonts, vector figures.

    python3 benchmarks/check_pdf.py paper/semlake_pvldb.pdf

Uses poppler's pdfinfo, pdffonts, pdfimages and pdftotext. Exit status 1 on a failed check.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

BODY_PAGES = 12  # PVLDB research papers: 12 pages of body, references on top


def _run(*cmd: str) -> str:
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def main() -> None:
    pdf = Path(sys.argv[1] if len(sys.argv) > 1 else "paper/semlake_pvldb.pdf")
    failures = []

    pages = int(next(line.split()[1] for line in _run("pdfinfo", str(pdf)).splitlines() if line.startswith("Pages:")))
    first_ref, body_before = None, False
    for page in range(1, pages + 1):
        text = _run("pdftotext", "-f", str(page), "-l", str(page), str(pdf), "-")
        if "REFERENCES" in text:
            first_ref = page
            # Body text above the heading on this page counts as body on this page.
            body_before = bool(text.split("REFERENCES")[0].strip().strip("0123456789").strip())
            break
    body_end = None if first_ref is None else first_ref if body_before else first_ref - 1
    print(f"pages: {pages}; body ends on page {body_end}; references start on page {first_ref}")
    if body_end is None or body_end > BODY_PAGES:
        failures.append(f"the body runs past page {BODY_PAGES}")
    if "??" in _run("pdftotext", str(pdf), "-"):
        failures.append("the text contains ?? (an undefined number or reference)")

    fonts = _run("pdffonts", str(pdf)).splitlines()[2:]
    not_embedded = [line.split()[0] for line in fonts if line.split()[-5] != "yes"]
    bitmap = [line.split()[0] for line in fonts if "Type 3" in line]
    print(f"fonts: {len(fonts)}, not embedded: {len(not_embedded)}, bitmap (Type 3): {len(bitmap)}")
    if not_embedded:
        failures.append(f"fonts not embedded: {not_embedded}")
    if bitmap:
        failures.append(f"bitmap fonts: {bitmap}")

    images = _run("pdfimages", "-list", str(pdf)).splitlines()[2:]
    print(f"raster images: {len(images)}")
    if images:
        failures.append(f"{len(images)} raster image(s) in the PDF; every figure must be vector")

    for failure in failures:
        print(f"FAILED: {failure}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
