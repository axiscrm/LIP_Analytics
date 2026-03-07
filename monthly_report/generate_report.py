#!/usr/bin/env python3
"""
generate_report.py
Main entry point for adviser monthly report generation.

Usage:
    python3 generate_report.py --user_id 80 --month 2 --year 2026
    python3 generate_report.py --user_id 80 --month 2 --year 2026 --output /path/to/output.pdf

Currently: data is hardcoded for user_id=80 / Feb 2026 in report_config.py.
Next step: replace with live DB queries that populate report_config.py dynamically.
"""
import argparse
import os
import sys
from PyPDF2 import PdfMerger

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")


def build_report(user_id: int, month: int, year: int, output_path: str = None):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    import report_config as cfg
    print(f"Generating report for {cfg.ADVISER_NAME} ({cfg.PRACTICE_NAME})")
    print(f"Period: {cfg.REPORT_MONTH_NAME} {cfg.REPORT_YEAR}")
    print(f"User ID: {user_id}\n")

    section_pdfs = []
    builders = [
        ("Section 1:  Executive Summary",        "test_section1",  "draw_section1",  "section1_sample.pdf"),
        ("Section 2:  12-Month Table",            "test_section2",  "draw_section2",  "section2_sample.pdf"),
        ("Section 3:  Licensee Benchmarking",     "test_section3",  "draw_section3",  "section3_sample.pdf"),
        ("Section 4:  Referral Partners",         "test_section4",  "draw_section4",  "section4_sample.pdf"),
        ("Section 5/6: Insurer + Submissions",    "test_section5",  "draw_section5",  "section5_sample.pdf"),
        ("Section 7:  Speed-to-Contact",          "test_section7",  "draw_section7",  "section7_sample.pdf"),
        ("Section 8:  Completion Forecast",       "test_section8",  "draw_section8",  "section8_sample.pdf"),
        ("Section 9:  Quoted Pipeline",           "test_section9",  "draw_section9",  "section9_sample.pdf"),
        ("Section 10: What Works",                "test_section10", "draw_section10", "section10_sample.pdf"),
        ("Section 11: Strongest Predictor",       "test_section11", "draw_section11", "section11_sample.pdf"),
        ("Section 12: Summary + Milestone",       "test_section12", "draw_section12", "section12_sample.pdf"),
        ("Section 13: Glossary + Appendix",        "test_section13", "draw_section13", "section13_sample.pdf"),
    ]

    for label, module_name, func_name, filename in builders:
        print(f"  Building {label}...")
        mod = __import__(module_name)
        fn = getattr(mod, func_name)
        out = os.path.join(OUTPUT_DIR, filename)
        fn(out)
        section_pdfs.append(out)

    print("\n  Merging sections...")
    merger = PdfMerger()
    for pdf in section_pdfs:
        if os.path.exists(pdf):
            merger.append(pdf)

    if output_path is None:
        safe_name = cfg.ADVISER_NAME.lower().replace(" ", "_")
        month_name = cfg.REPORT_MONTH_NAME.lower()
        output_path = os.path.join(
            OUTPUT_DIR,
            f"adviser_report_{safe_name}_{month_name}{cfg.REPORT_YEAR}.pdf"
        )

    merger.write(output_path)
    merger.close()

    size_kb = os.path.getsize(output_path) / 1024
    print(f"\n✅ Report generated: {output_path} ({size_kb:.0f} KB)")
    print(f"   Adviser: {cfg.ADVISER_NAME} ({cfg.PRACTICE_NAME})")
    print(f"   Period:  {cfg.REPORT_MONTH_NAME} {cfg.REPORT_YEAR}")
    print(f"   Pages:   {cfg.TOTAL_PAGES}")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate adviser monthly report")
    parser.add_argument("--user_id", type=int, default=80)
    parser.add_argument("--month", type=int, default=2)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()
    build_report(args.user_id, args.month, args.year, args.output)
