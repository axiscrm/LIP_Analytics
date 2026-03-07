"""
chart_builder.py — Generates all report charts as base64-encoded PNGs.
Each function returns a data URI string ready for <img src="...">.
"""
import io, base64
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

NAVY = "#1B2A4A"
GREY = "#808080"
BODY = "#333333"
GREEN = "#548235"
GOLD = "#D4A843"


def _to_base64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    buf.seek(0)
    return "data:image/png;base64," + base64.b64encode(buf.read()).decode()


# ── 1. 12-Month Trend (bar + line combo) ──────────────────
def trend_chart(months, premiums, app_counts, avg_line):
    fig, ax1 = plt.subplots(figsize=(8, 3.8))
    x = np.arange(len(months))
    bars = ax1.bar(x, [p / 1000 for p in premiums], color="#2E75B6", alpha=0.85, zorder=3)
    ax1.set_ylabel("Submitted Premium ($K)", fontsize=9, color=NAVY)
    ax1.set_xticks(x)
    ax1.set_xticklabels(months, fontsize=8, rotation=0)
    ax1.set_ylim(0, max(premiums) / 1000 * 1.3)
    ax1.axhline(y=avg_line / 1000, color="#C0504D", linestyle="--", linewidth=1, alpha=0.7, label=f"12m avg: ${avg_line/1000:.0f}K")
    for spine in ["top", "right"]: ax1.spines[spine].set_visible(False)
    ax1.grid(axis="y", alpha=0.15)

    # Value labels on bars
    for bar, val in zip(bars, premiums):
        if val == max(premiums):
            ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                     f"${val/1000:.0f}K\n★", ha="center", va="bottom", fontsize=8, fontweight="bold", color=GOLD)
        elif val >= avg_line:
            ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                     f"${val/1000:.0f}K", ha="center", va="bottom", fontsize=7, color=NAVY)

    ax2 = ax1.twinx()
    ax2.plot(x, app_counts, color=GOLD, linewidth=2.5, marker="o", markersize=6, markerfacecolor=GOLD, zorder=5)
    ax2.set_ylabel("Applications", fontsize=9, color=GOLD)
    ax2.set_ylim(0, max(app_counts) * 1.3)
    ax2.spines["top"].set_visible(False)
    ax1.legend(fontsize=7, loc="upper left")

    fig.suptitle("12-Month Submitted Premium & Application Volume", fontsize=11, fontweight="bold", color=NAVY, y=1.0)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    return _to_base64(fig)


# ── 2. Benchmarking histogram ─────────────────────────────
def benchmark_chart(bins, counts, adviser_rate, adviser_rank, total, median_rate, avg_rate):
    fig, ax = plt.subplots(figsize=(7, 3.2))
    x = np.arange(len(bins))
    bar_colors = ["#2E75B6"] * len(bins)
    # Find adviser's bin
    for i, b in enumerate(bins):
        lo = int(b.split("-")[0]) if "-" in b else 50
        hi = int(b.split("-")[1]) if "-" in b else 100
        if lo <= adviser_rate < hi or (b == "50+" and adviser_rate >= 50):
            bar_colors[i] = GREEN
    ax.bar(x, counts, color=bar_colors, zorder=3, edgecolor="white", linewidth=0.5)
    ax.axvline(x=bins.index([b for b in bins if "10" in b or "15" in b][0]) if any("10" in b or "15" in b for b in bins) else 2,
               color="#C0504D", linestyle="--", linewidth=1.5, alpha=0.6)
    ax.set_xticks(x); ax.set_xticklabels(bins, fontsize=8)
    ax.set_xlabel("Client Conversion Rate (%)", fontsize=9)
    ax.set_ylabel("Number of Practices", fontsize=9)
    # Annotate adviser position
    for i, c in enumerate(bar_colors):
        if c == GREEN:
            ax.annotate(f"(#{adviser_rank} of {total})", xy=(x[i], counts[i]),
                        xytext=(x[i]+0.5, counts[i]+1), fontsize=8, fontweight="bold",
                        arrowprops=dict(arrowstyle="->", color=GREEN), color=GREEN)
    # Median/avg lines
    ax.set_title(f"Where You Sit - Conversion Rate Across {total} Active Practices",
                 fontsize=10, fontweight="bold", color=NAVY, pad=10)
    for spine in ["top","right"]: ax.spines[spine].set_visible(False)
    ax.grid(axis="y", alpha=0.15); ax.set_axisbelow(True)
    plt.tight_layout()
    return _to_base64(fig)


# ── 3. Referral partner chart ─────────────────────────────
def referral_chart(sources, app_counts, premiums):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 3.5))
    colors_list = ["#2E75B6", "#548235", "#D4A843", "#C0504D", "#8B5E3C", "#7F7F7F", "#5B9BD5", "#A9D18E"]
    y = np.arange(len(sources))
    ax1.barh(y, app_counts, color=colors_list[:len(sources)], height=0.6)
    ax1.set_yticks(y); ax1.set_yticklabels(sources, fontsize=8)
    ax1.invert_yaxis(); ax1.set_title("Applications by Source", fontsize=10, fontweight="bold", color=NAVY)
    for spine in ["top","right"]: ax1.spines[spine].set_visible(False)
    for i, v in enumerate(app_counts):
        ax1.text(v + 0.3, i, str(v), va="center", fontsize=8, fontweight="bold")

    ax2.barh(y, [p/1000 for p in premiums], color=colors_list[:len(sources)], height=0.6)
    ax2.set_yticks(y); ax2.set_yticklabels(sources, fontsize=8)
    ax2.invert_yaxis(); ax2.set_title("Premium by Source ($K)", fontsize=10, fontweight="bold", color=NAVY)
    for spine in ["top","right"]: ax2.spines[spine].set_visible(False)
    for i, v in enumerate(premiums):
        ax2.text(v/1000 + 0.3, i, f"${v/1000:.0f}K", va="center", fontsize=8, fontweight="bold")

    plt.tight_layout()
    return _to_base64(fig)


# ── 4. Insurer donut chart ────────────────────────────────
def insurer_chart(insurers, counts):
    fig, ax = plt.subplots(figsize=(3.5, 2.8))
    colors_list = ["#2E75B6", "#548235", "#D4A843", "#C0504D", "#8B5E3C", "#7F7F7F", "#5B9BD5", "#A9D18E"]
    wedges, texts, autotexts = ax.pie(counts, labels=insurers, autopct="%1.0f%%",
                                       colors=colors_list[:len(insurers)],
                                       pctdistance=0.78, startangle=90,
                                       textprops={"fontsize": 7.5})
    centre = plt.Circle((0, 0), 0.55, fc="white")
    ax.add_artist(centre)
    ax.text(0, 0, f"{len(insurers)}\nInsurers", ha="center", va="center", fontsize=10, fontweight="bold", color=NAVY)
    plt.tight_layout()
    return _to_base64(fig)


# ── 5. Speed-to-contact dual bar ──────────────────────────
def speed_chart(buckets, conv_rates, case_values, total_leads, period):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 3.2))
    fig.subplots_adjust(wspace=0.35)
    x = np.arange(len(buckets)); bw = 0.55
    conv_colors = ["#C0504D", "#2E75B6", "#2E75B6", "#548235"]
    case_colors = ["#8DB4E2", "#5B9BD5", "#2E75B6", "#1B4F8A"]

    ax1.bar(x, conv_rates, width=bw, color=conv_colors, zorder=3); ax1.set_ylim(0, 105)
    ax1.set_xticks(x); ax1.set_xticklabels(buckets, fontsize=8)
    ax1.set_title("Conversion by Call Activity", fontsize=10, fontweight="bold", color=NAVY, pad=8)
    for spine in ["top","right"]: ax1.spines[spine].set_visible(False)
    ax1.grid(axis="y", alpha=0.15)
    for bar, val in zip(ax1.patches, conv_rates):
        ax1.text(bar.get_x()+bar.get_width()/2, bar.get_height()+1.5, f"{val}%", ha="center", va="bottom", fontsize=8, fontweight="bold", color=NAVY)

    ax2.bar(x, case_values, width=bw, color=case_colors, zorder=3); ax2.set_ylim(0, max(case_values)*1.2)
    ax2.set_xticks(x); ax2.set_xticklabels(buckets, fontsize=8)
    ax2.set_title("Case Value by Call Activity", fontsize=10, fontweight="bold", color=NAVY, pad=8)
    for spine in ["top","right"]: ax2.spines[spine].set_visible(False)
    ax2.grid(axis="y", alpha=0.15)
    for bar, val in zip(ax2.patches, case_values):
        ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+15, f"${val:,}", ha="center", va="bottom", fontsize=8, fontweight="bold", color=NAVY)

    fig.suptitle(f"Speed-to-Contact Analysis ({total_leads} leads, {period})", fontsize=10, fontweight="bold", color=NAVY, y=1.0)
    plt.tight_layout(rect=[0, 0, 1, 0.94])
    return _to_base64(fig)


# ── 6. Completion forecast (bar + cumulative line) ────────
def completion_chart(buckets, per_pct, cum_pct, total_completed):
    fig, ax = plt.subplots(figsize=(7, 3.0))
    x = np.arange(len(buckets)); bw = 0.5
    bars = ax.bar(x, per_pct, width=bw, color="#2E75B6", alpha=0.85, zorder=3)
    ax.set_ylim(0, 115); ax.set_xticks(x); ax.set_xticklabels(buckets, fontsize=8)
    for spine in ["top","right"]: ax.spines[spine].set_visible(False)
    ax.grid(axis="y", alpha=0.15)
    for i, (bar, val) in enumerate(zip(bars, per_pct)):
        if i == 0:
            ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()-4, f"{val}%", ha="center", va="top", fontsize=8, fontweight="bold", color="white")
        else:
            ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+1.5, f"{val}%", ha="center", va="bottom", fontsize=8, fontweight="bold", color="#2E75B6")
    ax.plot(x, cum_pct, color=NAVY, linewidth=2.5, marker="o", markersize=6, markerfacecolor=NAVY, zorder=5)
    for i, val in enumerate(cum_pct):
        offset = 5 if i == 0 else 4
        ax.text(x[i], val+offset, f"{val}%", ha="center", va="bottom", fontsize=8, fontweight="bold", color=NAVY)
    ax.set_title(f"Application Completion Timeline ({total_completed} historical apps)", fontsize=10, fontweight="bold", color=NAVY, pad=10)
    plt.tight_layout()
    return _to_base64(fig)


# ── 7. Conversion driver dual chart (call activity + quoted vs unquoted) ──
def conversion_driver_chart(call_buckets, conv_rates, multiplier, unquoted_rate, quoted_rate, quoted_mult):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8.5, 3.5))
    fig.subplots_adjust(wspace=0.35)
    x = np.arange(len(call_buckets)); bw = 0.55
    gradient = ["#C0504D", "#5B9BD5", "#2E75B6", GREEN]
    bars1 = ax1.bar(x, conv_rates, width=bw, color=gradient, zorder=3)
    ax1.set_ylim(0, 105); ax1.set_xticks(x); ax1.set_xticklabels(call_buckets, fontsize=8)
    ax1.set_ylabel("Your Conversion Rate (%)", fontsize=8, color=GREY)
    ax1.set_title("Your Conversion by Call Activity", fontsize=9.5, fontweight="bold", color=NAVY, pad=8)
    for spine in ["top","right"]: ax1.spines[spine].set_visible(False)
    ax1.grid(axis="y", alpha=0.15)
    for bar, val in zip(bars1, conv_rates):
        ax1.text(bar.get_x()+bar.get_width()/2, bar.get_height()+1.5, f"{val}%", ha="center", va="bottom", fontsize=8.5, fontweight="bold", color=NAVY)
    ax1.text(len(call_buckets)/2, max(conv_rates)*0.6, f"{multiplier}x", ha="center", fontsize=20, fontweight="bold", color=NAVY, alpha=0.3)

    # Quoted vs unquoted
    cats = ["Unquoted\nLeads", "Quoted\nLeads"]
    vals = [unquoted_rate, quoted_rate]
    bars2 = ax2.bar([0, 1], vals, width=0.55, color=["#C0504D", GREEN], zorder=3)
    ax2.set_ylim(0, 85); ax2.set_xticks([0, 1]); ax2.set_xticklabels(cats, fontsize=8)
    ax2.set_ylabel("Your Conversion Rate (%)", fontsize=8, color=GREY)
    ax2.set_title("Your Conversion: Quoted vs Unquoted", fontsize=9.5, fontweight="bold", color=NAVY, pad=8)
    for spine in ["top","right"]: ax2.spines[spine].set_visible(False)
    ax2.grid(axis="y", alpha=0.15)
    for bar, val in zip(bars2, vals):
        ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+1.5, f"{val}%", ha="center", va="bottom", fontsize=9, fontweight="bold", color=NAVY)
    ax2.text(0.5, max(vals)*0.5, f"{quoted_mult}x", ha="center", fontsize=20, fontweight="bold", color=NAVY, alpha=0.3)

    fig.suptitle("What Drives Your Results", fontsize=11, fontweight="bold", color=NAVY, y=1.0)
    plt.tight_layout(rect=[0, 0, 1, 0.94])
    return _to_base64(fig)


# ── 8. Pipeline by engagement level (horizontal bar) ──────
def pipeline_engagement_chart(segments):
    """segments: list of (label, leads, rate_str, est_premium_k)"""
    bar_colors = ["#2E7D32", "#5B7BA5", GOLD, "#999999"]
    fig, ax = plt.subplots(figsize=(6.5, 2.5))
    labels = [s[0] for s in segments]
    values = [s[3] for s in segments]
    y_pos = np.arange(len(segments))
    ax.barh(y_pos, values, height=0.55, color=bar_colors[:len(segments)], edgecolor='none')
    ax.set_yticks(y_pos); ax.set_yticklabels(labels, fontsize=8, color=BODY)
    ax.invert_yaxis()
    ax.set_xlabel("Estimated Premium Value ($K)", fontsize=8, color=GREY)
    ax.set_title("Your Pipeline by Engagement Level", fontsize=10, fontweight='bold', color=NAVY, pad=10)
    for i, (_, leads, rate, val) in enumerate(segments):
        ax.text(val + 1.5, i, f"${val}K  ({leads} leads × {rate})", va='center', fontsize=7.5, color=BODY)
    ax.set_xlim(0, max(values) * 1.8)
    for spine in ["top","right"]: ax.spines[spine].set_visible(False)
    ax.spines['left'].set_color('#CCCCCC'); ax.spines['bottom'].set_color('#CCCCCC')
    ax.grid(axis='x', alpha=0.3, linewidth=0.5)
    plt.tight_layout()
    return _to_base64(fig)
