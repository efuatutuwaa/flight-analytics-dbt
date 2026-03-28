"""
Shared theme and styling for Flight Analytics dashboard.
One place to change colors, fonts, and chart defaults.
"""

# ── Color palette ──────────────────────────────────────────────────────────────
PRIMARY     = "#2563EB"   # blue  — main accent
SUCCESS     = "#16A34A"   # green — good performance
WARNING     = "#D97706"   # amber — moderate
DANGER      = "#DC2626"   # red   — poor performance
NEUTRAL     = "#6B7280"   # grey  — secondary info
LIGHT_GREY  = "#F3F4F6"   # chart gridlines

# Sequential scale: low → high (blue only, easy on the eye)
BLUE_SCALE  = ["#DBEAFE", "#93C5FD", "#3B82F6", "#1D4ED8", "#1E3A8A"]

# Diverging scale: good → bad (green → amber → red)
PERF_SCALE  = [SUCCESS, WARNING, DANGER]

# ── Base chart layout ──────────────────────────────────────────────────────────
BASE_LAYOUT = dict(
    font=dict(family="Inter, sans-serif", size=13, color="#111827"),
    plot_bgcolor="white",
    paper_bgcolor="white",
    margin=dict(t=20, b=20, l=10, r=10),
    xaxis=dict(
        showgrid=True,
        gridcolor=LIGHT_GREY,
        gridwidth=1,
        linecolor=LIGHT_GREY,
        tickfont=dict(size=12),
    ),
    yaxis=dict(
        showgrid=True,
        gridcolor=LIGHT_GREY,
        gridwidth=1,
        linecolor=LIGHT_GREY,
        tickfont=dict(size=12),
    ),
    hoverlabel=dict(
        bgcolor="white",
        bordercolor=LIGHT_GREY,
        font_size=13,
    ),
    coloraxis_showscale=False,
)


def apply_theme(fig, height: int = 420):
    """Apply consistent styling to any Plotly figure."""
    fig.update_layout(height=height, **BASE_LAYOUT)
    return fig
