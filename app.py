import os
import re
import pandas as pd

import dash
from dash import Dash, html, dcc, callback, Input, Output, State
from dash.dash_table import DataTable
import dash_bootstrap_components as dbc

# Import user utilities
from analytics_utils import db_utils

# -------------- Helpers --------------
def parse_program_ids(text: str):
    if not text:
        return []
    # split on comma/space and keep digits
    parts = re.split(r"[\s,]+", text.strip())
    ids = []
    for p in parts:
        if not p:
            continue
        try:
            ids.append(int(p))
        except ValueError:
            pass
    return ids

# -------------- App ------------------
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

app.layout = dbc.Container(
    [
        html.H2("Engagement Explorer (Basic Dash App)"),
        html.P("Enter one or more program IDs (comma or space separated), choose a campaign type, then click Run."),

        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Label("Program IDs"),
                        dbc.Input(id="program-ids", placeholder="e.g., 9365, 9364, 9086", type="text"),
                    ],
                    md=6,
                ),
                dbc.Col(
                    [
                        dbc.Label("Campaign Type"),
                        dcc.Dropdown(
                            id="campaign-type",
                            options=[
                                {"label": "Custom (email + assets + surveys + choozle)", "value": "custom"},
                                {"label": "Turnkey (email + AdButler)", "value": "turnkey"},
                            ],
                            value="custom",
                            clearable=False,
                        ),
                    ],
                    md=6,
                ),
            ],
            className="mb-3",
        ),

        dbc.Row(
            [
                dbc.Col(
                    dbc.Button("Run", id="run-btn", color="primary"),
                    width="auto",
                ),
                dbc.Col(
                    dbc.Button("Test Postgres", id="test-pg", color="secondary", outline=True, className="ms-2"),
                    width="auto",
                ),
                dbc.Col(
                    dbc.Button("Test MySQL (via SSH)", id="test-mysql", color="secondary", outline=True, className="ms-2"),
                    width="auto",
                ),
                dbc.Col(
                    html.Div(id="conn-status", className="ms-3 mt-2"),
                    width=True,
                ),
            ],
            className="mb-3",
        ),

        html.Div(id="result-summary"),
        dcc.Download(id="download-data"),

        DataTable(
            id="engagement-table",
            page_size=15,
            style_table={"overflowX": "auto"},
            style_cell={"minWidth": 120, "whiteSpace": "normal", "height": "auto"},
        ),

        dbc.Row(
            [
                dbc.Col(
                    dbc.Button("Download CSV", id="download-btn", color="success", className="mt-2"),
                    width="auto",
                )
            ]
        ),

        html.Hr(),
        html.P("Tip: Update credentials in analytics_utils/config.py. Functions come from db_utils.py."),
    ],
    fluid=True,
    className="p-4",
)

# -------------- Callbacks --------------
@callback(
    Output("conn-status", "children"),
    Input("test-pg", "n_clicks"),
    Input("test-mysql", "n_clicks"),
    prevent_initial_call=True,
)
def test_connections(pg_clicks, my_clicks):
    ctx = dash.callback_context  # type: ignore
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate  # type: ignore
    which = ctx.triggered[0]["prop_id"].split(".")[0]

    if which == "test-pg":
        ok = db_utils.test_postgres_connection()
        return "✅ PostgreSQL OK" if ok else "❌ PostgreSQL failed. See server logs."
    else:
        ok = db_utils.test_mysql_connection()
        return "✅ MySQL OK" if ok else "❌ MySQL failed. See server logs."


@callback(
    Output("engagement-table", "data"),
    Output("engagement-table", "columns"),
    Output("result-summary", "children"),
    Input("run-btn", "n_clicks"),
    State("program-ids", "value"),
    State("campaign-type", "value"),
    prevent_initial_call=True,
)
def run_query(n_clicks, ids_text, campaign_type):
    program_ids = parse_program_ids(ids_text or "")
    if not program_ids:
        return [], [], "Provide at least one valid program ID."

    df = db_utils.run_combined_engagement_query(program_ids, campaign_type=campaign_type)
    if df is None or df.empty:
        return [], [], f"No results for {program_ids}."

    cols = [{"name": c, "id": c} for c in df.columns]
    summary = f"Returned {len(df):,} rows across {len(df.columns)} columns for programs: {program_ids}."
    # cache in a simple global for download (demo-friendly)
    app.server.df_cache = df  # type: ignore
    return df.to_dict("records"), cols, summary


@callback(
    Output("download-data", "data"),
    Input("download-btn", "n_clicks"),
    prevent_initial_call=True,
)
def download_csv(n_clicks):
    df = getattr(app.server, "df_cache", None)  # type: ignore
    if df is None or df.empty:
        return dash.no_update  # type: ignore
    return dcc.send_data_frame(df.to_csv, "engagements.csv", index=False)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", 8050)), debug=True)