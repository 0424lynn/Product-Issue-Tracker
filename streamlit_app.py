# -*- coding: utf-8 -*-
"""
Product Issue Tracker (Standalone) - SharePoint Images Version
- Google Sheet as DB
- SharePoint Document Library folder for images (auto ensure folder)
- IssueID: ISS-YYYYMMDD-0001

✅ Settings page supports: Add / Rename / Delete (Product Category, Issue Type, Severity, Model)
✅ Optional rename behavior: sync updates to related data (models / issues fields)
✅ Fix 429: fewer reads + exponential backoff for 429 + bootstrap once + partial cache refresh
✅ Add "Status" field: Open / Pending Implementation / Done
✅ Add "Edit Issue" page: editable fields including status
✅ Image storage: SharePoint (auto discovers drive_id; no need to set SP_DRIVE_ID)
"""

import re
import io
import json
import time
from datetime import datetime, date
from typing import Optional, List, Dict

import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

import requests

# =========================
# Settings
# =========================
SPREADSHEET_ID = st.secrets["GSHEET_SPREADSHEET_ID"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

TAB_ISSUES = "issues"
TAB_CATS = "product_categories"
TAB_TYPES = "issue_types"
TAB_SEV = "severities"
TAB_MODELS = "models"
TAB_CFG = "app_config"

STATUS_OPTIONS = ["Open", "Pending Implementation", "Done"]

ISSUE_HEADERS = [
    "IssueID",
    "ProductCategory",
    "Model",
    "IssueName",
    "Severity",
    "IssueType",
    "Description",
    "TempFix",
    "ImprovePlan",
    "Status",
    "CreatedAt",
    "ImplementDate",
    "ImageLinks",
    "UpdatedAt",
]
# === Issue Progress Updates (log table) ===
TAB_UPDATES = "issue_updates"

UPDATES_HEADERS = [
    "IssueID",
    "UpdateAt",
    "Status",
    "Note",
    "NextStep",
    "UpdatedBy",
]

MS_TENANT_ID = st.secrets["MS_TENANT_ID"]
MS_CLIENT_ID = st.secrets["MS_CLIENT_ID"]
MS_CLIENT_SECRET = st.secrets["MS_CLIENT_SECRET"]

SP_SITE_ID = st.secrets.get("SP_SITE_ID", "").strip()
SP_BASE_FOLDER = st.secrets.get("SP_BASE_FOLDER", "Product-Issue-Images").strip()
SP_LINK_SCOPE = st.secrets.get("SP_LINK_SCOPE", "organization").strip()  # organization recommended


# =========================
# Small versioning (local refresh)
# =========================
def bump_ver(key: str):
    st.session_state[key] = int(st.session_state.get(key, 0)) + 1

def ver(key: str) -> int:
    return int(st.session_state.get(key, 0))

def invalidate_cache():
    try:
        st.cache_data.clear()
    except Exception:
        pass


# =========================
# Retry helpers (GSpread + Graph)
# =========================
def _retry_gspread(fn, *, tries=5, base_sleep=0.7):
    last = None
    for i in range(tries):
        try:
            return fn()
        except APIError as e:
            last = e
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                time.sleep(base_sleep * (2 ** i))
                continue
            raise
    raise last

def _first_url(s: str) -> str:
    if s is None:
        return ""
    text = str(s).strip()
    if not text:
        return ""
    parts = [p.strip() for p in text.split(";") if p.strip()]
    for p in parts:
        if p.lower().startswith(("http://", "https://")):
            return p
    return ""

def _preview_text(s: str, n: int = 80) -> str:
    t = str(s or "").strip().replace("\n", " ")
    return (t[:n] + "…") if len(t) > n else t

def _retry_http(fn, *, tries=6, base_sleep=0.8):
    """
    Graph / HTTP 429 / 503 exponential backoff retry
    fn() should return requests.Response
    """
    last_exc = None
    for i in range(tries):
        try:
            r = fn()
            if r.status_code in (429, 503, 504):
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        time.sleep(float(ra))
                        continue
                    except Exception:
                        pass
                time.sleep(base_sleep * (2 ** i))
                continue
            return r
        except Exception as e:
            last_exc = e
            time.sleep(base_sleep * (2 ** i))
            continue
    if last_exc:
        raise last_exc
    raise RuntimeError("HTTP retry failed")


# =========================
# Google Sheet clients
# =========================
@st.cache_resource
def get_gs_creds():
    raw = st.secrets["GCP_SERVICE_ACCOUNT_JSON"]
    info = json.loads(raw) if isinstance(raw, str) else dict(raw)
    return Credentials.from_service_account_info(info, scopes=SCOPES)

@st.cache_resource
def gs_client():
    return gspread.authorize(get_gs_creds())

@st.cache_resource
def gsheet():
    return gs_client().open_by_key(SPREADSHEET_ID)

@st.cache_resource
def ws_cache():
    return {}

def get_or_create_ws(name: str, rows=5000, cols=50):
    cache = ws_cache()
    if name in cache:
        return cache[name]
    sh = gsheet()

    def _get():
        return sh.worksheet(name)

    try:
        ws = _retry_gspread(_get)
    except gspread.WorksheetNotFound:
        def _add():
            return sh.add_worksheet(title=name, rows=rows, cols=cols)
        ws = _retry_gspread(_add)

    cache[name] = ws
    return ws

def ensure_headers(tab: str, headers: List[str]):
    ws = get_or_create_ws(tab)
    first = _retry_gspread(lambda: ws.row_values(1))
    if not first or all(str(x).strip() == "" for x in first):
        _retry_gspread(lambda: ws.update("A1", [headers]))
        return

    missing = [h for h in headers if h not in first]
    if missing:
        new_header = first + missing
        _retry_gspread(lambda: ws.update("A1", [new_header]))

    now_header = _retry_gspread(lambda: ws.row_values(1))
    if now_header[:len(headers)] != headers:
        st.warning(
            f"⚠️ The header row in '{tab}' does not fully match the expected schema "
            f"(missing columns were appended). If you need strict alignment, please manually reorder headers."
        )

@st.cache_data(ttl=120)
def load_df(tab: str, _v: int = 0) -> pd.DataFrame:
    ws = get_or_create_ws(tab)
    recs = _retry_gspread(ws.get_all_records)
    return pd.DataFrame(recs) if recs else pd.DataFrame()

def append_row(tab: str, headers: List[str], row: dict):
    ws = get_or_create_ws(tab)
    ensure_headers(tab, headers)
    header_now = _retry_gspread(lambda: ws.row_values(1))
    _retry_gspread(lambda: ws.append_row([row.get(h, "") for h in header_now]))
    invalidate_cache()

def kv_get(key: str) -> Optional[str]:
    df = load_df(TAB_CFG, ver("v_cfg"))
    if df.empty:
        return None
    m = df[df["Key"].astype(str).str.strip() == key]
    if m.empty:
        return None
    return str(m.iloc[0]["Value"]).strip()

def kv_set(key: str, value: str):
    ws = get_or_create_ws(TAB_CFG)
    ensure_headers(TAB_CFG, ["Key", "Value"])
    rows = _retry_gspread(ws.get_all_records)
    for i, r in enumerate(rows, start=2):
        if str(r.get("Key", "")).strip() == key:
            _retry_gspread(lambda: ws.update(f"B{i}", [[value]]))
            bump_ver("v_cfg")
            return
    _retry_gspread(lambda: ws.append_row([key, value]))
    bump_ver("v_cfg")


# =========================
# Sheet helpers for edit/delete
# =========================
@st.cache_data(ttl=120)
def load_df_with_row(tab: str, _v: int = 0) -> pd.DataFrame:
    ws = get_or_create_ws(tab)
    vals = _retry_gspread(ws.get_all_values)
    if not vals or len(vals) < 2:
        return pd.DataFrame()
    headers = vals[0]
    data = vals[1:]
    df = pd.DataFrame(data, columns=headers)
    df["_row"] = list(range(2, 2 + len(data)))
    return df

def ws_col_index(ws, col_name: str) -> Optional[int]:
    headers = _retry_gspread(lambda: ws.row_values(1))
    try:
        return headers.index(col_name) + 1
    except ValueError:
        return None

def update_cell_by_row(tab: str, row_num: int, col_name: str, value: str):
    ws = get_or_create_ws(tab)
    ci = ws_col_index(ws, col_name)
    if not ci:
        raise ValueError(f"Column not found: {col_name}")
    _retry_gspread(lambda: ws.update_cell(row_num, ci, value))
    invalidate_cache()

def delete_row_by_rownum(tab: str, row_num: int):
    ws = get_or_create_ws(tab)
    _retry_gspread(lambda: ws.delete_rows(row_num))
    invalidate_cache()

def replace_value_in_column(tab: str, col_name: str, old: str, new: str) -> int:
    ws = get_or_create_ws(tab)
    ci = ws_col_index(ws, col_name)
    if not ci:
        return 0

    vals = _retry_gspread(ws.get_all_values)
    if not vals or len(vals) < 2:
        return 0

    to_update = []
    for r in range(2, len(vals) + 1):
        row_vals = vals[r - 1]
        v = row_vals[ci - 1] if ci - 1 < len(row_vals) else ""
        if str(v).strip() == str(old).strip():
            to_update.append((r, ci))

    if not to_update:
        return 0
    cells = [gspread.cell.Cell(row=r, col=c, value=new) for (r, c) in to_update]
    _retry_gspread(lambda: ws.update_cells(cells))
    invalidate_cache()
    return len(to_update)
def delete_updates_by_issueid(issue_id: str) -> int:
    """Delete all rows in TAB_UPDATES for this IssueID. Return count."""
    dfu_rows = load_df_with_row(TAB_UPDATES, ver("v_updates"))
    if dfu_rows.empty or "IssueID" not in dfu_rows.columns:
        return 0
    hit = dfu_rows[dfu_rows["IssueID"].astype(str).str.strip() == str(issue_id).strip()]
    if hit.empty:
        return 0

    # ✅ 从大到小删，避免行号位移
    rows = sorted(hit["_row"].astype(int).tolist(), reverse=True)
    for r in rows:
        delete_row_by_rownum(TAB_UPDATES, int(r))
    return len(rows)

# =========================
# Issue update helpers (progress / timeline)
# =========================

@st.cache_data(ttl=120)
def load_updates(_v: int = 0) -> pd.DataFrame:
    ensure_headers(TAB_UPDATES, UPDATES_HEADERS)
    dfu = load_df(TAB_UPDATES, _v)
    if dfu.empty:
        return dfu
    dfu["UpdateAt_dt"] = pd.to_datetime(dfu.get("UpdateAt", ""), errors="coerce")
    return dfu


def latest_update_map(dfu: pd.DataFrame) -> Dict[str, dict]:
    """
    return:
    {
        IssueID: {
            "LastUpdateAt": "...",
            "LastStatus": "...",
            "LastNote": "...",
            "LastNextStep": "..."
        }
    }
    """
    if dfu is None or dfu.empty:
        return {}

    dfu2 = dfu.copy()
    dfu2["IssueID"] = dfu2["IssueID"].astype(str).str.strip()
    dfu2 = dfu2.sort_values("UpdateAt_dt", ascending=False)

    out = {}
    for _, r in dfu2.iterrows():
        iid = str(r.get("IssueID", "")).strip()
        if not iid or iid in out:
            continue

        out[iid] = {
            "LastUpdateAt": str(r.get("UpdateAt", "")).strip(),
            "LastStatus": str(r.get("Status", "")).strip(),
            "LastNote": str(r.get("Note", "")).strip(),
            "LastNextStep": str(r.get("NextStep", "")).strip(),
        }
    return out

# =========================
# Graph / SharePoint helpers
# =========================
@st.cache_resource
def _graph_token() -> str:
    """Client Credentials: get Graph access token"""
    url = f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": MS_CLIENT_ID,
        "client_secret": MS_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }

    def _do():
        return requests.post(url, data=data, timeout=30)

    r = _retry_http(_do)
    if r.status_code != 200:
        raise RuntimeError(f"Graph token failed: {r.status_code} {r.text}")
    return r.json()["access_token"]

def _graph_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {_graph_token()}"}

@st.cache_resource
def sp_site_id() -> str:
    """
    Prefer SP_SITE_ID (most stable).
    You can extend to auto-discover site_id via hostname/sitePath if needed.
    """
    v = (kv_get("SP_SITE_ID") or SP_SITE_ID or "").strip()
    if not v:
        raise RuntimeError("Missing SP_SITE_ID. Please set SP_SITE_ID in secrets.toml or app_config.")
    return v

@st.cache_resource
def sp_drive_id_auto() -> str:
    """Auto-discover drive_id (Shared Documents / Documents)"""
    sid = sp_site_id()
    url = f"https://graph.microsoft.com/v1.0/sites/{sid}/drives"

    def _do():
        return requests.get(url, headers=_graph_headers(), timeout=30)

    r = _retry_http(_do)
    if r.status_code != 200:
        raise RuntimeError(f"List drives failed: {r.status_code} {r.text}")

    drives = r.json().get("value", [])

    for d in drives:
        web = str(d.get("webUrl", "") or "")
        if "Shared%20Documents" in web or "Shared Documents" in web:
            return d["id"]

    for d in drives:
        nm = str(d.get("name", "") or "").lower()
        if nm in ("documents", "shared documents"):
            return d["id"]

    if drives:
        return drives[0]["id"]

    raise RuntimeError("No usable drive found. Please check Graph permissions.")

def _sp_item_by_path(drive_id: str, path: str) -> Optional[dict]:
    """Get item by path (returns None if not found)"""
    path = path.strip().lstrip("/")
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{path}"

    def _do():
        return requests.get(url, headers=_graph_headers(), timeout=30)

    r = _retry_http(_do)
    if r.status_code == 200:
        return r.json()
    if r.status_code == 404:
        return None
    raise RuntimeError(f"Get item by path failed: {r.status_code} {r.text}")

def _sp_create_folder(drive_id: str, parent_item_id: str, folder_name: str) -> dict:
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{parent_item_id}/children"
    body = {"name": folder_name, "folder": {}, "@microsoft.graph.conflictBehavior": "fail"}

    def _do():
        return requests.post(
            url,
            headers={**_graph_headers(), "Content-Type": "application/json"},
            json=body,
            timeout=30,
        )

    r = _retry_http(_do)
    if r.status_code in (200, 201):
        return r.json()

    if r.status_code == 409:
        item = _sp_item_by_path(drive_id, folder_name)
        if item:
            return item

    raise RuntimeError(f"Create folder failed: {r.status_code} {r.text}")

@st.cache_resource
def sp_ensure_base_folder() -> dict:
    """
    Ensure SP_BASE_FOLDER exists at drive root.
    Returns the folder item (with id).
    """
    drive_id = sp_drive_id_auto()
    root_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root"

    def _root():
        return requests.get(root_url, headers=_graph_headers(), timeout=30)

    rr = _retry_http(_root)
    rr.raise_for_status()
    root = rr.json()
    root_id = root["id"]

    existing = _sp_item_by_path(drive_id, SP_BASE_FOLDER)
    if existing:
        return existing

    return _sp_create_folder(drive_id, root_id, SP_BASE_FOLDER)

def sp_upload_file_to_base_folder(file) -> str:
    """
    Upload a file to SharePoint base folder and return a sharable link (createLink).
    """
    drive_id = sp_drive_id_auto()
    _ = sp_ensure_base_folder()
    base_path = SP_BASE_FOLDER.strip().strip("/")

    safe_name = re.sub(r"[\\/:*?\"<>|]+", "_", str(file.name))

    upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{base_path}/{safe_name}:/content"
    data = file.getvalue()

    def _do_put():
        return requests.put(upload_url, headers=_graph_headers(), data=data, timeout=60)

    r = _retry_http(_do_put)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Upload failed: {r.status_code} {r.text}")

    item = r.json()
    item_id = item["id"]

    link_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/createLink"
    body = {"type": "view", "scope": SP_LINK_SCOPE}

    def _do_link():
        return requests.post(
            link_url,
            headers={**_graph_headers(), "Content-Type": "application/json"},
            json=body,
            timeout=30,
        )

    lr = _retry_http(_do_link)
    if lr.status_code not in (200, 201):
        return item.get("webUrl", "")

    link = lr.json().get("link", {}).get("webUrl", "")
    return link or item.get("webUrl", "")


# =========================
# IssueID
# =========================
def next_issue_id(df_issues: pd.DataFrame) -> str:
    ds = datetime.now().strftime("%Y%m%d")
    prefix = f"ISS-{ds}-"
    if df_issues is None or df_issues.empty or "IssueID" not in df_issues.columns:
        return f"{prefix}0001"
    today = [x for x in df_issues["IssueID"].astype(str).tolist() if x.startswith(prefix)]
    if not today:
        return f"{prefix}0001"
    mx = 0
    for x in today:
        m = re.match(rf"^ISS-{ds}-(\d+)$", x)
        if m:
            mx = max(mx, int(m.group(1)))
    return f"{prefix}{mx+1:04d}"

def _parse_date_safe(s: str) -> Optional[date]:
    try:
        if not s:
            return None
        s2 = str(s).strip()
        if not s2:
            return None
        return pd.to_datetime(s2, errors="coerce").date()
    except Exception:
        return None


# =========================
# Bootstrap tabs & defaults
# =========================
def bootstrap():
    ensure_headers(TAB_ISSUES, ISSUE_HEADERS)
    ensure_headers(TAB_CATS, ["Category"])
    ensure_headers(TAB_TYPES, ["Type"])
    ensure_headers(TAB_SEV, ["Severity"])
    ensure_headers(TAB_MODELS, ["Model", "Category"])
    ensure_headers(TAB_CFG, ["Key", "Value"])
    ensure_headers(TAB_UPDATES, UPDATES_HEADERS)
    # Optional default seeds (edit as needed)
    if load_df(TAB_CATS, ver("v_cats")).empty:
        ws = get_or_create_ws(TAB_CATS)
        for x in ["Undercounter", "Reach-in", "Display Case", "Hot Equip - Electric", "Hot Equip - Gas"]:
            _retry_gspread(lambda x=x: ws.append_row([x]))
        bump_ver("v_cats")

    if load_df(TAB_TYPES, ver("v_types")).empty:
        ws = get_or_create_ws(TAB_TYPES)
        for x in ["Design Issue", "Structural Issue", "Shipping/Damage"]:
            _retry_gspread(lambda x=x: ws.append_row([x]))
        bump_ver("v_types")

    if load_df(TAB_SEV, ver("v_sev")).empty:
        ws = get_or_create_ws(TAB_SEV)
        for x in ["Critical", "High", "Medium", "Low"]:
            _retry_gspread(lambda x=x: ws.append_row([x]))
        bump_ver("v_sev")

    _ = sp_ensure_base_folder()


# =========================
# UI Pages
# =========================
def page_config():
    st.set_page_config(page_title="Product Issue Tracker", layout="wide")
    st.title("🧩 Product Issue Tracker")
    st.caption("Data in Google Sheets, images in SharePoint; IssueID auto-generated: ISS-YYYYMMDD-0001")


def tab_settings():
    st.subheader("⚙️ Settings")

    if st.button("🔄 Force Refresh (Reload from Sheet)", key="btn_force_refresh"):
        invalidate_cache()
        st.toast("Cache cleared. Reloaded from Google Sheets.")
        st.rerun()

    try:
        did = sp_drive_id_auto()
        st.info(f"✅ SharePoint image location: Drive(auto)={did[:10]}...  Folder={SP_BASE_FOLDER}")
    except Exception as e:
        st.error(f"SharePoint configuration/permission error: {e}")

    sync_update = st.checkbox("Sync related data when renaming (recommended)", value=True)

    def _flush(*keys):
        for k in keys:
            bump_ver(k)
        st.rerun()

    c1, c2 = st.columns(2)

    # Product Categories
    with c1:
        st.markdown("### Product Categories (Manageable)")
        df = load_df_with_row(TAB_CATS, ver("v_cats"))
        st.dataframe(df.drop(columns=["_row"], errors="ignore"), use_container_width=True, hide_index=True)

        st.markdown("#### ➕ Add New")
        new_cat = st.text_input("New Category", key="new_cat")
        if st.button("Add Category", key="btn_add_cat"):
            if new_cat.strip():
                get_or_create_ws(TAB_CATS).append_row([new_cat.strip()])
                st.success("Added.")
                _flush("v_cats", "v_models", "v_issues")

        st.markdown("#### ✏️ Edit / 🗑️ Delete")
        if not df.empty:
            cat_list = df["Category"].astype(str).tolist()
            pick = st.selectbox("Select a category to edit", cat_list, key="pick_cat")
            row_num = int(df[df["Category"].astype(str) == pick].iloc[0]["_row"])
            new_name = st.text_input("New Name", value=pick, key="cat_rename")

            b1, b2 = st.columns(2)
            with b1:
                if st.button("✅ Save Changes", key="btn_cat_save"):
                    if not new_name.strip():
                        st.error("New name cannot be empty."); st.stop()
                    update_cell_by_row(TAB_CATS, row_num, "Category", new_name.strip())
                    if sync_update and new_name.strip() != pick:
                        replace_value_in_column(TAB_MODELS, "Category", pick, new_name.strip())
                        replace_value_in_column(TAB_ISSUES, "ProductCategory", pick, new_name.strip())
                    st.success("Updated.")
                    _flush("v_cats", "v_models", "v_issues")

            with b2:
                if st.button("🗑️ Delete Category", key="btn_cat_del"):
                    delete_row_by_rownum(TAB_CATS, row_num)
                    st.success("Deleted.")
                    _flush("v_cats", "v_models", "v_issues")

    # Issue Types
    with c2:
        st.markdown("### Issue Types (Manageable)")
        df = load_df_with_row(TAB_TYPES, ver("v_types"))
        st.dataframe(df.drop(columns=["_row"], errors="ignore"), use_container_width=True, hide_index=True)

        st.markdown("#### ➕ Add New")
        new_t = st.text_input("New Issue Type", key="new_type")
        if st.button("Add Issue Type", key="btn_add_type"):
            if new_t.strip():
                get_or_create_ws(TAB_TYPES).append_row([new_t.strip()])
                st.success("Added.")
                _flush("v_types", "v_issues")

        st.markdown("#### ✏️ Edit / 🗑️ Delete")
        if not df.empty:
            type_list = df["Type"].astype(str).tolist()
            pick = st.selectbox("Select an issue type to edit", type_list, key="pick_type")
            row_num = int(df[df["Type"].astype(str) == pick].iloc[0]["_row"])
            new_name = st.text_input("New Name", value=pick, key="type_rename")

            b1, b2 = st.columns(2)
            with b1:
                if st.button("✅ Save Changes", key="btn_type_save"):
                    if not new_name.strip():
                        st.error("New name cannot be empty."); st.stop()
                    update_cell_by_row(TAB_TYPES, row_num, "Type", new_name.strip())
                    if sync_update and new_name.strip() != pick:
                        replace_value_in_column(TAB_ISSUES, "IssueType", pick, new_name.strip())
                    st.success("Updated.")
                    _flush("v_types", "v_issues")

            with b2:
                if st.button("🗑️ Delete Issue Type", key="btn_type_del"):
                    delete_row_by_rownum(TAB_TYPES, row_num)
                    st.success("Deleted.")
                    _flush("v_types", "v_issues")

    st.markdown("---")

    # Severities
    st.markdown("### Severities (Manageable)")
    df = load_df_with_row(TAB_SEV, ver("v_sev"))
    st.dataframe(df.drop(columns=["_row"], errors="ignore"), use_container_width=True, hide_index=True)

    st.markdown("#### ➕ Add New")
    new_s = st.text_input("New Severity", key="new_sev")
    if st.button("Add Severity", key="btn_add_sev"):
        if new_s.strip():
            get_or_create_ws(TAB_SEV).append_row([new_s.strip()])
            st.success("Added.")
            bump_ver("v_sev"); bump_ver("v_issues")
            st.rerun()

    st.markdown("#### ✏️ Edit / 🗑️ Delete")
    if not df.empty:
        sev_list = df["Severity"].astype(str).tolist()
        pick = st.selectbox("Select a severity to edit", sev_list, key="pick_sev")
        row_num = int(df[df["Severity"].astype(str) == pick].iloc[0]["_row"])
        new_name = st.text_input("New Name", value=pick, key="sev_rename")

        b1, b2 = st.columns(2)
        with b1:
            if st.button("✅ Save Changes", key="btn_sev_save"):
                if not new_name.strip():
                    st.error("New name cannot be empty."); st.stop()
                update_cell_by_row(TAB_SEV, row_num, "Severity", new_name.strip())
                if sync_update and new_name.strip() != pick:
                    replace_value_in_column(TAB_ISSUES, "Severity", pick, new_name.strip())
                st.success("Updated.")
                bump_ver("v_sev"); bump_ver("v_issues")
                st.rerun()

        with b2:
            if st.button("🗑️ Delete Severity", key="btn_sev_del"):
                delete_row_by_rownum(TAB_SEV, row_num)
                bump_ver("v_sev"); bump_ver("v_issues")
                st.session_state["toast"] = f"✅ Deleted severity: {pick}"
                st.query_params["tab"] = "settings"
                st.rerun()

    st.markdown("---")

    # Models
    st.markdown("### Models (Bind a model to a category)")
    dfm = load_df_with_row(TAB_MODELS, ver("v_models"))
    st.dataframe(dfm.drop(columns=["_row"], errors="ignore"), use_container_width=True, hide_index=True)

    cat_opts = load_df(TAB_CATS, ver("v_cats"))
    cat_list = cat_opts["Category"].astype(str).tolist() if not cat_opts.empty else []

    st.markdown("#### ➕ Add New Model")
    m1, m2 = st.columns([1.3, 1.0])
    with m1:
        new_model = st.text_input("New Model", key="new_model")
    with m2:
        model_cat = st.selectbox("Category", [""] + cat_list, key="model_cat")

    if st.button("Add Model", key="btn_add_model"):
        if new_model.strip() and model_cat.strip():
            get_or_create_ws(TAB_MODELS).append_row([new_model.strip(), model_cat.strip()])
            st.success("Added.")
            bump_ver("v_models"); bump_ver("v_issues")
            st.rerun()

    st.markdown("#### ✏️ Edit / 🗑️ Delete Model")
    if not dfm.empty:
        model_list = dfm["Model"].astype(str).tolist()
        pick = st.selectbox("Select a model to edit", model_list, key="pick_model")
        row_sel = dfm[dfm["Model"].astype(str) == pick].iloc[0]
        row_num = int(row_sel["_row"])
        old_cat = str(row_sel.get("Category", "")).strip()

        e1, e2 = st.columns([1.2, 1.0])
        with e1:
            new_model_name = st.text_input("New Model Name", value=pick, key="model_rename")
        with e2:
            new_model_cat = st.selectbox(
                "New Category",
                [""] + cat_list,
                index=([""] + cat_list).index(old_cat) if old_cat in cat_list else 0,
                key="model_cat_rename"
            )

        b1, b2 = st.columns(2)
        with b1:
            if st.button("✅ Save Changes", key="btn_model_save"):
                if not new_model_name.strip():
                    st.error("Model name cannot be empty."); st.stop()
                if not new_model_cat.strip():
                    st.error("Category cannot be empty."); st.stop()

                update_cell_by_row(TAB_MODELS, row_num, "Model", new_model_name.strip())
                update_cell_by_row(TAB_MODELS, row_num, "Category", new_model_cat.strip())

                if sync_update:
                    if new_model_name.strip() != pick:
                        replace_value_in_column(TAB_ISSUES, "Model", pick, new_model_name.strip())
                    if new_model_cat.strip() != old_cat and old_cat.strip():
                        replace_value_in_column(TAB_ISSUES, "ProductCategory", old_cat, new_model_cat.strip())

                st.success("Updated.")
                bump_ver("v_models"); bump_ver("v_issues"); bump_ver("v_cats")
                st.rerun()

        with b2:
            if st.button("🗑️ Delete Model", key="btn_model_del"):
                delete_row_by_rownum(TAB_MODELS, row_num)
                st.success("Deleted.")
                bump_ver("v_models"); bump_ver("v_issues")
                st.rerun()


def tab_new():
    st.subheader("➕ New Issue")
    df_issues = load_df(TAB_ISSUES, ver("v_issues"))
    df_models = load_df(TAB_MODELS, ver("v_models"))
    df_cats = load_df(TAB_CATS, ver("v_cats"))
    df_types = load_df(TAB_TYPES, ver("v_types"))
    df_sev = load_df(TAB_SEV, ver("v_sev"))

    issue_id = next_issue_id(df_issues)

    model_list = sorted(df_models["Model"].astype(str).tolist()) if not df_models.empty else []
    cat_list = sorted(df_cats["Category"].astype(str).tolist()) if not df_cats.empty else []
    type_list = sorted(df_types["Type"].astype(str).tolist()) if not df_types.empty else []
    sev_list = sorted(df_sev["Severity"].astype(str).tolist()) if not df_sev.empty else []

    c1, c2, c3 = st.columns([1.2, 1.0, 1.0])
    with c1:
        st.text_input("IssueID", value=issue_id, disabled=True)
    with c2:
        model = st.selectbox("Model", [""] + model_list)
    with c3:
        auto_cat = ""
        if model and not df_models.empty:
            m = df_models[df_models["Model"].astype(str) == model]
            if not m.empty:
                auto_cat = str(m.iloc[0]["Category"]).strip()
        idx = ([""] + cat_list).index(auto_cat) if auto_cat in cat_list else 0
        category = st.selectbox("Product Category", [""] + cat_list, index=idx)

    issue_name = st.text_input("Issue Name")

    c4, c5, c6 = st.columns(3)
    with c4:
        severity = st.selectbox("Severity", [""] + sev_list)
    with c5:
        issue_type = st.selectbox("Issue Type", [""] + type_list)
    with c6:
        status = st.selectbox("Status", STATUS_OPTIONS, index=0)

    desc = st.text_area("Description", height=120)
    temp_fix = st.text_area("Temporary Fix", height=100)
    improve = st.text_area("Improvement Plan", height=120)

    d1, d2 = st.columns(2)
    with d1:
        created = st.date_input("Created Date", value=date.today())
    with d2:
        implement = st.date_input("Implementation Date (Optional)", value=None)

    imgs = st.file_uploader(
        "Upload Images (Multiple Allowed)",
        type=["png", "jpg", "jpeg", "webp"],
        accept_multiple_files=True,
    )

    if st.button("✅ Save", key="btn_save_issue"):
        if not model.strip():
            st.error("Please select a model first (add models under Settings)."); st.stop()
        if not category.strip():
            st.error("Please select a product category (or bind category to the model first)."); st.stop()
        if not issue_name.strip():
            st.error("Please enter an issue name."); st.stop()

        links = []
        if imgs:
            with st.spinner("Uploading images to SharePoint..."):
                for f in imgs:
                    try:
                        links.append(sp_upload_file_to_base_folder(f))
                    except Exception as e:
                        st.warning(f"Upload failed for {f.name}: {e}")

        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "IssueID": issue_id,
            "ProductCategory": category,
            "Model": model,
            "IssueName": issue_name.strip(),
            "Severity": severity,
            "IssueType": issue_type,
            "Description": desc.strip(),
            "TempFix": temp_fix.strip(),
            "ImprovePlan": improve.strip(),
            "Status": status,
            "CreatedAt": str(created),
            "ImplementDate": str(implement) if implement else "",
            "ImageLinks": ";".join([x for x in links if x]),
            "UpdatedAt": now_ts,
        }
        append_row(TAB_ISSUES, ISSUE_HEADERS, row)
        bump_ver("v_issues")
        st.success(f"✅ Saved: {issue_id}")
        st.rerun()

def _get_issue_row(df: pd.DataFrame, issue_id: str) -> Optional[dict]:
    if df is None or df.empty or "IssueID" not in df.columns:
        return None
    m = df[df["IssueID"].astype(str).str.strip() == str(issue_id).strip()]
    if m.empty:
        return None
    return m.iloc[0].to_dict()

def _get_issue_updates(dfu: pd.DataFrame, issue_id: str) -> pd.DataFrame:
    if dfu is None or dfu.empty or "IssueID" not in dfu.columns:
        return pd.DataFrame()
    hist = dfu[dfu["IssueID"].astype(str).str.strip() == str(issue_id).strip()].copy()
    if hist.empty:
        return hist
    hist["UpdateAt_dt"] = pd.to_datetime(hist.get("UpdateAt", ""), errors="coerce")
    hist = hist.sort_values("UpdateAt_dt", ascending=False)
    return hist

def show_issue_detail_panel(issue_id: str, df_issues: pd.DataFrame, dfu: pd.DataFrame):
    r = _get_issue_row(df_issues, issue_id)
    if not r:
        st.warning("IssueID not found.")
        return
    st.markdown("---")
    st.markdown("### ⚠️ Danger Zone")

    # 找到 issues 的 row_num
    df_rows = load_df_with_row(TAB_ISSUES, ver("v_issues"))
    hit = df_rows[df_rows["IssueID"].astype(str).str.strip() == str(issue_id).strip()]
    row_num = int(hit.iloc[0]["_row"]) if (not hit.empty and "_row" in hit.columns) else None

    del_updates_too = st.checkbox("Also delete ALL progress updates for this issue", value=True, key=f"chk_del_allupd_{issue_id}")
    confirm_del = st.checkbox("I confirm I want to delete this issue", value=False, key=f"chk_del_issue_{issue_id}")

    if st.button("🗑️ Delete This Issue", key=f"btn_del_issue_{issue_id}"):
        if not row_num:
            st.error("Cannot locate row number for this issue.")
            st.stop()
        if not confirm_del:
            st.warning("Please check confirmation box first.")
            st.stop()

        if del_updates_too:
            delete_updates_by_issueid(issue_id)
            bump_ver("v_updates")

        delete_row_by_rownum(TAB_ISSUES, int(row_num))
        bump_ver("v_issues")

        # ✅ 关闭弹窗，避免删完还弹
        st.session_state["__open_issue_detail__"] = ""
        st.session_state["__selected_issueid__"] = ""
        st.success("Issue deleted.")
        st.rerun()

    st.markdown(f"## {r.get('IssueID','')}: {r.get('IssueName','')}")
    c1, c2, c3 = st.columns([1.1, 1.1, 1.0])
    with c1:
        st.write(f"**Category**: {r.get('ProductCategory','')}")
        st.write(f"**Model**: {r.get('Model','')}")
        st.write(f"**Severity**: {r.get('Severity','')}")
    with c2:
        st.write(f"**Issue Type**: {r.get('IssueType','')}")
        st.write(f"**Status**: {r.get('Status','')}")
        st.write(f"**Created**: {r.get('CreatedAt','')}")
    with c3:
        st.write(f"**Implement Date**: {r.get('ImplementDate','')}")
        st.write(f"**Updated At**: {r.get('UpdatedAt','')}")

    st.markdown("### Description")
    st.write(r.get("Description", ""))

    st.markdown("### Temporary Fix")
    st.write(r.get("TempFix", ""))

    st.markdown("### Improvement Plan")
    st.write(r.get("ImprovePlan", ""))

    # attachments
    links = str(r.get("ImageLinks", "") or "").strip()
    st.markdown("### Image / Attachment Links")
    if links:
        for lk in [x.strip() for x in links.split(";") if x.strip()]:
            st.markdown(f"- [Open]({lk})")
    else:
        st.caption("None")

    # progress history
    st.markdown("---")
    st.markdown("### Progress History (All Updates)")
    hist = _get_issue_updates(dfu, issue_id)
    if hist.empty:
        st.caption("No progress updates yet.")
    else:
        # ✅ 让每条 update 带上 row_num，支持删除
        dfu_rows = load_df_with_row(TAB_UPDATES, ver("v_updates"))
        dfu_rows["IssueID"] = dfu_rows["IssueID"].astype(str).str.strip()
        hit_rows = dfu_rows[dfu_rows["IssueID"] == str(issue_id).strip()].copy()

        # 用 UpdateAt + Note 等做一个“尽量稳定”的匹配，拿到 _row
        #（更稳的做法是 issue_updates 增加 UpdateID，但你现在没做 schema 改动，我先用轻量方式）
        hit_rows["UpdateAt"] = hit_rows.get("UpdateAt", "").astype(str).str.strip()
        hit_rows["Status"] = hit_rows.get("Status", "").astype(str).str.strip()
        hit_rows["Note"] = hit_rows.get("Note", "").astype(str).str.strip()
        hit_rows["NextStep"] = hit_rows.get("NextStep", "").astype(str).str.strip()
        hit_rows["UpdatedBy"] = hit_rows.get("UpdatedBy", "").astype(str).str.strip()

        show_hist_cols = [c for c in ["UpdateAt","Status","Note","NextStep","UpdatedBy"] if c in hit_rows.columns]
        show_df = hit_rows[["_row"] + show_hist_cols].copy()
        show_df = show_df.sort_values("UpdateAt", ascending=False).reset_index(drop=True)

        evt_u = st.dataframe(
            show_df[show_hist_cols],  # 不展示 _row，但我们用 selection index 去取
            use_container_width=True,
            hide_index=True,
            height=300,
            on_select="rerun",
            selection_mode="single-row",
            key=f"upd_table_{issue_id}",
        )

        sel_u = (evt_u.selection.rows or [])
        if sel_u:
            sel_idx = int(sel_u[0])
            st.session_state["__selected_update_row__"] = int(show_df.iloc[sel_idx]["_row"])

        d1, d2, d3 = st.columns([1.2, 1.2, 2.0])
        with d1:
            if st.button("🗑️ Delete Selected Update", key=f"btn_del_upd_{issue_id}"):
                row_to_del = st.session_state.get("__selected_update_row__", None)
                if not row_to_del:
                    st.warning("Please select an update row first.")
                else:
                    st.session_state[f"__confirm_del_upd_{issue_id}"] = True
                    st.rerun()

        with d2:
            if st.session_state.get(f"__confirm_del_upd_{issue_id}", False):
                ok = st.checkbox("Confirm delete this update", key=f"chk_del_upd_{issue_id}")
                if st.button("✅ Confirm Delete Update", key=f"btn_confirm_del_upd_{issue_id}"):
                    if not ok:
                        st.warning("Please check confirmation box first.")
                    else:
                        delete_row_by_rownum(TAB_UPDATES, int(st.session_state["__selected_update_row__"]))
                        bump_ver("v_updates")
                        st.session_state["__selected_update_row__"] = None
                        st.session_state[f"__confirm_del_upd_{issue_id}"] = False
                        st.success("Deleted update.")
                        st.rerun()

        with d3:
            st.caption("Tip: Select a row above, then delete it.")

        with st.expander("Timeline view", expanded=False):
            # 用 hist（dfu）展示阅读友好视图
            for _, rr in hist.iterrows():
                st.markdown(f"- **{rr.get('UpdateAt','')}** | **{rr.get('Status','')}** | {rr.get('Note','')}")
                ns = str(rr.get("NextStep","") or "").strip()
                if ns:
                    st.caption(f"Next: {ns}")

def tab_list():
    st.subheader("📋 Search / List")
    df = load_df(TAB_ISSUES, ver("v_issues"))
    if df.empty:
        st.info("No records yet. Please create one under 'New Issue'.")
        return

    df_cats = load_df(TAB_CATS, ver("v_cats"))
    df_models = load_df(TAB_MODELS, ver("v_models"))
    df_types = load_df(TAB_TYPES, ver("v_types"))
    df_sev = load_df(TAB_SEV, ver("v_sev"))

    cat_list = sorted(df_cats["Category"].astype(str).tolist()) if not df_cats.empty else []
    model_list = sorted(df_models["Model"].astype(str).tolist()) if not df_models.empty else []
    type_list = sorted(df_types["Type"].astype(str).tolist()) if not df_types.empty else []
    sev_list = sorted(df_sev["Severity"].astype(str).tolist()) if not df_sev.empty else []
    status_list = STATUS_OPTIONS

    f1, f2, f3, f4, f5 = st.columns(5)
    with f1:
        cat_sel = st.selectbox("Product Category", ["(All)"] + cat_list)
    with f2:
        model_sel = st.selectbox("Model", ["(All)"] + model_list)
    with f3:
        sev_sel = st.selectbox("Severity", ["(All)"] + sev_list)
    with f4:
        type_sel = st.selectbox("Issue Type", ["(All)"] + type_list)
    with f5:
        status_sel = st.selectbox("Status", ["(All)"] + status_list)

    q = st.text_input("Keyword (Name / Description / Fix / Plan)")

    view = df.copy()
    if cat_sel != "(All)":
        view = view[view.get("ProductCategory", "").astype(str) == cat_sel]
    if model_sel != "(All)":
        view = view[view.get("Model", "").astype(str) == model_sel]
    if sev_sel != "(All)":
        view = view[view.get("Severity", "").astype(str) == sev_sel]
    if type_sel != "(All)":
        view = view[view.get("IssueType", "").astype(str) == type_sel]
    if status_sel != "(All)":
        view = view[view.get("Status", "").astype(str) == status_sel]

    if q.strip():
        qq = q.strip().lower()
        blob = (
            view.get("IssueName", "").astype(str) + " " +
            view.get("Description", "").astype(str) + " " +
            view.get("TempFix", "").astype(str) + " " +
            view.get("ImprovePlan", "").astype(str)
        ).str.lower()
        view = view[blob.str.contains(re.escape(qq), na=False)]

    view["_dt"] = pd.to_datetime(view.get("CreatedAt", ""), errors="coerce")
    view = view.sort_values("_dt", ascending=False).drop(columns=["_dt"], errors="ignore")

    # --- Make ImageLinks clickable in table (but don't show raw ImageLinks column) ---
    view_show = view.copy()
    view_show["ImageLink"] = view_show.get("ImageLinks", "").apply(_first_url)
    view_show.loc[view_show["ImageLink"].astype(str).str.strip().eq(""), "ImageLink"] = None
    view_show["DescriptionPreview"] = view_show.get("Description", "").apply(lambda x: _preview_text(x, 80))
    # ===== Merge latest updates (progress tracking) =====
    dfu = load_updates(ver("v_updates"))
    lu = latest_update_map(dfu)

    view_show["LastUpdateAt"] = view_show["IssueID"].astype(str).map(
        lambda x: lu.get(str(x).strip(), {}).get("LastUpdateAt", "")
    )

    view_show["LastNotePreview"] = view_show["IssueID"].astype(str).map(
        lambda x: _preview_text(
            lu.get(str(x).strip(), {}).get("LastNote", ""), 60
        )
    )

    view_show["LastNextStepPreview"] = view_show["IssueID"].astype(str).map(
        lambda x: _preview_text(
            lu.get(str(x).strip(), {}).get("LastNextStep", ""), 60
        )
    )

    show_cols = [
        "IssueID","ProductCategory","Model","IssueName",
        "DescriptionPreview",
        "Severity","IssueType","Status",
        "LastUpdateAt",
        "LastNotePreview",
        "LastNextStepPreview",
        "CreatedAt","ImplementDate","UpdatedAt",
        "ImageLink",
    ]

    show_cols = [c for c in show_cols if c in view_show.columns]

    # ✅ 增加“打开详情”图标列（点击 🔎 直接弹窗）
    view_show = view_show.reset_index(drop=True)
    # ✅ 用“点行”触发弹窗（不需要 Open 列，不改 URL）
    evt = st.dataframe(
        view_show[show_cols],          # 注意：这里用 show_cols，不要 show_cols2
        use_container_width=True,
        hide_index=True,
        height=520,
        on_select="rerun",
        selection_mode="single-row",
        key="issues_table",
        column_config={
            "DescriptionPreview": st.column_config.TextColumn(
                "Description (Preview)",
                help="Short preview of Description.",
            ),
            "LastUpdateAt": st.column_config.TextColumn("Last Update"),
            "LastNotePreview": st.column_config.TextColumn("Latest Note"),
            "LastNextStepPreview": st.column_config.TextColumn("Next Step"),
            "ImageLink": st.column_config.LinkColumn(
                "Image Link",
                display_text="Open",
                help="Open SharePoint image/attachment (first link).",
            ),
        },
    )

    # ✅ 用户点某一行：只记录“选中”，不自动弹窗
    sel_rows = (evt.selection.rows or [])
    if sel_rows:
        iid = str(view_show.iloc[int(sel_rows[0])]["IssueID"]).strip()
        if iid:
            st.session_state["__selected_issueid__"] = iid

    # ✅ 必须点按钮才弹窗
    open_col1, open_col2 = st.columns([1, 3])
    with open_col1:
        if st.button("🔎 Open Selected", key="btn_open_selected"):
            iid = str(st.session_state.get("__selected_issueid__", "")).strip()
            if not iid:
                st.warning("Please select a row first.")
            else:
                st.session_state["__open_issue_detail__"] = iid
                st.session_state["__open_issue_once__"] = True   # ✅ 只允许打开一次
                st.rerun()


    with open_col2:
        sel_show = str(st.session_state.get("__selected_issueid__", "")).strip()
        if sel_show:
            st.caption(f"Selected: {sel_show}")

    # 只加载一次 updates
    dfu = load_updates(ver("v_updates"))

    issue_to_open = str(st.session_state.get("__open_issue_detail__", "")).strip()
    open_once = bool(st.session_state.get("__open_issue_once__", False))

    # ✅ 只有 open_once=True 才弹窗；弹完立刻复位，避免任何 rerun 再弹
    if issue_to_open and open_once:
        # 先复位：保证后续任何 rerun 都不会再弹
        st.session_state["__open_issue_once__"] = False

        try:
            @st.dialog(f"Issue Detail: {issue_to_open}", width="large")
            def _dlg():
                show_issue_detail_panel(issue_to_open, df, dfu)
                if st.button("Close", type="secondary"):
                    st.session_state["__open_issue_detail__"] = ""
                    st.rerun()
            _dlg()
        except Exception:
            with st.expander(f"Issue Detail (fallback): {issue_to_open}", expanded=True):
                show_issue_detail_panel(issue_to_open, df, dfu)
                if st.button("Close Detail", type="secondary"):
                    st.session_state["__open_issue_detail__"] = ""
                    st.rerun()


    # =========================
    # Quick Update
    # =========================
    st.markdown("### ⚡ Quick Update (Add Progress Log)")


    ids = sorted(df["IssueID"].astype(str).dropna().unique().tolist())
    colu1, colu2, colu3 = st.columns([1.2, 1.0, 1.0])

    with colu1:
        upd_issue = st.selectbox("Select IssueID", [""] + ids, key="quick_upd_issue")
    with colu2:
        upd_status = st.selectbox("New Status", STATUS_OPTIONS, key="quick_upd_status")
    with colu3:
        upd_by = st.text_input("Updated By (optional)", value="", key="quick_upd_by")

    upd_note = st.text_area("Update Note", height=100, key="quick_upd_note")
    upd_next = st.text_input("Next Step (optional)", value="", key="quick_upd_next")

    if st.button("✅ Save Update", key="btn_quick_save_update"):
        if not upd_issue.strip():
            st.error("Please select an IssueID."); st.stop()
        if not upd_note.strip():
            st.error("Please enter an update note."); st.stop()

        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 1) append to issue_updates
        append_row(
            TAB_UPDATES,
            UPDATES_HEADERS,
            {
                "IssueID": upd_issue.strip(),
                "UpdateAt": now_ts,
                "Status": upd_status,
                "Note": upd_note.strip(),
                "NextStep": upd_next.strip(),
                "UpdatedBy": upd_by.strip(),
            }
        )

        # 2) also update main issues table: Status + UpdatedAt (and optionally ImplementDate)
        df_rows = load_df_with_row(TAB_ISSUES, ver("v_issues"))
        hit = df_rows[df_rows["IssueID"].astype(str) == upd_issue.strip()]
        if not hit.empty:
            row_num = int(hit.iloc[0]["_row"])
            update_cell_by_row(TAB_ISSUES, row_num, "Status", upd_status)
            update_cell_by_row(TAB_ISSUES, row_num, "UpdatedAt", now_ts)

        bump_ver("v_updates")
        bump_ver("v_issues")
        st.success("✅ Update saved.")
        st.rerun()


def tab_edit():
    st.subheader("✏️ Edit Issue (Status: Open / Pending Implementation / Done)")

    ensure_headers(TAB_ISSUES, ISSUE_HEADERS)

    df = load_df_with_row(TAB_ISSUES, ver("v_issues"))
    if df.empty:
        st.info("No records.")
        return

    if "IssueID" not in df.columns:
        st.error("Missing 'IssueID' column in issues sheet. Please check headers.")
        return

    df["IssueID"] = df["IssueID"].astype(str)
    ids = [x for x in df["IssueID"].tolist() if str(x).strip()]
    ids = sorted(list(dict.fromkeys(ids)))
    if not ids:
        st.info("No valid IssueID found.")
        return

    pick = st.selectbox("Select IssueID to edit", ids, key="edit_pick_issueid")

    row_sel = df[df["IssueID"].astype(str) == str(pick)].iloc[0]
    row_num = int(row_sel["_row"])
    r = row_sel.to_dict()

    df_models = load_df(TAB_MODELS, ver("v_models"))
    df_cats = load_df(TAB_CATS, ver("v_cats"))
    df_types = load_df(TAB_TYPES, ver("v_types"))
    df_sev = load_df(TAB_SEV, ver("v_sev"))

    model_list = sorted(df_models["Model"].astype(str).tolist()) if (not df_models.empty and "Model" in df_models.columns) else []
    cat_list = sorted(df_cats["Category"].astype(str).tolist()) if (not df_cats.empty and "Category" in df_cats.columns) else []
    type_list = sorted(df_types["Type"].astype(str).tolist()) if (not df_types.empty and "Type" in df_types.columns) else []
    sev_list = sorted(df_sev["Severity"].astype(str).tolist()) if (not df_sev.empty and "Severity" in df_sev.columns) else []

    cur_model = str(r.get("Model", "")).strip()
    cur_cat = str(r.get("ProductCategory", "")).strip()
    cur_type = str(r.get("IssueType", "")).strip()
    cur_sev = str(r.get("Severity", "")).strip()
    cur_status = str(r.get("Status", "")).strip() or "Open"

    cur_created = _parse_date_safe(r.get("CreatedAt", "")) or date.today()
    cur_impl = _parse_date_safe(r.get("ImplementDate", ""))

    st.caption(f"Row number (Sheet): {row_num}")

    kpre = f"edit_{pick}_"

    c1, c2, c3 = st.columns([1.2, 1.0, 1.0])
    with c1:
        st.text_input("IssueID", value=pick, disabled=True, key=kpre + "issueid_show")
    with c2:
        model = st.selectbox(
            "Model",
            [""] + model_list,
            index=([""] + model_list).index(cur_model) if cur_model in model_list else 0,
            key=kpre + "model"
        )
    with c3:
        category = st.selectbox(
            "Product Category",
            [""] + cat_list,
            index=([""] + cat_list).index(cur_cat) if cur_cat in cat_list else 0,
            key=kpre + "category"
        )

    issue_name = st.text_input("Issue Name", value=str(r.get("IssueName", "") or ""), key=kpre + "issuename")

    d1, d2, d3 = st.columns(3)
    with d1:
        severity = st.selectbox(
            "Severity",
            [""] + sev_list,
            index=([""] + sev_list).index(cur_sev) if cur_sev in sev_list else 0,
            key=kpre + "severity"
        )
    with d2:
        issue_type = st.selectbox(
            "Issue Type",
            [""] + type_list,
            index=([""] + type_list).index(cur_type) if cur_type in type_list else 0,
            key=kpre + "issuetype"
        )
    with d3:
        status = st.selectbox(
            "Status",
            STATUS_OPTIONS,
            index=STATUS_OPTIONS.index(cur_status) if cur_status in STATUS_OPTIONS else 0,
            key=kpre + "status"
        )

    desc = st.text_area("Description", value=str(r.get("Description", "") or ""), height=120, key=kpre + "desc")
    temp_fix = st.text_area("Temporary Fix", value=str(r.get("TempFix", "") or ""), height=100, key=kpre + "tempfix")
    improve = st.text_area("Improvement Plan", value=str(r.get("ImprovePlan", "") or ""), height=120, key=kpre + "improve")

    t1, t2 = st.columns(2)
    with t1:
        created = st.date_input("Created Date", value=cur_created, key=kpre + "createdat")
    with t2:
        implement = st.date_input("Implementation Date (Optional)", value=cur_impl, key=kpre + "implement")

    st.markdown("### Image / Attachment Links (Read-only)")
    links = str(r.get("ImageLinks", "") or "").strip()
    if links:
        for lk in [x.strip() for x in links.split(";") if x.strip()]:
            st.markdown(f"- {lk}")
    else:
        st.caption("None")

    c_save, c_del = st.columns([1.0, 1.0])

    with c_save:
        if st.button("✅ Save Changes", key=kpre + "btn_update"):
            if not model.strip():
                st.error("Please select a model."); st.stop()
            if not category.strip():
                st.error("Please select a product category."); st.stop()
            if not issue_name.strip():
                st.error("Issue name cannot be empty."); st.stop()

            now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            update_cell_by_row(TAB_ISSUES, row_num, "ProductCategory", category.strip())
            update_cell_by_row(TAB_ISSUES, row_num, "Model", model.strip())
            update_cell_by_row(TAB_ISSUES, row_num, "IssueName", issue_name.strip())
            update_cell_by_row(TAB_ISSUES, row_num, "Severity", severity)
            update_cell_by_row(TAB_ISSUES, row_num, "IssueType", issue_type)
            update_cell_by_row(TAB_ISSUES, row_num, "Description", desc.strip())
            update_cell_by_row(TAB_ISSUES, row_num, "TempFix", temp_fix.strip())
            update_cell_by_row(TAB_ISSUES, row_num, "ImprovePlan", improve.strip())
            update_cell_by_row(TAB_ISSUES, row_num, "Status", status)
            update_cell_by_row(TAB_ISSUES, row_num, "CreatedAt", str(created))
            update_cell_by_row(TAB_ISSUES, row_num, "ImplementDate", str(implement) if implement else "")
            update_cell_by_row(TAB_ISSUES, row_num, "UpdatedAt", now_ts)

            bump_ver("v_issues")
            st.success("✅ Saved.")
            st.rerun()

    with c_del:
        st.caption("⚠️ Deletion cannot be undone (deletes this row only).")
        confirm = st.checkbox("I confirm I want to delete this issue", value=False, key=kpre + "del_confirm")
        if st.button("🗑️ Delete Issue", key=kpre + "btn_delete"):
            if not confirm:
                st.warning("Please check the confirmation box first."); st.stop()
            delete_row_by_rownum(TAB_ISSUES, row_num)
            bump_ver("v_issues")
            st.success("Deleted.")
            st.rerun()


def main():
    page_config()

    msg = st.session_state.pop("toast", None)
    if msg:
        st.toast(msg)

    if "bootstrapped" not in st.session_state:
        bootstrap()
        st.session_state["bootstrapped"] = True

    qp = st.query_params
    cur = qp.get("tab", "list")
    if cur not in ["list", "new", "edit", "settings"]:
        cur = "list"

    tab = st.radio(
        "Navigation",
        ["list", "new", "edit", "settings"],
        format_func=lambda x: {
            "list": "📋 List",
            "new": "➕ New Issue",
            "edit": "✏️ Edit Issue",
            "settings": "⚙️ Settings",
        }[x],
        index=["list", "new", "edit", "settings"].index(cur),
        horizontal=True,
        key="nav_tab",
    )
    st.query_params["tab"] = tab
    # ✅ 切换页面时，清掉弹窗/选择状态，避免回到 list 自动弹窗
    prev_tab = st.session_state.get("__prev_tab__", "")
    if prev_tab and prev_tab != tab:
        st.session_state["__open_issue_detail__"] = ""
        st.session_state["__selected_issueid__"] = ""
        st.session_state["__selected_update_row__"] = None
    st.session_state["__prev_tab__"] = tab

    if tab == "list":
        tab_list()
    elif tab == "new":
        tab_new()
    elif tab == "edit":
        tab_edit()
    else:
        tab_settings()


if __name__ == "__main__":
    main()
