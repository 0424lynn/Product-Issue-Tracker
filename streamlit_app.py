# -*- coding: utf-8 -*-
"""
Product Issue Tracker (Standalone)
- Google Sheet as DB
- Google Drive folder for images (auto create)
- IssueID: ISS-YYYYMMDD-0001

✅ 配置页支持：新增 / 编辑改名 / 删除（产品分类、问题分类、严重程度、型号）
✅ 改名可选：同步更新关联数据（models / issues 表字段）
✅ 修复 429：减少读请求 + 429 退避重试 + bootstrap 只执行一次 + 局部刷新缓存
✅ 新增“状态”字段：未完成 / 待实施 / 已完成
✅ 新增“编辑问题”页面：可编辑并保存状态等字段
"""

import re
import io
import json
import time
from datetime import datetime, date
from typing import Optional, List

import streamlit as st
import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# =========================
# Settings
# =========================
SPREADSHEET_ID = st.secrets["GSHEET_SPREADSHEET_ID"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Tabs
TAB_ISSUES = "issues"
TAB_CATS = "product_categories"
TAB_TYPES = "issue_types"
TAB_SEV = "severities"
TAB_MODELS = "models"
TAB_CFG = "app_config"

DEFAULT_FOLDER_NAME = "1dWCNP0ReJq_20ZWr0GUDzbcgO68ek_9k"

STATUS_OPTIONS = ["未完成", "待实施", "已完成"]

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
    "Status",          # ✅ 新增
    "CreatedAt",
    "ImplementDate",
    "ImageLinks",
    "UpdatedAt",
]

# =========================
# Small versioning (local refresh)
# =========================
def bump_ver(key: str):
    st.session_state[key] = int(st.session_state.get(key, 0)) + 1

def ver(key: str) -> int:
    return int(st.session_state.get(key, 0))

def invalidate_cache():
    # 让下一次 load_df / load_df_with_row 必定重新从 Google Sheet 读取
    try:
        st.cache_data.clear()
    except Exception:
        pass

# =========================
# GSpread retry helper
# =========================
def _retry_gspread(fn, *, tries=5, base_sleep=0.7):
    """
    遇到 429（读配额）时做退避重试，避免直接报错
    """
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

# =========================
# Clients
# =========================
@st.cache_resource
def get_creds():
    raw = st.secrets["GCP_SERVICE_ACCOUNT_JSON"]
    info = json.loads(raw) if isinstance(raw, str) else dict(raw)
    return Credentials.from_service_account_info(info, scopes=SCOPES)

@st.cache_resource
def gs_client():
    return gspread.authorize(get_creds())

@st.cache_resource
def gsheet():
    return gs_client().open_by_key(SPREADSHEET_ID)

@st.cache_resource
def drive():
    return build("drive", "v3", credentials=get_creds())

@st.cache_resource
def ws_cache():
    """
    Worksheet 对象缓存（避免反复 worksheet(name) 触发 metadata 读请求）
    """
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
    """
    兼容升级：如果表头缺少新列（比如 Status），自动补到末尾。
    如果完全不一致（顺序差异/旧字段），会提示 warning，但仍尽量补列，避免程序直接崩。
    """
    ws = get_or_create_ws(tab)

    first = _retry_gspread(lambda: ws.row_values(1))
    if not first or all(str(x).strip() == "" for x in first):
        _retry_gspread(lambda: ws.update("A1", [headers]))
        return

    # 缺哪些列就补到末尾
    missing = [h for h in headers if h not in first]
    if missing:
        new_header = first + missing
        _retry_gspread(lambda: ws.update("A1", [new_header]))

    # 如果顺序/内容不一致，提示
    now_header = _retry_gspread(lambda: ws.row_values(1))
    if now_header[:len(headers)] != headers:
        st.warning(f"⚠️ '{tab}' 表头与预期不完全一致（已尽量自动补列）。如需严格对齐，建议你手动对齐表头顺序。")

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

    invalidate_cache()  # ✅ 关键：写完立刻让读缓存失效

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
    """
    读取 sheet 并带真实行号（_row），用于 update/delete
    """
    ws = get_or_create_ws(tab)
    vals = _retry_gspread(ws.get_all_values)
    if not vals or len(vals) < 2:
        return pd.DataFrame()
    headers = vals[0]
    data = vals[1:]
    df = pd.DataFrame(data, columns=headers)
    df["_row"] = list(range(2, 2 + len(data)))  # 第1行是表头
    return df

def ws_col_index(ws, col_name: str) -> Optional[int]:
    headers = _retry_gspread(lambda: ws.row_values(1))
    try:
        return headers.index(col_name) + 1  # 1-based
    except ValueError:
        return None

def update_cell_by_row(tab: str, row_num: int, col_name: str, value: str):
    ws = get_or_create_ws(tab)
    ci = ws_col_index(ws, col_name)
    if not ci:
        raise ValueError(f"Column not found: {col_name}")
    _retry_gspread(lambda: ws.update_cell(row_num, ci, value))
    invalidate_cache()  # ✅

def delete_row_by_rownum(tab: str, row_num: int):
    ws = get_or_create_ws(tab)
    _retry_gspread(lambda: ws.delete_rows(row_num))
    invalidate_cache()  # ✅

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
    invalidate_cache()  # ✅
    return len(to_update)

# =========================
# Drive folder
# =========================
def find_folder_id(name: str) -> Optional[str]:
    res = drive().files().list(
        q=f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
        fields="files(id,name)"
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def create_folder(name: str) -> str:
    folder = drive().files().create(
        body={"name": name, "mimeType": "application/vnd.google-apps.folder"},
        fields="id"
    ).execute()
    return folder["id"]

def get_or_create_folder() -> str:
    fid = kv_get("GDRIVE_FOLDER_ID")
    if fid:
        return fid
    fid2 = find_folder_id(DEFAULT_FOLDER_NAME)
    if fid2:
        kv_set("GDRIVE_FOLDER_ID", fid2)
        return fid2
    fid3 = create_folder(DEFAULT_FOLDER_NAME)
    kv_set("GDRIVE_FOLDER_ID", fid3)
    return fid3

def upload_image(file):
    folder_id = get_or_create_folder()  # ✅ 关键：确保是有权限的 folder

    content = file.getvalue()
    fh = io.BytesIO(content)

    media = MediaIoBaseUpload(
        fh,
        mimetype=file.type,
        resumable=False
    )

    created = drive().files().create(
        body={
            "name": file.name,
            "parents": [folder_id],  # ✅ 不再用 FIXED_FOLDER_ID
        },
        media_body=media,
        fields="id, webViewLink"
    ).execute()

    # 设置为任何人可查看（只读）
    drive().permissions().create(
        fileId=created["id"],
        body={
            "type": "anyone",
            "role": "reader",
        },
    ).execute()

    return (
        created.get("webViewLink")
        or f"https://drive.google.com/file/d/{created['id']}/view"
    )


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

    if load_df(TAB_CATS, ver("v_cats")).empty:
        ws = get_or_create_ws(TAB_CATS)
        for x in ["矮柜", "高柜", "展示柜", "热设备-电", "热设备-燃气"]:
            _retry_gspread(lambda x=x: ws.append_row([x]))
        bump_ver("v_cats")

    if load_df(TAB_TYPES, ver("v_types")).empty:
        ws = get_or_create_ws(TAB_TYPES)
        for x in ["设计问题", "结构问题", "运输问题"]:
            _retry_gspread(lambda x=x: ws.append_row([x]))
        bump_ver("v_types")

    if load_df(TAB_SEV, ver("v_sev")).empty:
        ws = get_or_create_ws(TAB_SEV)
        for x in ["Critical", "High", "Medium", "Low"]:
            _retry_gspread(lambda x=x: ws.append_row([x]))
        bump_ver("v_sev")

    _ = get_or_create_folder()

# =========================
# UI Pages
# =========================
def page_config():
    st.set_page_config(page_title="产品问题跟踪", layout="wide")
    st.title("🧩 产品问题跟踪（全新系统）")
    st.caption("Google Sheet 存数据，Google Drive 存图片；IssueID 自动生成：ISS-YYYYMMDD-0001")

def tab_settings():
    st.subheader("⚙️ 配置")

    # 🔄 手动强制刷新（解决 Sheet 已删但系统还显示的问题）
    if st.button("🔄 强制刷新（重新从 Sheet 读取）", key="btn_force_refresh"):
        invalidate_cache()
        st.toast("缓存已清空，已重新从 Google Sheet 读取")
        st.rerun()

    folder_id = get_or_create_folder()
    st.info(f"✅ 图片默认文件夹：{DEFAULT_FOLDER_NAME} （folder_id={folder_id}）")

    sync_update = st.checkbox("改名时同步更新关联数据（推荐）", value=True)


    def _flush(*keys):
        for k in keys:
            bump_ver(k)
        st.rerun()

    c1, c2 = st.columns(2)

    # 产品分类
    with c1:
        st.markdown("### 产品分类（可维护）")
        df = load_df_with_row(TAB_CATS, ver("v_cats"))
        st.dataframe(df.drop(columns=["_row"], errors="ignore"), use_container_width=True, hide_index=True)

        st.markdown("#### ➕ 新增")
        new_cat = st.text_input("新增分类", key="new_cat")
        if st.button("添加分类", key="btn_add_cat"):
            if new_cat.strip():
                get_or_create_ws(TAB_CATS).append_row([new_cat.strip()])
                st.success("已添加")
                _flush("v_cats", "v_models", "v_issues")

        st.markdown("#### ✏️ 编辑 / 🗑️ 删除")
        if not df.empty:
            cat_list = df["Category"].astype(str).tolist()
            pick = st.selectbox("选择要编辑的分类", cat_list, key="pick_cat")
            row_num = int(df[df["Category"].astype(str) == pick].iloc[0]["_row"])
            new_name = st.text_input("新名称", value=pick, key="cat_rename")

            b1, b2 = st.columns(2)
            with b1:
                if st.button("✅ 保存修改", key="btn_cat_save"):
                    if not new_name.strip():
                        st.error("新名称不能为空")
                        st.stop()
                    update_cell_by_row(TAB_CATS, row_num, "Category", new_name.strip())

                    if sync_update and new_name.strip() != pick:
                        replace_value_in_column(TAB_MODELS, "Category", pick, new_name.strip())
                        replace_value_in_column(TAB_ISSUES, "ProductCategory", pick, new_name.strip())

                    st.success("已更新")
                    _flush("v_cats", "v_models", "v_issues")

            with b2:
                if st.button("🗑️ 删除该分类", key="btn_cat_del"):
                    delete_row_by_rownum(TAB_CATS, row_num)
                    st.success("已删除")
                    _flush("v_cats", "v_models", "v_issues")

    # 问题分类
    with c2:
        st.markdown("### 问题分类（可维护）")
        df = load_df_with_row(TAB_TYPES, ver("v_types"))
        st.dataframe(df.drop(columns=["_row"], errors="ignore"), use_container_width=True, hide_index=True)

        st.markdown("#### ➕ 新增")
        new_t = st.text_input("新增问题分类", key="new_type")
        if st.button("添加问题分类", key="btn_add_type"):
            if new_t.strip():
                get_or_create_ws(TAB_TYPES).append_row([new_t.strip()])
                st.success("已添加")
                _flush("v_types", "v_issues")

        st.markdown("#### ✏️ 编辑 / 🗑️ 删除")
        if not df.empty:
            type_list = df["Type"].astype(str).tolist()
            pick = st.selectbox("选择要编辑的问题分类", type_list, key="pick_type")
            row_num = int(df[df["Type"].astype(str) == pick].iloc[0]["_row"])
            new_name = st.text_input("新名称", value=pick, key="type_rename")

            b1, b2 = st.columns(2)
            with b1:
                if st.button("✅ 保存修改", key="btn_type_save"):
                    if not new_name.strip():
                        st.error("新名称不能为空")
                        st.stop()
                    update_cell_by_row(TAB_TYPES, row_num, "Type", new_name.strip())

                    if sync_update and new_name.strip() != pick:
                        replace_value_in_column(TAB_ISSUES, "IssueType", pick, new_name.strip())

                    st.success("已更新")
                    _flush("v_types", "v_issues")

            with b2:
                if st.button("🗑️ 删除该问题分类", key="btn_type_del"):
                    delete_row_by_rownum(TAB_TYPES, row_num)
                    st.success("已删除")
                    _flush("v_types", "v_issues")

    st.markdown("---")

    # 严重程度
    st.markdown("### 严重程度（可维护）")
    df = load_df_with_row(TAB_SEV, ver("v_sev"))
    st.dataframe(df.drop(columns=["_row"], errors="ignore"), use_container_width=True, hide_index=True)

    st.markdown("#### ➕ 新增")
    new_s = st.text_input("新增严重程度", key="new_sev")
    if st.button("添加严重程度", key="btn_add_sev"):
        if new_s.strip():
            get_or_create_ws(TAB_SEV).append_row([new_s.strip()])
            st.success("已添加")
            bump_ver("v_sev"); bump_ver("v_issues")
            st.rerun()

    st.markdown("#### ✏️ 编辑 / 🗑️ 删除")
    if not df.empty:
        sev_list = df["Severity"].astype(str).tolist()
        pick = st.selectbox("选择要编辑的严重程度", sev_list, key="pick_sev")
        row_num = int(df[df["Severity"].astype(str) == pick].iloc[0]["_row"])
        new_name = st.text_input("新名称", value=pick, key="sev_rename")

        b1, b2 = st.columns(2)
        with b1:
            if st.button("✅ 保存修改", key="btn_sev_save"):
                if not new_name.strip():
                    st.error("新名称不能为空")
                    st.stop()
                update_cell_by_row(TAB_SEV, row_num, "Severity", new_name.strip())

                if sync_update and new_name.strip() != pick:
                    replace_value_in_column(TAB_ISSUES, "Severity", pick, new_name.strip())

                st.success("已更新")
                bump_ver("v_sev"); bump_ver("v_issues")
                st.rerun()

        with b2:
            if st.button("🗑️ 删除该严重程度", key="btn_sev_del"):
                delete_row_by_rownum(TAB_SEV, row_num)
                bump_ver("v_sev"); bump_ver("v_issues")
                st.session_state["toast"] = f"✅ 已删除严重程度：{pick}"
                st.query_params["tab"] = "settings"
                st.rerun()

    st.markdown("---")

    # 型号管理
    st.markdown("### 型号管理（型号由你绑定分类）")
    dfm = load_df_with_row(TAB_MODELS, ver("v_models"))
    st.dataframe(dfm.drop(columns=["_row"], errors="ignore"), use_container_width=True, hide_index=True)

    cat_opts = load_df(TAB_CATS, ver("v_cats"))
    cat_list = cat_opts["Category"].astype(str).tolist() if not cat_opts.empty else []

    st.markdown("#### ➕ 新增型号")
    m1, m2 = st.columns([1.3, 1.0])
    with m1:
        new_model = st.text_input("新增型号 Model", key="new_model")
    with m2:
        model_cat = st.selectbox("所属分类", [""] + cat_list, key="model_cat")

    if st.button("添加型号", key="btn_add_model"):
        if new_model.strip() and model_cat.strip():
            get_or_create_ws(TAB_MODELS).append_row([new_model.strip(), model_cat.strip()])
            st.success("已添加")
            bump_ver("v_models"); bump_ver("v_issues")
            st.rerun()

    st.markdown("#### ✏️ 编辑 / 🗑️ 删除型号")
    if not dfm.empty:
        model_list = dfm["Model"].astype(str).tolist()
        pick = st.selectbox("选择要编辑的型号", model_list, key="pick_model")
        row_sel = dfm[dfm["Model"].astype(str) == pick].iloc[0]
        row_num = int(row_sel["_row"])
        old_cat = str(row_sel.get("Category", "")).strip()

        e1, e2 = st.columns([1.2, 1.0])
        with e1:
            new_model_name = st.text_input("新型号名称", value=pick, key="model_rename")
        with e2:
            new_model_cat = st.selectbox(
                "新所属分类",
                [""] + cat_list,
                index=([""] + cat_list).index(old_cat) if old_cat in cat_list else 0,
                key="model_cat_rename"
            )

        b1, b2 = st.columns(2)
        with b1:
            if st.button("✅ 保存修改", key="btn_model_save"):
                if not new_model_name.strip():
                    st.error("型号名称不能为空")
                    st.stop()
                if not new_model_cat.strip():
                    st.error("所属分类不能为空")
                    st.stop()

                update_cell_by_row(TAB_MODELS, row_num, "Model", new_model_name.strip())
                update_cell_by_row(TAB_MODELS, row_num, "Category", new_model_cat.strip())

                if sync_update:
                    if new_model_name.strip() != pick:
                        replace_value_in_column(TAB_ISSUES, "Model", pick, new_model_name.strip())
                    if new_model_cat.strip() != old_cat and old_cat.strip():
                        replace_value_in_column(TAB_ISSUES, "ProductCategory", old_cat, new_model_cat.strip())

                st.success("已更新")
                bump_ver("v_models"); bump_ver("v_issues"); bump_ver("v_cats")
                st.rerun()

        with b2:
            if st.button("🗑️ 删除该型号", key="btn_model_del"):
                delete_row_by_rownum(TAB_MODELS, row_num)
                st.success("已删除")
                bump_ver("v_models"); bump_ver("v_issues")
                st.rerun()

def tab_new():
    st.subheader("➕ 新增问题")
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
        model = st.selectbox("型号 Model", [""] + model_list)
    with c3:
        auto_cat = ""
        if model and not df_models.empty:
            m = df_models[df_models["Model"].astype(str) == model]
            if not m.empty:
                auto_cat = str(m.iloc[0]["Category"]).strip()
        idx = ([""] + cat_list).index(auto_cat) if auto_cat in cat_list else 0
        category = st.selectbox("产品分类 Category", [""] + cat_list, index=idx)

    issue_name = st.text_input("问题名称")
    c4, c5, c6 = st.columns(3)
    with c4:
        severity = st.selectbox("严重程度", [""] + sev_list)
    with c5:
        issue_type = st.selectbox("问题分类", [""] + type_list)
    with c6:
        status = st.selectbox("状态", STATUS_OPTIONS, index=0)

    desc = st.text_area("问题描述", height=120)
    temp_fix = st.text_area("临时维修方案", height=100)
    improve = st.text_area("改进方案", height=120)

    d1, d2 = st.columns(2)
    with d1:
        created = st.date_input("录入日期", value=date.today())
    with d2:
        implement = st.date_input("实施日期（可空）", value=None)

    imgs = st.file_uploader("上传图片（可多选）", type=["png","jpg","jpeg","webp"], accept_multiple_files=True)

    if st.button("✅ 保存", key="btn_save_issue"):
        if not model.strip():
            st.error("请先选择型号（先到【配置】里添加型号）")
            st.stop()
        if not category.strip():
            st.error("请先选择产品分类（或先给该型号绑定分类）")
            st.stop()
        if not issue_name.strip():
            st.error("请填写问题名称")
            st.stop()

        folder_id = get_or_create_folder()
        links = []
        if imgs:
            with st.spinner("上传图片到 Google Drive..."):
                for f in imgs:
                    try:
                        links.append(upload_image(f, folder_id))
                    except Exception as e:
                        st.warning(f"图片 {f.name} 上传失败：{e}")

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
            "ImageLinks": ";".join(links),
            "UpdatedAt": now_ts,
        }
        append_row(TAB_ISSUES, ISSUE_HEADERS, row)
        bump_ver("v_issues")
        st.success(f"✅ 已保存：{issue_id}")
        st.rerun()

def tab_list():
    st.subheader("📋 查询 / 列表")
    df = load_df(TAB_ISSUES, ver("v_issues"))
    if df.empty:
        st.info("暂无记录，请先在【新增问题】录入。")
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
        cat_sel = st.selectbox("产品分类", ["(All)"] + cat_list)
    with f2:
        model_sel = st.selectbox("型号", ["(All)"] + model_list)
    with f3:
        sev_sel = st.selectbox("严重程度", ["(All)"] + sev_list)
    with f4:
        type_sel = st.selectbox("问题分类", ["(All)"] + type_list)
    with f5:
        status_sel = st.selectbox("状态", ["(All)"] + status_list)

    q = st.text_input("关键词（名称/描述/方案）")

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

    show_cols = [
        "IssueID","ProductCategory","Model","IssueName",
        "Severity","IssueType","Status",
        "CreatedAt","ImplementDate","UpdatedAt","ImageLinks"
    ]
    show_cols = [c for c in show_cols if c in view.columns]
    st.dataframe(view[show_cols], use_container_width=True, hide_index=True)

    st.markdown("### 🔍 查看单条详情（输入 IssueID）")
    pick = st.text_input("IssueID", key="pick_issueid")
    if pick.strip():
        m = df[df["IssueID"].astype(str) == pick.strip()]
        if m.empty:
            st.warning("没找到该 IssueID")
        else:
            r = m.iloc[0].to_dict()
            st.markdown(f"## {r.get('IssueID','')}：{r.get('IssueName','')}")
            st.write(f"**分类/型号**：{r.get('ProductCategory','')} / {r.get('Model','')}")
            st.write(f"**严重程度**：{r.get('Severity','')} | **问题分类**：{r.get('IssueType','')} | **状态**：{r.get('Status','')}")
            st.write(f"**录入日期**：{r.get('CreatedAt','')} | **实施日期**：{r.get('ImplementDate','')}")

            st.markdown("### 问题描述"); st.write(r.get("Description",""))
            st.markdown("### 临时维修方案"); st.write(r.get("TempFix",""))
            st.markdown("### 改进方案"); st.write(r.get("ImprovePlan",""))

            links = str(r.get("ImageLinks","") or "").strip()
            if links:
                st.markdown("### 图片/附件链接")
                for lk in [x.strip() for x in links.split(";") if x.strip()]:
                    st.markdown(f"- {lk}")

def tab_edit():
    st.subheader("✏️ 编辑问题（含：未完成 / 待实施 / 已完成）")

    ensure_headers(TAB_ISSUES, ISSUE_HEADERS)

    df = load_df_with_row(TAB_ISSUES, ver("v_issues"))
    if df.empty:
        st.info("暂无记录。")
        return

    if "IssueID" not in df.columns:
        st.error("issues 表缺少 IssueID 列，请检查表头。")
        return

    df["IssueID"] = df["IssueID"].astype(str)
    ids = [x for x in df["IssueID"].tolist() if str(x).strip()]
    ids = sorted(list(dict.fromkeys(ids)))
    if not ids:
        st.info("暂无有效 IssueID。")
        return

    # ✅ 给选择 IssueID 的控件也加 key
    pick = st.selectbox("选择要编辑的 IssueID", ids, key="edit_pick_issueid")

    row_sel = df[df["IssueID"].astype(str) == str(pick)].iloc[0]
    row_num = int(row_sel["_row"])
    r = row_sel.to_dict()

    # 选项数据
    df_models = load_df(TAB_MODELS, ver("v_models"))
    df_cats = load_df(TAB_CATS, ver("v_cats"))
    df_types = load_df(TAB_TYPES, ver("v_types"))
    df_sev = load_df(TAB_SEV, ver("v_sev"))

    model_list = sorted(df_models["Model"].astype(str).tolist()) if (not df_models.empty and "Model" in df_models.columns) else []
    cat_list = sorted(df_cats["Category"].astype(str).tolist()) if (not df_cats.empty and "Category" in df_cats.columns) else []
    type_list = sorted(df_types["Type"].astype(str).tolist()) if (not df_types.empty and "Type" in df_types.columns) else []
    sev_list = sorted(df_sev["Severity"].astype(str).tolist()) if (not df_sev.empty and "Severity" in df_sev.columns) else []

    # 当前值
    cur_model = str(r.get("Model", "")).strip()
    cur_cat = str(r.get("ProductCategory", "")).strip()
    cur_type = str(r.get("IssueType", "")).strip()
    cur_sev = str(r.get("Severity", "")).strip()
    cur_status = str(r.get("Status", "")).strip() or "未完成"

    # 日期
    cur_created = _parse_date_safe(r.get("CreatedAt", "")) or date.today()
    cur_impl = _parse_date_safe(r.get("ImplementDate", ""))  # 可空

    st.caption(f"行号（Sheet）：{row_num}")

    # ✅ 用 IssueID 做前缀，确保每次编辑不同单子时 key 也不同（避免缓存/冲突）
    kpre = f"edit_{pick}_"

    c1, c2, c3 = st.columns([1.2, 1.0, 1.0])
    with c1:
        st.text_input("IssueID", value=pick, disabled=True, key=kpre + "issueid_show")
    with c2:
        model = st.selectbox(
            "型号 Model",
            [""] + model_list,
            index=([""] + model_list).index(cur_model) if cur_model in model_list else 0,
            key=kpre + "model"
        )
    with c3:
        category = st.selectbox(
            "产品分类 Category",
            [""] + cat_list,
            index=([""] + cat_list).index(cur_cat) if cur_cat in cat_list else 0,
            key=kpre + "category"
        )

    issue_name = st.text_input("问题名称", value=str(r.get("IssueName", "") or ""), key=kpre + "issuename")

    d1, d2, d3 = st.columns(3)
    with d1:
        severity = st.selectbox(
            "严重程度",
            [""] + sev_list,
            index=([""] + sev_list).index(cur_sev) if cur_sev in sev_list else 0,
            key=kpre + "severity"
        )
    with d2:
        issue_type = st.selectbox(
            "问题分类",
            [""] + type_list,
            index=([""] + type_list).index(cur_type) if cur_type in type_list else 0,
            key=kpre + "issuetype"
        )
    with d3:
        status = st.selectbox(
            "状态",
            STATUS_OPTIONS,
            index=STATUS_OPTIONS.index(cur_status) if cur_status in STATUS_OPTIONS else 0,
            key=kpre + "status"
        )

    desc = st.text_area("问题描述", value=str(r.get("Description", "") or ""), height=120, key=kpre + "desc")
    temp_fix = st.text_area("临时维修方案", value=str(r.get("TempFix", "") or ""), height=100, key=kpre + "tempfix")
    improve = st.text_area("改进方案", value=str(r.get("ImprovePlan", "") or ""), height=120, key=kpre + "improve")

    t1, t2 = st.columns(2)
    with t1:
        created = st.date_input("录入日期", value=cur_created, key=kpre + "createdat")
    with t2:
        implement = st.date_input("实施日期（可空）", value=cur_impl, key=kpre + "implement")

    st.markdown("### 图片/附件链接（只显示，不在编辑页改）")
    links = str(r.get("ImageLinks", "") or "").strip()
    if links:
        for lk in [x.strip() for x in links.split(";") if x.strip()]:
            st.markdown(f"- {lk}")
    else:
        st.caption("无")

    c_save, c_del = st.columns([1.0, 1.0])

    with c_save:
        if st.button("✅ 保存修改", key=kpre + "btn_update"):
            if not model.strip():
                st.error("请先选择型号")
                st.stop()
            if not category.strip():
                st.error("请先选择产品分类")
                st.stop()
            if not issue_name.strip():
                st.error("问题名称不能为空")
                st.stop()

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
            st.success("✅ 已保存")
            st.rerun()

    with c_del:
        st.caption("⚠️ 删除不可恢复（只删这一行）")
        confirm = st.checkbox("我确认要删除该问题", value=False, key=kpre + "del_confirm")
        if st.button("🗑️ 删除该问题", key=kpre + "btn_delete"):
            if not confirm:
                st.warning("请先勾选确认")
                st.stop()
            delete_row_by_rownum(TAB_ISSUES, row_num)
            bump_ver("v_issues")
            st.success("已删除")
            st.rerun()

def main():
    page_config()

    # ✅ toast（跨 rerun 仍能显示）
    msg = st.session_state.pop("toast", None)
    if msg:
        st.toast(msg)

    # ✅ bootstrap 只在本次会话第一次运行
    if "bootstrapped" not in st.session_state:
        bootstrap()
        st.session_state["bootstrapped"] = True

    # ✅ 读取当前 tab（从 URL query）
    qp = st.query_params
    cur = qp.get("tab", "list")
    if cur not in ["list", "new", "edit", "settings"]:
        cur = "list"

    # ✅ 导航（替代 st.tabs，避免 rerun 回到第一个 tab）
    tab = st.radio(
        "导航",
        ["list","new","edit","settings"],
        format_func=lambda x: {
            "list":"📋 查询列表",
            "new":"➕ 新增问题",
            "edit":"✏️ 编辑问题",
            "settings":"⚙️ 配置"
        }[x],
        index=["list","new","edit","settings"].index(cur),
        horizontal=True,
        key="nav_tab",
    )
    st.query_params["tab"] = tab

    # ✅ 渲染页面
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
