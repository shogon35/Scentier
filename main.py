import streamlit as st
import pandas as pd
import requests
import io
import time
import urllib.parse
import json  # JSON操作用

# 事前に定義するダミーのユーザー情報（ユーザー名: shogo, パスワード: test123）
VALID_USERNAME = "shogo"
VALID_PASSWORD = "test123"

# セッション状態にログイン状態とステップ管理の初期値を設定
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "draft_step" not in st.session_state:
    st.session_state.draft_step = 1
if "selected_persona" not in st.session_state:
    st.session_state.selected_persona = ""

# ------------------------------------------
# ログインフォームの表示
def login():
    st.title("ログイン")
    username = st.text_input("ユーザー名")
    password = st.text_input("パスワード", type="password")
    if st.button("ログイン"):
        if username == VALID_USERNAME and password == VALID_PASSWORD:
            st.session_state.logged_in = True
            st.success("ログイン成功！")
        else:
            st.error("ユーザー名またはパスワードが違います。")

# ------------------------------------------
# 各シートに対応する情報を管理する辞書（既存処理用）
sheet_info = {
    "企業情報一覧DB": {
        "gid": "1256434993",
        "start_row": 2,
        "gas_function": "processCompanyData"
    },
    "競合LP一覧DB": {
        "gid": "730266142",
        "start_row": 2,
        "gas_function": "analyzeLP"
    },
    "バリュープロポジションDB": {
        "gid": "103080236",
        "start_row": 2,
        "gas_function": "analyzeValueproPosition"
    },
    "ペルソナ・訴求DB": {
        "gid": "1313620893",  # ※実際のgidに合わせてください
        "start_row": 2,
        "gas_function": "processPersonaData1"
    },
    "カスタマージャーニーDB": {
        "gid": "1356120243",
        "start_row": 9,
        "gas_function": "analyzeCustomerJourney"
    },
    "訴求ポイント一覧DB": {
        "gid": "1487129962",
        "start_row": 9,
        "gas_function": "analyzeAppealPoint"
    },
    "構成案一覧DB": {
        "gid": "1201145926",
        "start_row": 11,
        "gas_function": "analyzeDraftComposition"
    }
}

# ------------------------------------------
# データ取得関数（既存処理用）
@st.cache_data
def load_data(sheet_name, dummy):
    info = sheet_info[sheet_name]
    gid = info["gid"]
    start_row = info.get("start_row", 1)
    url = (
        "https://docs.google.com/spreadsheets/d/1gAdVDh7NlVc8QoN-3lQzXk58uoyfvdTJ4uvX8ZKKAFw/"
        f"export?format=csv&id=1gAdVDh7NlVc8QoN-3lQzXk58uoyfvdTJ4uvX8ZKKAFw&gid={gid}"
    )
    response = requests.get(url, verify=False)
    response.encoding = "utf-8-sig"
    data = io.StringIO(response.text)
    skiprows = start_row - 1
    df = pd.read_csv(data, skiprows=skiprows)
    return df

# ------------------------------------------
# GAS Webアプリ実行用の基本URL（GET用）
GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxNUCChCm9UEJ6MyY5_DxA5UtT8y7iqC7oQ7BnB8y-QmsfBwG0DM3DKJGoV--002ypY/exec"

# 既存のGETリクエスト用関数
def run_gas(sheet_name):
    try:
        info = sheet_info[sheet_name]
        gas_function = info.get("gas_function", "")
        gas_url = f"{GAS_WEB_APP_URL}?sheet={urllib.parse.quote(sheet_name)}&function={urllib.parse.quote(gas_function)}"
        response = requests.get(gas_url, verify=False)
        response.raise_for_status()
        st.success(f"GAS関数 '{gas_function}' が実行されました")
    except Exception as e:
        st.error(f"GASの実行中にエラーが発生しました: {e}")

# ------------------------------------------
# GASへの更新命令をPOSTリクエストで送信する関数
def update_sheet(command, value):
    payload = {
        "command": command,
        "value": value
    }
    headers = {"Content-Type": "application/json"}
    # ここでは、GASのdoPost()として公開しているURLを利用します
    response = requests.post(GAS_WEB_APP_URL, headers=headers, data=json.dumps(payload), verify=False)
    return response.text

# ------------------------------------------
# アプリ本体のメイン処理
def main():
    if not st.session_state.logged_in:
        login()
        return

    # ページ選択：通常ページと構成案作成ページ
    page = st.sidebar.radio("ページ選択", options=["データベース確認", "構成案作成"])
    
    if page == "データベース確認":
        st.title("Scentier LPOツール")
        st.write("このアプリはスプレッドシートをデータベースとして利用し、複数シートに対応しています。")
        sheet_name = st.sidebar.selectbox("シートを選択してください", list(sheet_info.keys()))
        st.write(f"現在選択中のシート：**{sheet_name}**")
        if st.button(f"{sheet_name}を実行", type='primary'):
            run_gas(sheet_name)
            st.cache_data.clear()
        st.markdown(
            """
            <style>
            div[data-testid="stDataFrameResizable"] table td {
                white-space: normal !important;
                word-break: break-word !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        data = load_data(sheet_name, time.time())
        st.dataframe(data)
    
    elif page == "構成案作成":
        st.title("構成案作成")
        st.write("以下の各ステップに沿って、構成案の作成を進めてください。")
        
        # ----- Step 1: ペルソナ選択 -----
        st.header("Step 1: ペルソナ選択")
        persona_df = load_data("ペルソナ・訴求DB", time.time())
        if "ペルソナ名称" in persona_df.columns:
            persona_options = ["---- 選択してください ----"] + sorted(persona_df["ペルソナ名称"].dropna().unique().tolist())
            selected = st.selectbox("ペルソナ名称を選択してください", persona_options)
            if selected != "---- 選択してください ----":
                st.write(f"選択されたペルソナ：**{selected}**")
                st.session_state.selected_persona = selected
                # 選択したペルソナの詳細情報を expander で表示
                persona_info = persona_df[persona_df["ペルソナ名称"] == selected]
                with st.expander("選択したペルソナの詳細情報"):
                    for idx, row in persona_info.iterrows():
                        st.markdown(f"**AIDMA**: {row.get('AIDMA', '')}")
                        st.markdown(f"**ペルソナ**: {row.get('ペルソナ', '')}")
                        st.markdown(f"**行動**: {row.get('行動', '')}")
                        st.markdown(f"**知りたい情報**: {row.get('知りたい情報', '')}")
                        st.markdown(f"**感情**: {row.get('感情', '')}")
                        st.markdown(f"**訴求ポイント**: {row.get('訴求ポイント', '')}")
                        st.markdown(f"**具体的なペルソナ**: {str(row.get('具体的なペルソナ', '')).replace('<br>', '\n')}")
                        st.markdown("---")
            else:
                st.session_state.selected_persona = ""
        else:
            st.error("ペルソナ・訴求DBに『ペルソナ名称』列が見つかりません。")
            return
        
        if st.button("Step 1: 次に進む"):
            if st.session_state.selected_persona == "":
                st.warning("ペルソナを選択してください。")
                st.stop()
            else:
                st.session_state.draft_step = 2
        
        # ----- Step 2: カスタマージャーニー作成 -----
        if st.session_state.draft_step >= 2:
            st.header("Step 2: カスタマージャーニー作成")
            journey_df = load_data("カスタマージャーニーDB", time.time())
            if "ペルソナ" in journey_df.columns:
                journey_info = journey_df[journey_df["ペルソナ"] == st.session_state.selected_persona]
                if journey_info.empty:
                    st.error("カスタマージャーニーDBに選択されたペルソナのデータは存在しません。")
                    if st.button("カスタマージャーニーを生成する"):
                        result = update_sheet("updateCustomerJourney", st.session_state.selected_persona)
                        with st.spinner('実行中です。しばらくお待ちください...'):
                            run_gas("カスタマージャーニーDB")
                        st.success('実行が完了しました！')
                        st.cache_data.clear()  # キャッシュをクリアして最新のデータを取得
                        journey_df = load_data("カスタマージャーニーDB", time.time())
                        journey_info = journey_df[journey_df["ペルソナ"] == st.session_state.selected_persona]
                        st.success(result)
                        with st.expander("更新後のカスタマージャーニーDB内の対象データ"):
                            for idx, row in journey_info.iterrows():
                                st.markdown(f"**認知（Attention）→**: {row.get('認知（Attention）→', '')}")
                                st.markdown(f"**興味（Interest）→**: {row.get('興味（Interest）→', '')}")
                                st.markdown(f"**欲求（Desire）→**: {row.get('欲求（Desire）→', '')}")
                                st.markdown(f"**行動（Action）→**: {row.get('行動（Action）', '')}")
                                st.markdown("---")
                else:
                    with st.expander("カスタマージャーニーDB内の対象データ"):
                        for idx, row in journey_info.iterrows():
                            st.markdown(f"**認知（Attention）→**: {row.get('認知（Attention）→', '')}")
                            st.markdown(f"**興味（Interest）→**: {row.get('興味（Interest）→', '')}")
                            st.markdown(f"**欲求（Desire）→**: {row.get('欲求（Desire）→', '')}")
                            st.markdown(f"**行動（Action）→**: {row.get('行動（Action）', '')}")
                            st.markdown("---")
            else:
                st.error("カスタマージャーニーDBに『ペルソナ』列が見つかりません。")
            
            if st.button("Step 2: 次に進む"):
                st.session_state.draft_step = 3
        
        # ----- Step 3: 訴求ポイント作成 -----
        if st.session_state.draft_step >= 3:
            st.header("Step 3: 訴求ポイント作成")
            appeal_df = load_data("訴求ポイント一覧DB", time.time())
            if "ペルソナ" in appeal_df.columns:
                matched_appeal = appeal_df[appeal_df["ペルソナ"] == st.session_state.selected_persona]
                if matched_appeal.empty:
                    st.error("訴求ポイント一覧DBに選択されたペルソナのデータは存在しません。")
                    if st.button("訴求ポイントを生成する"):
                        result = update_sheet("updateAppealPoint", st.session_state.selected_persona)
                        with st.spinner('実行中です。しばらくお待ちください...'):
                            run_gas("訴求ポイント一覧DB")
                        st.success('実行が完了しました！')
                        st.cache_data.clear()
                        appeal_df = load_data("訴求ポイント一覧DB", time.time())
                        matched_appeal = appeal_df[appeal_df["ペルソナ"] == st.session_state.selected_persona]
                        st.success(result)
                        with st.expander("更新後の訴求ポイント一覧DB内の対象データ"):
                            for idx, row in matched_appeal.iterrows():
                                st.markdown(f"**{idx} - {row.get('ペルソナ', '')}**")
                                for col, value in row.items():
                                    st.markdown(f"**{col}**: {value}")
                                st.markdown("---")
                else:
                    with st.expander("訴求ポイント一覧DB内の対象データ"):
                        for idx, row in matched_appeal.iterrows():
                            st.markdown(f"**{idx} - {row.get('ペルソナ', '')}**")
                            for col, value in row.items():
                                st.markdown(f"**{col}**: {value}")
                            st.markdown("---")
            else:
                st.error("訴求ポイント一覧DBに『ペルソナ』列が見つかりません。")
            
            if st.button("Step 3: 次に進む"):
                st.session_state.draft_step = 4
        
        # ----- Step 4: 構成案作成 -----
        if st.session_state.draft_step >= 4:
            st.header("Step 4: 構成案作成")
            draft_df = load_data("構成案一覧DB", time.time())
            if "ペルソナ" in draft_df.columns:
                matched_draft = draft_df[draft_df["ペルソナ"] == st.session_state.selected_persona]
                if matched_draft.empty:
                    st.error("構成案一覧DBに選択されたペルソナのデータは存在しません。")
                    if st.button("構成案を生成する"):
                        result = update_sheet("updateDraftComposition", st.session_state.selected_persona)
                        with st.spinner('実行中です。しばらくお待ちください...'):
                            run_gas("構成案一覧DB")
                        st.success('実行が完了しました！')
                        st.cache_data.clear()
                        draft_df = load_data("構成案一覧DB", time.time())
                        matched_draft = draft_df[draft_df["ペルソナ"] == st.session_state.selected_persona]
                        st.success(result)
                        with st.expander("更新後の構成案一覧DB内の対象データ"):
                            for idx, row in matched_draft.iterrows():
                                st.markdown(f"**{idx} - {row.get('ペルソナ', '')}**")
                                for col, value in row.items():
                                    st.markdown(f"**{col}**: {value}")
                                st.markdown("---")
                else:
                    with st.expander("構成案一覧DB内の対象データ"):
                        for idx, row in matched_draft.iterrows():
                            st.markdown(f"**{idx} - {row.get('ペルソナ', '')}**")
                            for col, value in row.items():
                                st.markdown(f"**{col}**: {value}")
                            st.markdown("---")
            else:
                st.error("構成案一覧DBに『ペルソナ』列が見つかりません。")
            
            if st.button("Step 4: 次に進む"):
                st.session_state.draft_step = 5
        
        if st.session_state.draft_step >= 5:
            st.success("構成案の作成が完了しました！")
    
    st.markdown(
        """
        <style>
        div[data-testid="stDataFrameResizable"] table td {
            white-space: normal !important;
            word-break: break-word !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

if __name__ == "__main__":
    main()
