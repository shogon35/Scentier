import httpx
import streamlit as st
from openai import OpenAI
import pandas as pd
from google.cloud import bigquery
from datetime import date, timedelta, datetime
import json
import streamlit.components.v1 as components
import pygwalker as pyg  # pygwalkerのインポート

# ワイド表示（必要に応じて）
st.set_page_config(layout="wide")

# Looker Studio のダッシュボードの公開用 URL（適宜変更してください）
dashboard_url = "https://lookerstudio.google.com/embed/reporting/2f075562-791f-46ff-99d6-5425963c698c/page/6HI9"
iframe_html = f'''
    <iframe width="100%" height="400" src="{dashboard_url}" frameborder="0" style="border:0" allowfullscreen></iframe>
'''

# 定数（実際のプロジェクト・データセット名に合わせて変更）
TEMPLATES_TABLE = "scentier.sql_templates.dataset"  # テンプレート管理テーブル
RESULTS_TABLE = "scentier.sql_templates.template_execution_results"  # 実行結果保存用テーブル

# APIキーの設定（直接記入）
client = OpenAI(
    api_key="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",
    http_client=httpx.Client(verify=False)
)

# BigQuery クライアントの初期化（認証情報もコード内で指定）
bq_client = bigquery.Client.from_service_account_json('/Users/sh.noda/Desktop/app作成/scentier-analysis/scentier-35712e322bc6.json')

# ----- 共通関数 -----

def get_table_list(project_id, dataset_id):
    dataset_ref = bq_client.dataset(dataset_id)
    tables = list(bq_client.list_tables(dataset_ref))
    table_list = [f"{project_id}.{dataset_id}.{table.table_id}" for table in tables]
    return table_list

def get_ga4_sample(table_name):
    query = f"SELECT * FROM {table_name} LIMIT 50"
    return execute_bq_query(query)

def generate_sql(question, sample_data, table_name):
    schema_description = f"テーブル名: {table_name}"
    prompt = f"""
    あなたはBigQuery SQLエキスパートです。
    GA4データを分析するために以下のようなテーブルがあります。

    {schema_description}

    データサンプル:
    {sample_data.head().to_json(orient='records', force_ascii=False)}

    以下の質問を実行可能な正しいBigQuery標準SQLのみで答えてください。
    余計な説明や「TO」など不要なキーワードは絶対に含めないでください。※実行用のSQL文のみを出力しなさい。

    【重要】
    - SQL以外の自然言語やコメント、説明文を絶対に含めないでください。
    - 必ず「SELECT」または「WITH」キーワードでSQLを開始してください。

    質問: {question}
    """
    response = client.chat.completions.create(
        model="o3-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        timeout=30
    )
    return response.choices[0].message.content.strip()

def execute_bq_query(sql):
    query_job = bq_client.query(sql)
    return query_job.result().to_dataframe()

def generate_insights(question, result_df):
    sample_json = result_df.head().to_json(orient='records', force_ascii=False)
    prompt = f"""
    あなたは熟練のデータアナリストです。以下はBigQueryで実行したSQLの結果サンプルです。
    このデータに基づいて、主要なトレンドや重要なポイント、今後の改善や施策の示唆を、短い日本語で要約してください。

    【質問】
    {question}

    【SQL実行結果サンプル】
    {sample_json}
    """
    response = client.chat.completions.create(
        model="o3-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        timeout=30
    )
    return response.choices[0].message.content.strip()

# --- 新規追加：次の分析提案生成用関数 ---
def generate_next_analysis_suggestions(question, sample_data, result_df):
    # テーブルのカラム情報（型）を作成
    schema_info = "カラム情報:\n"
    for col in sample_data.columns:
        schema_info += f"{col}: {sample_data[col].dtype}\n"
    sample_json = sample_data.head().to_json(orient='records', force_ascii=False)
    result_sample_json = result_df.head().to_json(orient='records', force_ascii=False)
    prompt = f"""
    あなたは熟練のデータアナリストです。以下はBigQueryから取得したテーブルのスキーマ情報、データサンプル、及び初期分析の結果サンプルです。

    【テーブルスキーマ】
    {schema_info}

    【データサンプル】
    {sample_json}

    【初期分析結果サンプル】
    {result_sample_json}

    上記の情報に基づいて、さらに深堀りできそうな分析の視点やテーマを、候補リスト形式で複数提案してください。
    それぞれの候補には、簡単な説明と具体的な次の分析のためのSQL例も含めてください。
    """
    response = client.chat.completions.create(
        model="o3-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        timeout=30
    )
    return response.choices[0].message.content.strip()

# ----- SQLテンプレート用関数 -----

def get_sql_templates():
    query = f"""
    SELECT template_name, sql_template
    FROM {TEMPLATES_TABLE}
    ORDER BY template_name
    """
    try:
        df = bq_client.query(query).result().to_dataframe()
        return {row["template_name"]: row["sql_template"] for index, row in df.iterrows()}
    except Exception as e:
        st.error(f"テンプレート取得エラー: {e}")
        return {}

def add_sql_template(template_name, sql_template):
    query = f"""
    INSERT INTO {TEMPLATES_TABLE} (template_name, sql_template, updated_at)
    VALUES (@template_name, @sql_template, CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("template_name", "STRING", template_name),
            bigquery.ScalarQueryParameter("sql_template", "STRING", sql_template)
        ]
    )
    bq_client.query(query, job_config=job_config).result()

def update_sql_template(template_name, new_sql_template):
    query = f"""
    UPDATE {TEMPLATES_TABLE}
    SET sql_template = @new_sql_template, updated_at = CURRENT_TIMESTAMP()
    WHERE template_name = @template_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("template_name", "STRING", template_name),
            bigquery.ScalarQueryParameter("new_sql_template", "STRING", new_sql_template)
        ]
    )
    bq_client.query(query, job_config=job_config).result()

# 新たに実行結果をBQに保存する関数
def save_template_execution_result(template_name, result_df):
    result_json = result_df.to_json(orient='records', force_ascii=False)
    row = {
        "template_name": template_name,
        "execution_time": datetime.utcnow().isoformat(),
        "result_json": result_json
    }
    errors = bq_client.insert_rows_json(RESULTS_TABLE, [row])
    if errors:
        st.error(f"結果保存エラー: {errors}")
    else:
        st.success("実行結果をBigQueryに保存しました。")

# 最新の実行結果を各テンプレートごとに取得する関数
def get_latest_template_results():
    query = f"""
    SELECT template_name, result_json, execution_time
    FROM (
      SELECT template_name, result_json, execution_time,
             ROW_NUMBER() OVER (PARTITION BY template_name ORDER BY execution_time DESC) as rn
      FROM {RESULTS_TABLE}
    )
    WHERE rn = 1
    ORDER BY template_name
    """
    try:
        df = bq_client.query(query).result().to_dataframe()
        return df
    except Exception as e:
        st.error(f"実行結果取得エラー: {e}")
        return pd.DataFrame()


# --- 新規：次の分析提案生成用関数（改善版） ---
def generate_next_analysis_suggestions(question, result_df, llm_insights):
    # 取得結果のサンプルをJSON形式で取得
    result_sample_json = result_df.head().to_json(orient='records', force_ascii=False)
    prompt = f"""
    あなたは熟練のデータアナリストです。以下はユーザーが初回に入力した質問、BigQueryで取得した結果サンプル、及びLLMによる初期分析の示唆です。

    【ユーザーの質問】
    {question}

    【取得結果サンプル】
    {result_sample_json}

    【LLMによる初期分析の示唆】
    {llm_insights}

    上記の情報を基に、ユーザーの意図を踏まえて、さらに深堀りできそうな分析の視点やテーマを候補リスト形式で複数提案してください。
    それぞれの候補には、簡単な説明を加えてください。
    """
    response = client.chat.completions.create(
        model="o3-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        timeout=30
    )
    return response.choices[0].message.content.strip()


# ----- Streamlit UI -----

# ページ切替用（サイドバー）
page = st.sidebar.selectbox("ページ選択", [
    "自然言語分析", 
    "SQLテンプレート実行", 
    "レポート一覧", 
    "チャットモード", 
    "ダッシュボードでのデータ分析",
    "レポートCSV分析"  # 新規追加
])

# ----- サイドバーに追加実装：チャットモード（入力欄を下部に配置、メッセージ表示をスクロール可能に） -----
with st.sidebar.expander("チャットモード", expanded=False):
    st.write("生成AIとチャットして質問や相談ができます。")
    
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []
    
    # チャットメッセージ表示用HTML生成関数
    def render_chat_messages(messages):
        chat_html = "<div style='height:300px; overflow-y: auto; padding:5px; border: 1px solid #ffffff; background-color: white; border-radius: 5px;'>"
        for msg in messages:
            if msg["role"] == "user":
                chat_html += f"<p><strong>User:</strong> {msg['content']}</p>"
            else:
                chat_html += f"<p><strong>Assistant:</strong> {msg['content']}</p>"
        chat_html += "</div>"
        return chat_html

    # 表示領域を更新するコンテナ
    chat_display_container = st.empty()
    chat_display_container.markdown(render_chat_messages(st.session_state.chat_messages), unsafe_allow_html=True)
    
    # 入力欄は下部に配置
    user_input_sidebar = st.text_input("メッセージを入力", key="sidebar_chat_input_bottom")
    if st.button("送信", key="sidebar_chat_send_bottom") and user_input_sidebar:
        # ユーザーの入力をセッションに保存
        st.session_state.chat_messages.append({"role": "user", "content": user_input_sidebar})
        
        # まずユーザーの入力を即時に表示
        chat_display_container.markdown(render_chat_messages(st.session_state.chat_messages), unsafe_allow_html=True)
        
        # AIの応答をストリーミングで取得し、チャット表示領域内で更新
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.chat_messages],
            stream=True,
        )
        
        response_text = ""
        for chunk in stream:
            token = chunk.choices[0].delta.content or ""
            response_text += token
            
            # 一時的にAIの応答を追加して表示エリアを更新
            temp_messages = st.session_state.chat_messages + [{"role": "assistant", "content": response_text}]
            chat_display_container.markdown(render_chat_messages(temp_messages), unsafe_allow_html=True)
        
        # 応答が完了したら正式にセッション変数に追加
        st.session_state.chat_messages.append({"role": "assistant", "content": response_text})
        chat_display_container.markdown(render_chat_messages(st.session_state.chat_messages), unsafe_allow_html=True)





# セッション変数の初期化
if "generated_sql" not in st.session_state:
    st.session_state.generated_sql = ""
if "sample_data" not in st.session_state:
    st.session_state.sample_data = pd.DataFrame()
if "template_results" not in st.session_state:
    st.session_state.template_results = {}
if "edit_mode" not in st.session_state:
    st.session_state.edit_mode = False
if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []  # チャットモード用
if "last_result_df" not in st.session_state:
    st.session_state.last_result_df = None
if "next_analysis_suggestions" not in st.session_state:
    st.session_state.next_analysis_suggestions = ""

st.markdown(
    """
    <style>
    [data-testid="stSidebar"] {
        min-width: 400px;
        max-width: 800px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# 自然言語分析ページ（既存のコード＋次の分析提案機能追加）
if page == "自然言語分析":
    st.title("データ分析アプリ")
    project_id = bq_client.project
    dataset_id = "analytics_398653344"
    # ダッシュボード埋め込み
    components.html(iframe_html, height=400)



    # # サイドバーに iframe を埋め込み
    # with st.sidebar:
    #     components.html(
    #         """
    #         <iframe
    #          src="https://udify.app/chatbot/kE1tcLRffcRC4i8L"
    #          style="width: 100%; height: 100%; min-height: 600px"
    #          frameborder="0"
    #          allow="microphone">
    #         </iframe>
    #         """,
    #         height=700
    #     )

    with st.spinner("BigQueryからテーブル情報を取得中..."):
        try:
            table_list = get_table_list(project_id, dataset_id)
        except Exception as e:
            st.error(f"テーブル情報取得エラー: {e}")
            st.stop()
    selected_table = st.selectbox("分析対象のテーブルを選択してください", table_list)
    question = st.text_input("分析したい内容を日本語で入力してください")
    if st.button("分析開始"):
        if not question.strip():
            st.error("質問を入力してください。")
            st.stop()
        with st.spinner("GA4データのサンプル取得中..."):
            try:
                st.session_state.sample_data = get_ga4_sample(selected_table)
                st.subheader("GA4データのサンプル")
                st.dataframe(st.session_state.sample_data.head())
            except Exception as e:
                st.error(f"サンプル取得エラー: {e}")
                st.stop()
        with st.spinner("SQL生成中..."):
            try:
                st.session_state.generated_sql = generate_sql(question, st.session_state.sample_data, selected_table)
            except Exception as e:
                st.error(f"SQL生成エラー: {e}")
                st.stop()
    if st.session_state.generated_sql:
        user_edited_sql = st.text_area("SQL文を確認・修正してください:", st.session_state.generated_sql, height=300)
        
        if st.button("SQLを実行", key="execute_sql"):
            with st.spinner("SQLを実行中..."):
                try:
                    result_df = execute_bq_query(user_edited_sql)
                    st.subheader("取得結果")
                    st.dataframe(result_df)
                    # CSVダウンロードボタンなどの処理...
                    csv_data = result_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="CSVとしてダウンロード",
                        data=csv_data,
                        file_name="query_result.csv",
                        mime="text/csv"
                    )
                    # 初回分析結果をセッション変数に保存
                    st.session_state.last_result_df = result_df
                    
                    with st.spinner("LLMによる分析・示唆出し中..."):
                        insights = generate_insights(question, result_df)
                        st.session_state.llm_insights = insights
                    st.subheader("LLMによる分析・示唆")
                    st.write(insights)
                except Exception as e:
                    st.error(f"クエリ実行エラー: {e}")

        
        # ----- 次の分析提案セクション（初回分析が完了していれば表示） -----
        if st.session_state.last_result_df is not None and "llm_insights" in st.session_state:
            if st.button("次の分析提案を生成", key="gen_next_suggestions"):
                # 既に保存されたLLM示唆も表示（必要に応じて）
                st.subheader("取得結果")
                st.dataframe(st.session_state.last_result_df)
                st.subheader("LLMによる分析・示唆")
                st.write(st.session_state.llm_insights)
                with st.spinner("次の分析提案生成中..."):
                    suggestions = generate_next_analysis_suggestions(
                        question,
                        st.session_state.last_result_df,
                        st.session_state.llm_insights
                    )
                    st.session_state.next_analysis_suggestions = suggestions
            if st.session_state.next_analysis_suggestions:
                st.subheader("次の分析提案")
                st.info(st.session_state.next_analysis_suggestions)
 # "SQLテンプレート実行", "レポート一覧", "チャットモード", "ダッシュボードでのデータ分析"部分は省略       

# SQLテンプレート実行ページ（既存コード）
elif page == "SQLテンプレート実行":
    st.title("SQLテンプレート実行")
    components.html(iframe_html, height=400)
    st.subheader("SQLテンプレート追加")
    new_template_name = st.text_input("テンプレート名を入力してください", key="new_template_name")
    new_sql_template = st.text_area("SQLテンプレートの内容を入力してください", key="new_sql_template")
    if st.button("テンプレート追加"):
        if not new_template_name.strip() or not new_sql_template.strip():
            st.error("テンプレート名とSQLは必須です。")
        else:
            try:
                add_sql_template(new_template_name, new_sql_template)
                st.success("テンプレートを追加しました。")
            except Exception as e:
                st.error(f"テンプレート追加エラー: {e}")
    with st.spinner("BigQueryからテンプレート情報を取得中..."):
        templates = get_sql_templates()
    if templates:
        template_names = list(templates.keys())
        selected_template = st.selectbox("SQLテンプレートを選択してください", template_names)
        st.subheader("選択中のテンプレート")
        st.code(templates[selected_template], language="sql")
        if st.button("編集モードに切り替え"):
            st.session_state.edit_mode = True
        if st.session_state.edit_mode:
            edited_sql = st.text_area("テンプレートSQLを編集してください", value=templates[selected_template], key="edited_template_sql")
            if st.button("テンプレート更新"):
                if not edited_sql.strip():
                    st.error("SQL文は空白にできません。")
                else:
                    try:
                        update_sql_template(selected_template, edited_sql)
                        st.success("テンプレートを更新しました。")
                        st.session_state.edit_mode = False
                        templates = get_sql_templates()
                    except Exception as e:
                        st.error(f"テンプレート更新エラー: {e}")
        default_end = date.today()
        default_start = default_end - timedelta(days=7)
        start_date, end_date = st.date_input("実行期間を指定してください", value=(default_start, default_end))
        sql_to_execute = templates[selected_template]
        if "{{start_date}}" in sql_to_execute:
            sql_to_execute = sql_to_execute.replace("{{start_date}}", start_date.strftime("%Y-%m-%d"))
        if "{{end_date}}" in sql_to_execute:
            sql_to_execute = sql_to_execute.replace("{{end_date}}", end_date.strftime("%Y-%m-%d"))
        st.subheader("実行されるSQL")
        st.code(sql_to_execute, language="sql")
        if selected_template in st.session_state.template_results:
            st.subheader("既存の実行結果")
            st.dataframe(st.session_state.template_results[selected_template])
        else:
            st.info("このテンプレートの実行結果はまだ存在しません。")
        if st.button("データ更新"):
            with st.spinner("SQL実行中..."):
                try:
                    result_df = execute_bq_query(sql_to_execute)
                    st.session_state.template_results[selected_template] = result_df
                    st.success("実行結果をセッションに保存しました。")
                    st.dataframe(result_df)
                    # CSVダウンロードボタンを追加（テンプレート実行結果用）
                    csv_data = result_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="CSVとしてダウンロード",
                        data=csv_data,
                        file_name="template_query_result.csv",
                        mime="text/csv"
                    )
                    save_template_execution_result(selected_template, result_df)
                except Exception as e:
                    st.error(f"SQL実行エラー: {e}")
    else:
        st.info("登録済みのテンプレートはありません。上記フォームからテンプレートを追加してください。")


# レポート一覧ページ（既存コード）
elif page == "レポート一覧":
    st.title("レポート一覧")
    with st.spinner("最新の実行結果を取得中..."):
        latest_results = get_latest_template_results()
    if latest_results.empty:
        st.info("まだ実行結果が存在しません。")
    else:
        for index, row in latest_results.iterrows():
            template_name = row["template_name"]
            execution_time = row["execution_time"]
            result_json = row["result_json"]
            try:
                report_df = pd.read_json(result_json, orient="records")
            except Exception as e:
                st.error(f"{template_name} の結果JSONの解析エラー: {e}")
                continue
            st.subheader(f"テンプレート: {template_name} （実行時刻: {execution_time}）")
            cols = st.columns(2)
            with cols[0]:
                st.write("【データ】")
                st.dataframe(report_df)
            with cols[1]:
                st.write("【グラフ】")
                numeric_cols = report_df.select_dtypes(include=['int64', 'float64']).columns
                if len(numeric_cols) > 0:
                    st.bar_chart(report_df.set_index(report_df.columns[0])[numeric_cols[0]])
                else:
                    st.info("グラフ化する数値データがありません。")

# チャットモードページ（既存コード修正済み）
elif page == "チャットモード":
    st.title("チャットモード")
    st.write("生成AIとチャットして質問や相談ができます。")
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []
    for msg in st.session_state.chat_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
    user_input = st.chat_input("メッセージを入力してください")
    if user_input:
        st.session_state.chat_messages.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)
        stream = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.chat_messages],
            stream=True,
        )
        response_text = ""
        placeholder = st.empty()
        with st.chat_message("assistant"):
            for chunk in stream:
                token = chunk.choices[0].delta.content or ""
                response_text += token
                placeholder.markdown(response_text)
        st.session_state.chat_messages.append({"role": "assistant", "content": response_text})

elif page == "自動提案エンジン":
    st.title("自動提案エンジン")
    st.write("データサンプルと初期分析結果に基づき、次に取り組むべき分析テーマを提案します。")
    
    # セッション変数の初期化（存在しない場合のみ）
    if "sample_data" not in st.session_state:
        st.session_state.sample_data = None
    if "initial_summary" not in st.session_state:
        st.session_state.initial_summary = ""
    if "suggestions" not in st.session_state:
        st.session_state.suggestions = []
    if "selected_suggestion" not in st.session_state:
        st.session_state.selected_suggestion = None
    if "generated_sql" not in st.session_state:
        st.session_state.generated_sql = ""
    
    project_id = bq_client.project
    dataset_id = "analytics_398653344"
    try:
        table_list = get_table_list(project_id, dataset_id)
    except Exception as e:
        st.error(f"テーブル情報取得エラー: {e}")
        st.stop()
    
    # テーブル選択（キーを指定してセッションに保持）
    selected_table = st.selectbox("分析対象のテーブルを選択してください", table_list, key="auto_table")
    
    # サンプルデータ取得ボタン
    if st.button("サンプルデータ取得", key="sample_data_button"):
        try:
            st.session_state.sample_data = get_ga4_sample(selected_table)
            st.subheader("データサンプル")
            st.dataframe(st.session_state.sample_data.head())
            with st.spinner("初期分析結果の要約生成中..."):
                st.session_state.initial_summary = generate_initial_summary(st.session_state.sample_data)
            st.subheader("初期分析結果の要約")
            st.write(st.session_state.initial_summary)
            with st.spinner("分析提案を生成中..."):
                st.session_state.suggestions = generate_analysis_suggestions(st.session_state.sample_data, st.session_state.initial_summary)
            if st.session_state.suggestions:
                st.subheader("分析提案候補")
                # radio ウィジェットのキーを指定して、選択した値がセッションに保持されるようにする
                st.session_state.selected_suggestion = st.radio("次に取り組むべき分析テーマを選択してください", st.session_state.suggestions, key="suggestion_radio")
            else:
                st.info("提案候補が生成されませんでした。")
        except Exception as e:
            st.error(f"エラー: {e}")
    
    # 「この提案で分析開始」ボタン：ここで生成されたSQLをセッションに保存
    if st.button("この提案で分析開始", key="start_analysis_button"):
        if st.session_state.selected_suggestion:
            st.info(f"選択された分析テーマ: {st.session_state.selected_suggestion}")
            with st.spinner("SQL生成中..."):
                generated_sql = generate_sql(st.session_state.selected_suggestion, st.session_state.sample_data, selected_table)
            st.subheader("生成されたSQL")
            st.code(generated_sql, language="sql")
            st.session_state.generated_sql = generated_sql  # 生成されたSQLをセッションに保存
        else:
            st.error("分析テーマが選択されていません。")
    
    # 「SQLを実行」ボタンは、保存済みのSQLがある場合に実行
    if st.button("SQLを実行", key="execute_sql_button"):
        if st.session_state.generated_sql:
            with st.spinner("SQL実行中..."):
                result_df = execute_bq_query(st.session_state.generated_sql)
            st.subheader("取得結果")
            st.dataframe(result_df)
            with st.spinner("LLMによる追加分析・示唆生成中..."):
                insights = generate_insights(st.session_state.selected_suggestion, result_df)
            st.subheader("追加分析の示唆")
            st.write(insights)
        else:
            st.error("生成されたSQLがありません。")

# 新規追加：レポートCSV分析ページ
elif page == "レポートCSV分析":
    st.title("ウェブ広告レポート分析")
    st.write("CSVファイルをアップロードし、生成AIを用いて各タスクの分析を実施します。")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロード", type=["csv"], key="report_csv")
    if uploaded_file is not None:
        try:
            csv_df = pd.read_csv(uploaded_file)
            st.subheader("アップロードされたCSVのサンプル")
            st.dataframe(csv_df.head())
            st.session_state.report_csv_df = csv_df
        except Exception as e:
            st.error(f"CSV読み込みエラー: {e}")
    else:
        st.session_state.report_csv_df = None

    # --- タスク②：事実抽出と仮説生成 ---
    def generate_facts_and_hypotheses_csv(df):
        full_json = df.to_json(orient='records', force_ascii=False)
        prompt = f"""
            以下のCSVファイルには、今週と先週のウェブ広告配信実績データおよび目標値が含まれています。このデータをもとに、以下の内容を抽出してください。
            1. 各指標（例：インプレッション数、クリック数、CTR、費用、コンバージョン数など）の今週と先週の実績の違いを明確にする。
            2. 目標値との乖離や実績のトレンドから、重要な事実や傾向を抽出する。
            3. 実績の変動要因として考えられる仮説（例：ターゲット層の変動、配信タイミングの影響、予算変更、クリエイティブの違いなど）を複数提案し、各仮説の根拠となるデータポイントや傾向を示す。
            【CSVデータサンプル】
            {full_json}
        """
        response = client.chat.completions.create(
             model="o3-mini",
             messages=[
                 {"role": "system", "content": "You are a helpful assistant."},
                 {"role": "user", "content": prompt}
             ],
             timeout=30
        )
        return response.choices[0].message.content.strip()

    if st.session_state.report_csv_df is not None:
        if st.button("事実抽出と仮説生成", key="btn_task2"):
            with st.spinner("生成中..."):
                facts_hypotheses = generate_facts_and_hypotheses_csv(st.session_state.report_csv_df)
                st.session_state.facts_hypotheses = facts_hypotheses
                st.subheader("事実抽出と仮説生成結果")
                st.write(facts_hypotheses)
        
        # --- タスク③：検証方法の方針生成 ---
        def generate_validation_plan_csv(df, hypotheses):
            full_json = df.to_json(orient='records', force_ascii=False)
            prompt = f"""
                タスク②で抽出した仮説は以下の通りです:
                {hypotheses}

                上記仮説に基づき、CSVデータのサンプル（以下）から、各仮説を検証するための具体的な検証方法の方針を策定してください。以下の点を含めてください。
                1. 検証に必要な追加データや分析手法の提案（例：時系列分析、相関分析など）。
                2. 具体的な検証ステップや評価指標の提案。
                3. 仮説ごとに、検証結果をどのように解釈するかの基準と、次に取るべきアクションの方針。
                【CSVデータサンプル】
                {full_json}
            """
            response = client.chat.completions.create(
                 model="o3-mini",
                 messages=[
                     {"role": "system", "content": "You are a helpful assistant."},
                     {"role": "user", "content": prompt}
                 ],
                 timeout=30
            )
            return response.choices[0].message.content.strip()
        
        if "facts_hypotheses" in st.session_state:
            if st.button("検証方法の方針生成", key="btn_task3"):
                with st.spinner("生成中..."):
                    validation_plan = generate_validation_plan_csv(st.session_state.report_csv_df, st.session_state.facts_hypotheses)
                    st.session_state.validation_plan = validation_plan
                    st.subheader("検証方法の方針生成結果")
                    st.write(validation_plan)
        
        # --- タスク④：検証用SQL生成 ---
        def generate_validation_sql_csv(df, validation_plan):
            full_json = df.to_json(orient='records', force_ascii=False)
            prompt = f"""
                タスク③で策定した検証方法の方針は以下の通りです:
                {validation_plan}

                上記検証方法に沿って、CSVデータ（以下のサンプル）から必要な情報を抽出するためのSQLクエリを作成してください。以下の要件に留意してください。
                1. 今週と先週の各主要指標（例：クリック数、コンバージョン数、費用など）の比較を行うクエリ。
                2. 目標値との乖離を確認するための条件や集計ロジックを含むこと。
                3. 各仮説に関連するデータを抽出できるようにすること。
                【CSVデータサンプル】
                {full_json}
            """
            response = client.chat.completions.create(
                 model="o3-mini",
                 messages=[
                     {"role": "system", "content": "You are a helpful assistant."},
                     {"role": "user", "content": prompt}
                 ],
                 timeout=30
            )
            return response.choices[0].message.content.strip()
        
        if "validation_plan" in st.session_state:
            if st.button("検証用SQL生成", key="btn_task4"):
                with st.spinner("生成中..."):
                    validation_sql = generate_validation_sql_csv(st.session_state.report_csv_df, st.session_state.validation_plan)
                    st.session_state.validation_sql = validation_sql
                    st.subheader("検証用SQL生成結果")
                    st.code(validation_sql, language="sql")
        
        # --- タスク⑤：分析実行 ---
        if "validation_sql" in st.session_state:
            if st.button("分析実行", key="btn_task5"):
                # ※ここではCSVの内容をそのまま分析結果とする（実際はSQL実行などの処理が必要）
                analysis_result_df = st.session_state.report_csv_df
                st.session_state.analysis_result_df = analysis_result_df
                st.subheader("分析実行結果")
                st.dataframe(analysis_result_df)
        
        # --- タスク⑥：最終的な分析示唆生成 ---
        def generate_final_insights_csv(df, hypotheses, sql_query):
            full_json = df.to_json(orient='records', force_ascii=False)
            prompt = f"""
                以下は今週と先週のウェブ広告配信実績データおよび目標値を含むCSVデータのサンプルです:
                {full_json}

                タスク②で提案された仮説:
                {hypotheses}

                タスク④で作成された検証用SQLクエリ:
                {sql_query}

                上記情報をもとに、各仮説の検証結果から主要な要因を特定し、変動要因の重要度や相関関係に基づいて、優先的に改善すべきポイントと今後の対策を具体的に提案してください。
            """
            response = client.chat.completions.create(
                 model="o3-mini",
                 messages=[
                     {"role": "system", "content": "You are a helpful assistant."},
                     {"role": "user", "content": prompt}
                 ],
                 timeout=30
            )
            return response.choices[0].message.content.strip()
        
        if "analysis_result_df" in st.session_state and "validation_sql" in st.session_state:
            if st.button("最終的な分析示唆生成", key="btn_task6"):
                with st.spinner("生成中..."):
                    final_insights = generate_final_insights_csv(
                        st.session_state.analysis_result_df,
                        st.session_state.facts_hypotheses,
                        st.session_state.validation_sql
                    )
                    st.session_state.final_insights = final_insights
                    st.subheader("最終的な分析示唆")
                    st.write(final_insights)

# ダッシュボードでのデータ分析ページ（既存コード）
elif page == "ダッシュボードでのデータ分析":
    st.title("ダッシュボードでのデータ分析")
    st.write("CSVファイルをアップロードして、ダッシュボードを起動してください。")
    uploaded_file = st.file_uploader("CSVファイルをアップロード", type=["csv"])
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            st.dataframe(df.head())
        except Exception as e:
            st.error(f"ファイルの読み込みエラー: {e}")
    else:
        df = None

    if st.button("ダッシュボードを起動"):
        if df is not None:
            pyg.walk(df, env='Streamlit')
        else:
            st.warning("まずCSVファイルをアップロードしてください。")