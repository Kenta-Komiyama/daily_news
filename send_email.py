# send_email.py
# 標準ライブラリのみで Gmail 送信（SMTP + アプリ パスワード）
import os, glob, mimetypes, datetime as dt
from email.message import EmailMessage
import smtplib

def newest_outputs(out_dir: str, date_prefix: str, lookback_hours: str):
    """想定ファイル名に一致する CSV/MD を優先し、無ければ out_dir 内の最近のファイルを拾う"""
    targets = [
        os.path.join(out_dir, f"{date_prefix}_news_{lookback_hours}h_fulltext.csv"),
        os.path.join(out_dir, f"{date_prefix}_news_{lookback_hours}h_fulltext.md"),
    ]
    files = [p for p in targets if os.path.exists(p)]
    if files:
        return files

    # フォールバック：out_dir の全ファイルから新しい順に最大4つ
    pats = [os.path.join(out_dir, "*")]
    found = []
    for pat in pats:
        found.extend(glob.glob(pat))
    found.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return found[:4]

def attach_file(msg: EmailMessage, path: str):
    ctype, encoding = mimetypes.guess_type(path)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"
    maintype, subtype = ctype.split("/", 1)
    with open(path, "rb") as f:
        msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=os.path.basename(path))

def main():
    gmail_user = os.environ.get("GMAIL_USER")            # 送信元（xxx@gmail.com）
    gmail_app_pw = os.environ.get("GMAIL_APP_PASSWORD")  # アプリ パスワード（16桁）
    to_addrs    = os.environ.get("GMAIL_TO")             # 送信先（カンマ区切り可）
    subject_prefix = os.environ.get("GMAIL_SUBJECT_PREFIX", "[News]")
    out_dir     = os.environ.get("OUT_DIR", "out")
    lookback    = os.environ.get("LOOKBACK_HOURS", "24")
    tzname      = os.environ.get("TZ", "Asia/Tokyo")

    if not gmail_user or not gmail_app_pw or not to_addrs:
        raise SystemExit("GMAIL_USER / GMAIL_APP_PASSWORD / GMAIL_TO のいずれかが未設定です。")

    # 件名・本文
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    date_prefix_env = os.environ.get("DATE_PREFIX")  # news_crawler.py と合わせる場合に使用
    if not date_prefix_env:
        date_prefix_env = dt.datetime.now().strftime("%Y-%m-%d")

    files = newest_outputs(out_dir, date_prefix_env, lookback)
    if not files:
        body_txt = f"出力ファイルが見つかりませんでした。\nOUT_DIR={out_dir}"
    else:
        body_txt = "最新のクロール結果を添付します。\n\n" + "\n".join(f"- {os.path.basename(p)}" for p in files)

    msg = EmailMessage()
    msg["Subject"] = f"{subject_prefix} {date_prefix_env} 直近{lookback}h 出力"
    msg["From"] = gmail_user
    msg["To"] = to_addrs

    # プレーンテキスト + シンプルHTML
    msg.set_content(body_txt)
    msg.add_alternative(f"""\
    <html>
      <body>
        <p>最新のクロール結果を送付します（{tzname} / {now}）。</p>
        <ul>
          {''.join(f'<li>{os.path.basename(p)}</li>' for p in files)}
        </ul>
      </body>
    </html>
    """, subtype="html")

    for p in files:
        attach_file(msg, p)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(gmail_user, gmail_app_pw)
        smtp.send_message(msg)

    print(f"[OK] Email sent to: {to_addrs} / attachments: {len(files)}")

if __name__ == "__main__":
    main()
