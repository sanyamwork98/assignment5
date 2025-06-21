import os
import yagmail


SENDER_EMAIL = "sanyamworks98@gmail.com"
APP_PASSWORD = "pgsdphpkhfogsfgc"
RECIPIENT_EMAIL = "sanyamjain9875@gmail.com"  


export_folder = os.getcwd()  


files_to_send = [
    os.path.join(export_folder, f)
    for f in os.listdir(export_folder)
    if f.endswith((".csv", ".parquet", ".avro"))
]


try:
    yag = yagmail.SMTP(SENDER_EMAIL, APP_PASSWORD)
    yag.send(
        to=RECIPIENT_EMAIL,
        subject=" Exported Tables - Data Pipeline",
        contents="Please find attached the exported tables (CSV, Parquet, Avro).",
        attachments=files_to_send
    )
    print(" Email sent with all exports!")
except Exception as e:
    print(" Failed to send email:", e)
