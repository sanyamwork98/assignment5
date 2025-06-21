import schedule
import time
import subprocess

def job():
    print(" Running export job...")
   
    subprocess.run(["python", "export_formats.py"])


schedule.every(10).minutes.do(job)

print("✅ Auto-export scheduler started. Will run every 10 minutes.")


while True:
    schedule.run_pending()
    time.sleep(1)
