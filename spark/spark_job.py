from pyspark.sql import SparkSession
import time

# Wait for logs to accumulate
print("⏳ Waiting 30 seconds for logs to accumulate...")
time.sleep(30)

# Create Spark session
spark = SparkSession.builder \
    .appName("LogAnalysis") \
    .master("local[*]") \
    .getOrCreate()

print("🔥 Spark Analysis Started...")

# Read logs
try:
    logs_df = spark.read.text("/logs/all_logs.txt")
    
    print("\n" + "="*60)
    print("📊 LOG ANALYSIS RESULTS")
    print("="*60)
    
    # Total logs
    total = logs_df.count()
    print(f"\n📈 Total Logs: {total}")
    
    # Count by level
    error_count = logs_df.filter(logs_df.value.contains("ERROR")).count()
    warn_count = logs_df.filter(logs_df.value.contains("WARN")).count()
    info_count = logs_df.filter(logs_df.value.contains("INFO")).count()
    
    print(f"\n🔴 ERROR Logs: {error_count} ({error_count*100//total if total > 0 else 0}%)")
    print(f"🟡 WARN Logs:  {warn_count} ({warn_count*100//total if total > 0 else 0}%)")
    print(f"🟢 INFO Logs:  {info_count} ({info_count*100//total if total > 0 else 0}%)")
    
    # Count by service
    print("\n📦 Logs by Service:")
    for service in ["UserService", "PaymentService", "AuthService", "DatabaseService"]:
        count = logs_df.filter(logs_df.value.contains(service)).count()
        print(f"   {service}: {count}")
# Show sample logs
    print("\n📋 Sample Logs (First 5):")
    logs_df.show(5, truncate=False)
    
    print("="*60)
    print("✅ Analysis Complete!")
    print("="*60 + "\n")

except Exception as e:
    print(f"❌ Error: {e}")
    print("No logs found yet. Run this after logs accumulate.")

spark.stop()
