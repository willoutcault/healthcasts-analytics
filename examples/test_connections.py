from analytics_utils import db_utils

print("\n--- Testing PostgreSQL ---")
db_utils.test_postgres_connection()

print("\n--- Testing MySQL via SSH ---")
db_utils.test_mysql_connection()

print("\n--- Running Turnkey Test Data ---")
db_utils.run_combined_engagement_query([9296, 9469, 9316], campaign_type="turnkey")

print("\n--- Running Custom Test Data ---")
db_utils.run_combined_engagement_query([9123], campaign_type="custom")

print("\n--- Running App Time Spent ---")
db_utils.run_time_spent_summary_query([9123])