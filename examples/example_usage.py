from analytics_utils import db_utils as db

if __name__ == "__main__":
    df = db.run_email_engagement_query([1234])
    print(df.head())
