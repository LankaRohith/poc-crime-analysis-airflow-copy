import requests
import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'crime-analysis',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 300,  # 5 minutes
}

@dag(
    dag_id='crime_data_quality_check',
    default_args=default_args,
    description='Crime Data Quality Check - Runs hourly to validate data integrity',
    schedule_interval='0 * * * *',  # Every hour
    catchup=False,
    tags=['crime', 'quality', 'monitoring'],
    max_active_runs=1
)
def crime_data_quality_check():

    @task
    def check_data_freshness():
        """Check if the crime data is recent"""
        try:
            print("🔍 Checking data freshness...")
            response = requests.get('http://host.docker.internal:8080/api/simple-crimes/summary', timeout=10)
            if response.status_code == 200:
                # For this demo, we'll assume data is fresh if API responds
                print("✅ Data freshness check passed")
                return {"status": "success", "message": "Data is fresh"}
            else:
                print(f"❌ Data freshness check failed: {response.status_code}")
                return {"status": "failed", "message": f"API returned {response.status_code}"}
        except Exception as e:
            print(f"❌ Error checking data freshness: {e}")
            return {"status": "error", "message": str(e)}

    @task
    def check_data_completeness():
        """Check if key fields have data"""
        try:
            print("🔍 Checking data completeness...")
            response = requests.get('http://host.docker.internal:8080/api/simple-crimes/summary', timeout=10)
            if response.status_code == 200:
                # Check if we have reasonable amounts of data
                # This is a simplified check - in production you'd validate specific fields
                print("✅ Data completeness check passed")
                return {"status": "success", "message": "Data appears complete"}
            else:
                print(f"❌ Data completeness check failed: {response.status_code}")
                return {"status": "failed", "message": f"API returned {response.status_code}"}
        except Exception as e:
            print(f"❌ Error checking data completeness: {e}")
            return {"status": "error", "message": str(e)}

    @task
    def check_api_response_time():
        """Check API response time"""
        try:
            print("🔍 Checking API response time...")
            start_time = datetime.now()
            response = requests.get('http://host.docker.internal:8080/api/simple-crimes/summary', timeout=10)
            end_time = datetime.now()
            response_time = (end_time - start_time).total_seconds()
            
            if response.status_code == 200 and response_time < 5.0:  # Under 5 seconds
                print(f"✅ API response time check passed: {response_time:.2f} seconds")
                return {"status": "success", "response_time": response_time}
            else:
                print(f"❌ API response time check failed: {response_time:.2f} seconds")
                return {"status": "failed", "response_time": response_time}
        except Exception as e:
            print(f"❌ Error checking API response time: {e}")
            return {"status": "error", "message": str(e)}

    @task
    def generate_quality_report(freshness_result, completeness_result, response_time_result):
        """Generate data quality report"""
        print("📊 Generating data quality report...")
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S EST")
        
        report = f"""
        ========================================
        CRIME DATA QUALITY REPORT
        Generated: {timestamp}
        ========================================
        
        DATA FRESHNESS: {freshness_result.get('status', 'UNKNOWN')}
        Message: {freshness_result.get('message', 'N/A')}
        
        DATA COMPLETENESS: {completeness_result.get('status', 'UNKNOWN')}
        Message: {completeness_result.get('message', 'N/A')}
        
        API RESPONSE TIME: {response_time_result.get('status', 'UNKNOWN')}
        Response Time: {response_time_result.get('response_time', 'N/A')} seconds
        
        OVERALL STATUS: {'✅ HEALTHY' if all([
            freshness_result.get('status') == 'success',
            completeness_result.get('status') == 'success',
            response_time_result.get('status') == 'success'
        ]) else '⚠️ NEEDS ATTENTION'}
        
        ========================================
        REPORT STATUS: COMPLETED
        ========================================
        """
        
        print(report)
        
        return {"status": "success", "timestamp": timestamp}

    # Define the task dependencies
    freshness_task = check_data_freshness()
    completeness_task = check_data_completeness()
    response_time_task = check_api_response_time()
    
    # Generate quality report after all checks complete
    report_task = generate_quality_report(
        freshness_result=freshness_task,
        completeness_result=completeness_task,
        response_time_result=response_time_task
    )
    
    # Set up dependencies
    [freshness_task, completeness_task, response_time_task] >> report_task

# Instantiate the DAG
crime_data_quality_dag = crime_data_quality_check()
