import requests
import json
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor

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
    dag_id='crime_analysis_pipeline',
    default_args=default_args,
    description='Crime Analysis Pipeline - Runs daily at 3 PM EST',
    schedule_interval='0 15 * * *',  # 3 PM EST (15:00) daily
    catchup=False,
    tags=['crime', 'analysis', 'daily'],
    max_active_runs=1
)
def crime_analysis_pipeline():

    @task
    def check_api_health():
        """Check if the Spring Boot API is running"""
        try:
            response = requests.get('http://host.docker.internal:8080/api/simple-crimes/summary', timeout=10)
            if response.status_code == 200:
                print("✅ API is healthy and responsive")
                return True
            else:
                print(f"❌ API returned status code: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"❌ API health check failed: {e}")
            return False

    @task
    def run_summary_statistics():
        """Run summary statistics analysis"""
        try:
            print("🔄 Running summary statistics...")
            response = requests.get('http://host.docker.internal:8080/api/simple-crimes/summary', timeout=30)
            if response.status_code == 200:
                result = response.json()
                print("✅ Summary statistics completed successfully")
                print(f"Response: {result}")
                return {"status": "success", "data": result}
            else:
                print(f"❌ Summary statistics failed with status: {response.status_code}")
                return {"status": "failed", "error": response.text}
        except Exception as e:
            print(f"❌ Error running summary statistics: {e}")
            return {"status": "error", "error": str(e)}

    @task
    def run_top_locations_analysis():
        """Run top 3 locations analysis"""
        try:
            print("🔄 Running top locations analysis...")
            response = requests.get('http://host.docker.internal:8080/api/simple-crimes/top-locations', timeout=30)
            if response.status_code == 200:
                result = response.json()
                print("✅ Top locations analysis completed successfully")
                print(f"Top 3 locations: {result}")
                return {"status": "success", "data": result}
            else:
                print(f"❌ Top locations analysis failed with status: {response.status_code}")
                return {"status": "failed", "error": response.text}
        except Exception as e:
            print(f"❌ Error running top locations analysis: {e}")
            return {"status": "error", "error": str(e)}

    @task
    def run_common_weapons_analysis():
        """Run common weapons analysis"""
        try:
            print("🔄 Running common weapons analysis...")
            response = requests.get('http://host.docker.internal:8080/api/simple-crimes/common-weapons', timeout=30)
            if response.status_code == 200:
                result = response.json()
                print("✅ Common weapons analysis completed successfully")
                print(f"Top 5 weapons: {result}")
                return {"status": "success", "data": result}
            else:
                print(f"❌ Common weapons analysis failed with status: {response.status_code}")
                return {"status": "failed", "error": response.text}
        except Exception as e:
            print(f"❌ Error running common weapons analysis: {e}")
            return {"status": "error", "error": str(e)}

    @task
    def run_crime_types_analysis():
        """Run crime types analysis"""
        try:
            print("🔄 Running crime types analysis...")
            response = requests.get('http://host.docker.internal:8080/api/simple-crimes/crime-types', timeout=30)
            if response.status_code == 200:
                result = response.json()
                print("✅ Crime types analysis completed successfully")
                print(f"Top 10 crime types: {result}")
                return {"status": "success", "data": result}
            else:
                print(f"❌ Crime types analysis failed with status: {response.status_code}")
                return {"status": "failed", "error": response.text}
        except Exception as e:
            print(f"❌ Error running crime types analysis: {e}")
            return {"status": "error", "error": str(e)}

    @task
    def run_area_analysis():
        """Run crimes by area analysis"""
        try:
            print("🔄 Running crimes by area analysis...")
            response = requests.get('http://host.docker.internal:8080/api/simple-crimes/by-area', timeout=30)
            if response.status_code == 200:
                result = response.json()
                print("✅ Crimes by area analysis completed successfully")
                print(f"Areas: {result}")
                return {"status": "success", "data": result}
            else:
                print(f"❌ Crimes by area analysis failed with status: {response.status_code}")
                return {"status": "failed", "error": response.text}
        except Exception as e:
            print(f"❌ Error running crimes by area analysis: {e}")
            return {"status": "error", "error": str(e)}

    @task
    def generate_daily_report(summary_result, locations_result, weapons_result, crime_types_result, area_result):
        """Generate a consolidated daily report"""
        print("📊 Generating daily crime analysis report...")
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S EST")
        
        report = f"""
        ========================================
        CRIME ANALYSIS DAILY REPORT
        Generated: {timestamp}
        ========================================
        
        SUMMARY STATISTICS:
        {summary_result.get('data', 'N/A')}
        
        TOP 3 LOCATIONS:
        {locations_result.get('data', 'N/A')}
        
        TOP 5 WEAPONS:
        {weapons_result.get('data', 'N/A')}
        
        TOP 10 CRIME TYPES:
        {crime_types_result.get('data', 'N/A')}
        
        CRIMES BY AREA:
        {area_result.get('data', 'N/A')}
        
        ========================================
        REPORT STATUS: COMPLETED
        ========================================
        """
        
        print(report)
        
        # Save report to file
        with open(f"/opt/airflow/logs/crime_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt", "w") as f:
            f.write(report)
        
        return {"status": "success", "timestamp": timestamp, "report_generated": True}

    # Define the task dependencies
    health_check = check_api_health()
    
    # Run analyses in parallel after health check
    summary_task = run_summary_statistics()
    locations_task = run_top_locations_analysis()
    weapons_task = run_common_weapons_analysis()
    crime_types_task = run_crime_types_analysis()
    area_task = run_area_analysis()
    
    # Generate final report after all analyses complete
    report_task = generate_daily_report(
        summary_result=summary_task,
        locations_result=locations_task,
        weapons_result=weapons_task,
        crime_types_result=crime_types_task,
        area_result=area_task
    )
    
    # Set up dependencies
    health_check >> [summary_task, locations_task, weapons_task, crime_types_task, area_task] >> report_task

# Instantiate the DAG
crime_analysis_dag = crime_analysis_pipeline()
