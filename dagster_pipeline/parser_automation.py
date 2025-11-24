import boto3
import json
import os
from dagster import (
    asset, 
    AssetExecutionContext, 
    define_asset_job,
    ScheduleDefinition,
    Definitions
)
from datetime import datetime

AWS_ACCOUNT_ID = '992382507941'
AWS_REGION = 'us-east-1'
S3_BUCKET = 'scrapers-resturantlambda'

try:
    lambda_client = boto3.client('lambda', region_name=AWS_REGION)
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    print("AWS clients initialized successfully")
except Exception as e:
    print(f"Warning: Could not initialize AWS clients: {e}")


def invoke_lambda(function_name, payload):
    try:
        print(f"\nInvoking Lambda: {function_name}")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        result = json.loads(response['Payload'].read())
        
        print(f"Lambda {function_name} completed")
        print(f"Result: {json.dumps(result, indent=2)}\n")
        
        return result
        
    except Exception as e:
        error_msg = f"Error invoking {function_name}: {str(e)}"
        print(error_msg)
        return {"error": error_msg, "success": False}


def check_s3_files_exist(prefix, min_files=1):
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix,
            MaxKeys=min_files + 1
        )
        
        file_count = response.get('KeyCount', 0)
        exists = file_count >= min_files
        
        status = 'FOUND' if exists else 'NOT FOUND'
        print(f"S3 Check: {prefix} - {file_count} files - {status}")
        
        return exists
        
    except Exception as e:
        print(f"Error checking S3: {e}")
        return False


def send_notification(subject, message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print("\n" + "="*70)
    print("NOTIFICATION")
    print("="*70)
    print(f"Time: {timestamp}")
    print(f"Subject: {subject}")
    print(f"\n{message}")
    print("="*70 + "\n")


@asset(
    group_name="metro",
    description="Step 1: Fetch all Metro store locations from API"
)
def metro_location(context: AssetExecutionContext):
    context.log.info("Starting Metro Location scraping")
    
    result = invoke_lambda(
        function_name='scrapers',
        payload={
            "parser": "metro",
            "action": "process.location",
            "use_proxy": False,
            "page_size": 100,
            "offset": 0
        }
    )
    
    if check_s3_files_exist('location/', min_files=1):
        context.log.info("Metro location files verified in S3")
    else:
        context.log.warning("No files found in location/ folder")
    
    return result


@asset(
    group_name="metro",
    deps=[metro_location],
    description="Step 2: Fetch Metro product categories for each store"
)
def metro_menu(context: AssetExecutionContext, metro_location):
    context.log.info("Starting Metro Menu scraping")
    context.log.info("This depends on location data being available")
    
    result = invoke_lambda(
        function_name='scrapers',
        payload={
            "parser": "metro",
            "action": "process.menu",
            "use_proxy": False,
            "page_size": 100,
            "offset": 0,
            "force_fetch": False,
            "goto_next_step": True
        }
    )
    
    if check_s3_files_exist('menu/', min_files=1):
        context.log.info("Metro menu files verified in S3")
    
    return result


@asset(
    group_name="metro",
    deps=[metro_menu],
    description="Step 3: Fetch all Metro products for each category"
)
def metro_post_menu(context: AssetExecutionContext, metro_menu):
    context.log.info("Starting Metro Post-Menu (products) scraping")
    context.log.warning("This step may take 10-30 minutes depending on product count")
    
    result = invoke_lambda(
        function_name='scrapers',
        payload={
            "parser": "metro",
            "action": "process.post.menu",
            "use_proxy": False,
            "page_size": 100,
            "offset": 0
        }
    )
    
    if check_s3_files_exist('post-menu/', min_files=1):
        context.log.info("Metro product files verified in S3")
    
    return result


@asset(
    group_name="metro",
    deps=[metro_post_menu],
    description="Step 4: Convert Metro JSON data to CSV format"
)
def metro_csv(context: AssetExecutionContext, metro_post_menu):
    context.log.info("Starting Metro CSV generation")
    
    result = invoke_lambda(
        function_name='scrapers',
        payload={
            "parser": "metro",
            "action": "make.csv",
            "use_proxy": False,
            "page_size": 100,
            "offset": 0
        }
    )
    
    if check_s3_files_exist('result/', min_files=1):
        context.log.info("Metro CSV files verified in S3")
        
        send_notification(
            subject="Metro Parser Completed Successfully",
            message=f"""
Metro parsing pipeline has finished all steps:

Step 1: Locations scraped (process.location)
Step 2: Categories fetched (process.menu)
Step 3: Products downloaded (process.post.menu)
Step 4: CSV generated (make.csv)

Files are available in S3 bucket: {S3_BUCKET}

Result Summary:
{json.dumps(result, indent=2)}

Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        )
    
    return result


@asset(
    group_name="imtiaz",
    description="Step 1: Fetch all Imtiaz store locations from API"
)
def imtiaz_location(context: AssetExecutionContext):
    context.log.info("Starting Imtiaz Location scraping")
    
    result = invoke_lambda(
        function_name='scrapers',
        payload={
            "parser": "imtiaz",
            "action": "process.location",
            "use_proxy": False,
            "page_size": 100,
            "offset": 0
        }
    )
    
    if check_s3_files_exist('location/', min_files=1):
        context.log.info("Imtiaz location files verified in S3")
    
    return result


@asset(
    group_name="imtiaz",
    deps=[imtiaz_location],
    description="Step 2: Fetch Imtiaz product categories"
)
def imtiaz_menu(context: AssetExecutionContext, imtiaz_location):
    context.log.info("Starting Imtiaz Menu scraping")
    
    result = invoke_lambda(
        function_name='scrapers',
        payload={
            "parser": "imtiaz",
            "action": "process.menu",
            "use_proxy": False,
            "page_size": 100,
            "offset": 0
        }
    )
    
    if check_s3_files_exist('menu/', min_files=1):
        context.log.info("Imtiaz menu files verified in S3")
    
    return result


@asset(
    group_name="imtiaz",
    deps=[imtiaz_menu],
    description="Step 3: Fetch all Imtiaz products"
)
def imtiaz_post_menu(context: AssetExecutionContext, imtiaz_menu):
    context.log.info("Starting Imtiaz Post-Menu (products) scraping")
    
    result = invoke_lambda(
        function_name='scrapers',
        payload={
            "parser": "imtiaz",
            "action": "process.post.menu",
            "use_proxy": False,
            "page_size": 100,
            "offset": 0
        }
    )
    
    if check_s3_files_exist('post-menu/', min_files=1):
        context.log.info("Imtiaz product files verified in S3")
    
    return result


@asset(
    group_name="imtiaz",
    deps=[imtiaz_post_menu],
    description="Step 4: Convert Imtiaz JSON to CSV"
)
def imtiaz_csv(context: AssetExecutionContext, imtiaz_post_menu):
    context.log.info("Starting Imtiaz CSV generation")
    
    result = invoke_lambda(
        function_name='scrapers',
        payload={
            "parser": "imtiaz",
            "action": "make.csv",
            "use_proxy": False,
            "page_size": 100,
            "offset": 0
        }
    )
    
    if check_s3_files_exist('result/', min_files=1):
        context.log.info("Imtiaz CSV files verified in S3")
        
        send_notification(
            subject="Imtiaz Parser Completed Successfully",
            message=f"""
Imtiaz parsing pipeline has finished all steps:

Step 1: Locations scraped (process.location)
Step 2: Categories fetched (process.menu)
Step 3: Products downloaded (process.post.menu)
Step 4: CSV generated (make.csv)

Files are available in S3 bucket: {S3_BUCKET}

Result Summary:
{json.dumps(result, indent=2)}

Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        )
    
    return result


metro_job = define_asset_job(
    name="metro_parser_job",
    description="Run complete Metro parsing pipeline (Location → Menu → Products → CSV)",
    selection=["metro_location", "metro_menu", "metro_post_menu", "metro_csv"]
)

imtiaz_job = define_asset_job(
    name="imtiaz_parser_job",
    description="Run complete Imtiaz parsing pipeline (Location → Menu → Products → CSV)",
    selection=["imtiaz_location", "imtiaz_menu", "imtiaz_post_menu", "imtiaz_csv"]
)

all_parsers_job = define_asset_job(
    name="all_parsers_job",
    description="Run both Metro and Imtiaz parsers in parallel",
    selection="*"
)

metro_daily_schedule = ScheduleDefinition(
    job=metro_job,
    cron_schedule="0 2 * * *",
    name="metro_daily_2am",
    description="Runs Metro parser every day at 2:00 AM"
)

imtiaz_daily_schedule = ScheduleDefinition(
    job=imtiaz_job,
    cron_schedule="0 2 * * *",
    name="imtiaz_daily_2am",
    description="Runs Imtiaz parser every day at 2:00 AM"
)

defs = Definitions(
    assets=[
        metro_location,
        metro_menu,
        metro_post_menu,
        metro_csv,
        imtiaz_location,
        imtiaz_menu,
        imtiaz_post_menu,
        imtiaz_csv
    ],
    jobs=[
        metro_job,
        imtiaz_job,
        all_parsers_job
    ],
    schedules=[
        metro_daily_schedule,
        imtiaz_daily_schedule,
    ]
)

if __name__ == "__main__":
    print("\n" + "="*70)
    print("AWS CONNECTION TEST")
    print("="*70 + "\n")
    
    try:
        sts = boto3.client('sts', region_name=AWS_REGION)
        identity = sts.get_caller_identity()
        print("AWS Credentials Valid")
        print(f"   Account: {identity['Account']}")
        print(f"   User ARN: {identity['Arn']}")
        print(f"   Region: {AWS_REGION}")
    except Exception as e:
        print(f"AWS Credentials Error: {e}")
    
    print("\n" + "="*70)
    print("S3 BUCKET ACCESS TEST")
    print("="*70 + "\n")
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=1)
        print(f"S3 Bucket Access Successful")
        print(f"   Bucket: {S3_BUCKET}")
        print(f"   Region: {AWS_REGION}")
    except Exception as e:
        print(f"S3 Access Error: {e}")
    
    print("\n" + "="*70)
    print("LAMBDA FUNCTION TEST")
    print("="*70 + "\n")
    try:
        lambda_list = lambda_client.list_functions(MaxItems=5)
        functions = lambda_list.get('Functions', [])
        print(f"Lambda Access Successful")
        print(f"   Found {len(functions)} functions")
        
        if functions:
            print("\n   Available Lambda functions:")
            for func in functions:
                print(f"   - {func['FunctionName']}")
    except Exception as e:
        print(f"Lambda Access Error: {e}")
    
    print("\n" + "="*70)
    print("DAGSTER PIPELINE STATUS")
    print("="*70)
    print("\nPipeline configured successfully")
    print(f"\nS3 Bucket: {S3_BUCKET}")
    print(f"AWS Region: {AWS_REGION}")
    print(f"AWS Account: {AWS_ACCOUNT_ID}")
    
    print("\nAvailable Jobs:")
    print("   1. metro_parser_job - Run Metro parser")
    print("      → process.location → process.menu → process.post.menu → make.csv")
    print("   2. imtiaz_parser_job - Run Imtiaz parser")
    print("      → process.location → process.menu → process.post.menu → make.csv")
    print("   3. all_parsers_job - Run both parsers in parallel")
    
    print("\nConfigured Schedules:")
    print("   - metro_daily_2am: Metro parser at 2:00 AM daily")
    print("   - imtiaz_daily_2am: Imtiaz parser at 2:00 AM daily")
    print("   - Both run in PARALLEL (at same time)")
    
    print("\nNext Steps:")
    print("   1. Start Dagster UI: dagster dev -f parser_automation.py")
    print("   2. Open browser: http://localhost:3000")
    print("   3. Test manually first (Assets → Materialize)")
    print("   4. Enable schedules (Automation → Schedules → Toggle ON)")
    
    print("\n" + "="*70 + "\n")