import yfinance as yf
import pandas as pd
from datetime import datetime
import os
import boto3
import logging
from botocore.exceptions import NoCredentialsError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_stock_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch stock data from yfinance with error handling."""
    try:
        logger.info(f"Fetching data for {ticker} from {start_date} to {end_date}")
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if data is None or data.empty:
            logger.warning(f"No data returned for {ticker}")
            return pd.DataFrame()
        return data
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {str(e)}")
        return pd.DataFrame()

def save_to_csv(data: pd.DataFrame, ticker: str, prefix: str = "latest") -> str:
    """Save data to CSV with timestamp."""
    if data.empty:
        raise ValueError("Cannot save empty DataFrame")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{ticker}_{prefix}_{timestamp}.csv"
    
    try:
        data.to_csv(filename)
        logger.info(f"Successfully saved data to {filename}")
        return filename
    except Exception as e:
        logger.error(f"Error saving CSV: {str(e)}")
        raise

def upload_to_s3(file_path: str, bucket: str, s3_folder: str = "latest") -> bool:
    """Upload file to S3 with versioning and error handling."""
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist")
        return False

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        # Extract filename without path
        file_name = os.path.basename(file_path)
        
        # Upload to latest folder
        latest_key = f"{s3_folder}/{file_name}"
        s3.upload_file(file_path, bucket, latest_key)
        logger.info(f"Uploaded to s3://{bucket}/{latest_key}")
        
        # Archive to historical with timestamp
        if s3_folder == "latest":
            timestamp = datetime.now().strftime('%Y%m%d')
            historical_key = f"historical/{timestamp}/{file_name}"
            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': latest_key},
                Key=historical_key
            )
            logger.info(f"Archived to s3://{bucket}/{historical_key}")
            
        return True
        
    except NoCredentialsError:
        logger.error("AWS credentials not available")
        return False
    except ClientError as e:
        logger.error(f"AWS client error: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return False

if __name__ == "__main__":
    # Configuration
    TICKER = "AAPL"
    START_DATE = "2023-01-01"
    END_DATE = "2023-12-31"
    BUCKET_NAME = os.getenv('AWS_S3_BUCKET', 'your-bucket-name')
    
    # Fetch data
    stock_data = fetch_stock_data(TICKER, START_DATE, END_DATE)
    
    if not stock_data.empty:
        # Save locally
        try:
            csv_file = save_to_csv(stock_data, TICKER, "latest")
            
            # Upload to S3
            if not upload_to_s3(csv_file, BUCKET_NAME):
                logger.error("Failed to upload to S3")
                exit(1)
                
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            exit(1)
    else:
        logger.error("No data available to process")
        exit(1)