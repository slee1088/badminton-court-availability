from google.cloud import storage
import io
import pandas as pd

from datetime import datetime

def get_day_of_week(date_str):
    """
    Returns the day of the week for a given date string (YYYY-MM-DD).
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        return days[date_obj.weekday()]
    except ValueError:
        return "Invalid date format. Please use YYYY-MM-DD."

def export_df_to_gcs_json(df, bucket_name, file_name):
  """Exports a pandas DataFrame to a JSON file in Google Cloud Storage.

  Args:
    df: The pandas DataFrame to export.
    bucket_name: The name of the GCS bucket.
    file_name: The name of the JSON file within the bucket.
  """

  # Create a client object for interacting with Google Cloud Storage
  storage_client = storage.Client()

  # Get a reference to the blob (file)
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(file_name)
  df = df.astype(str)
  # Convert the DataFrame to a JSON string
  json_data = df.to_json(orient='records')

  # Upload the JSON string to the blob
  print(df.dtypes)
  blob.upload_from_string(json_data, content_type='application/json')
  print("uploaded")

def upload_dataframe_to_gcs(bucket_name, df, destination_blob_name):
    """Uploads a Pandas DataFrame to Google Cloud Storage as a CSV."""

    try:
        # Convert DataFrame to CSV string in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')  # Important: Use UTF-8 encoding
        csv_content = csv_buffer.getvalue()

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(csv_content, content_type='text/csv')

        print(f"DataFrame uploaded to gs://{bucket_name}/{destination_blob_name}")
        return True # Return true if successfull
    except Exception as e:
        print(f"An error occurred: {e}")
        return False # Return false if there is an error

def read_csv_from_gcs(bucket_name, file_name):
  """Reads a CSV file from Google Cloud Storage and returns a pandas DataFrame.

  Args:
    bucket_name: The name of the GCS bucket.
    file_name: The name of the CSV file within the bucket.

  Returns:
    A pandas DataFrame containing the data from the CSV file.
  """

  # Create a client object for interacting with Google Cloud Storage
  storage_client = storage.Client()

  # Get a reference to the blob (file)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(file_name)

  # Download the file contents as a string
  contents = blob.download_as_string().decode('utf-8')

  # Create a pandas DataFrame from the string
  df = pd.read_csv(io.StringIO(contents))

  return df

# Example usage:
bucket_name = "badminton-bookings"  # Replace with your bucket name

alpha_slough = read_csv_from_gcs(bucket_name, "alpha_slough_bookings.csv")
alpha_auburn = read_csv_from_gcs(bucket_name, "alpha_auburn_bookings.csv")
alpha_egerton = read_csv_from_gcs(bucket_name, "alpha_egerton_bookings.csv")
roketto = read_csv_from_gcs(bucket_name, "roketto_bookings.csv")
nbc_silverwater = read_csv_from_gcs(bucket_name, "nbc_silverwater_bookings.csv")
kbc_rydalmere =  read_csv_from_gcs(bucket_name, "kbc_rydalmere_bookings.csv")

all_combined = pd.concat([alpha_slough, alpha_auburn, alpha_egerton, roketto,nbc_silverwater,kbc_rydalmere], axis=0, ignore_index=True) 
all_combined['Day'] = all_combined['Date'].apply(get_day_of_week)

nbc_filter = (all_combined['Location'] == 'NBC Silverwater') & \
       (~all_combined['Day'].isin(['Saturday', 'Sunday'])) & \
       (all_combined['Time'].isin(["7:00am", "8:00am", "9:00am"]))
       
nbc_filter_2 = (all_combined['Location'] == 'NBC Silverwater') & \
       (~all_combined['Day'].isin(['Friday'])) & \
       (all_combined['Time'].isin(["10:00pm"]))
       
kbc_filter = (all_combined['Location'] == 'KBC Rydalmere') & \
       (~all_combined['Day'].isin(['Saturday', 'Sunday'])) & \
       (all_combined['Time'].isin(["7:00am", "8:00am", "9:00am"]))
       
kbc_filter_2 = (all_combined['Location'] == 'KBC Rydalmere') & \
       (~all_combined['Day'].isin(['Saturday'])) & \
       (all_combined['Time'].isin(["7:00am"]))

# Set 'Status' to 'Unavailable' where conditions are met
all_combined.loc[nbc_filter, 'Status'] = 'unavailable'
all_combined.loc[nbc_filter_2, 'Status'] = 'unavailable'
all_combined.loc[kbc_filter, 'Status'] = 'unavailable'
all_combined.loc[kbc_filter_2, 'Status'] = 'unavailable'


bucket_name = "badminton-bookings"
blob_name = "all_bookings.csv" 

if upload_dataframe_to_gcs(bucket_name, all_combined, blob_name):
    print("Upload successful (CSV)")
else:
    print("Upload failed (CSV)")

blob_name = "all_bookings.json"  # Replace with the desired file name

if export_df_to_gcs_json(all_combined, bucket_name, blob_name):
    print("Upload successful (JSON)")
else:
    print("Upload failed (JSON)")

