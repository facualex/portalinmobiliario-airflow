from datetime import datetime
import pandas as pd
import os

# If you don't provide a date, today's date will be used.
def get_uf_value(date = datetime.now().strftime('%d-%m-%Y')):
    import requests

    try:
        bool(datetime.strptime(date, '%d-%m-%Y'))
    except ValueError:
        print("The provided date must be in the DD-MM-YYYY format")
        return False

    # API URL
    api_url = f'https://mindicador.cl/api/uf/{date}'

    # Send GET request to the API
    response = requests.get(api_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        data = response.json()

        result = data['serie'][0]

        if(result and result['valor']):
            return result['valor']
        else:
            print("No UF value found for the provided date")
            return
    else:
        print(f'Failed to retrieve data from the API with status code {response.status_code}')
        return

def update_uf_csv(csv_file_path, new_data_date = datetime.now().strftime('%d-%m-%Y')):
    new_row = pd.DataFrame([{
        "date": new_data_date,
        "value": get_uf_value(date = new_data_date),
    }])

    if not os.path.exists(csv_file_path):
        # File doesn't exist, create a new DataFrame
        uf_csv_df = pd.DataFrame(columns=['date', 'value']) 
    else:
        # Get current data CSV (if exists)
        uf_csv_df = pd.read_csv(csv_file_path)
    
    # Check if the value for the date is not already in the file
    existing_value = uf_csv_df[uf_csv_df['date']==new_data_date].values

    if(existing_value.size > 0):
        print("The UF value for that date already exists in the csv. Nothing was added.")
        return False

    # Concat the new row to the csv file
    uf_csv_df = pd.concat([uf_csv_df, new_row], ignore_index=True)

    # Save csv with the new data
    uf_csv_df.to_csv(csv_file_path, index=False)