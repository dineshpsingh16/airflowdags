# util/tasks.py


def task1_fun(csv_data_json):
    import pandas as pd

    # Convert JSON back to DataFrame
    df = pd.read_json(csv_data_json)
    
    # Initialize balance column
    df['balance'] = 0.0
    
    # Calculate balance based on debit and credit columns
    balance = 0.0
    for index, row in df.iterrows():
        if pd.notnull(row['debit']):
            balance -= row['debit']
        if pd.notnull(row['credit']):
            balance += row['credit']
        df.at[index, 'balance'] = balance
    
    # Print for debugging
    print("Updated DataFrame with balance column:")
    print(df)
    
    # Return updated DataFrame as JSON
    return df.to_json()
