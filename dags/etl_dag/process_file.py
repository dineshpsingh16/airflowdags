# /usr/local/airflow/dags/process_file.py

import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import os
from airflow import secrets
from airflow.configuration import conf
# from reportlab.pdfgen import canvas  # Install reportlab library: pip install reportlab
dags_folder = conf.get('core', 'dags_folder')
# def generate_pdf_report(data,reportpath):
#     """
#     Generates a simple PDF report using reportlab
#     You can customize this function to create your desired report layout
#     """

#     # Create a new PDF document
#     pdf = canvas.Canvas(reportpath)

#     # Set title and other formatting options
#     pdf.setFont("Helvetica", 16)
#     pdf.drawString(100, 700, "Processed CSV Data Report")

#     # Add table headers
#     pdf.drawString(50, 650, "Column 1")
#     pdf.drawString(250, 650, "Column 2")
#     # ... (add more headers if needed)

#     # Write data from DataFrame
#     y_pos = 600
#     for index, row in data.iterrows():
#         pdf.drawString(50, y_pos, str(row[0]))  # Access first column data
#         pdf.drawString(250, y_pos, str(row[1]))  # Access second column data
#         # ... (add more data access for other columns)
#         y_pos -= 20

#     # Save the PDF document
#     pdf.save()

def process_csv():
    input_path = f"{dags_folder}/data/etl_file_input.csv"
    output_path = f"{dags_folder}/data/etl_file_output.csv"
    # reportpath = '/usr/local/airflow/dags/data/etl_file_output.csv'
    # Load the CSV file
    df = pd.read_csv(input_path)
    
    # Here you can add any processing to the dataframe if needed
    # For now, we just save it to the output file
    
    # Save the dataframe to a new CSV file
    df.to_csv(output_path, index=False)
    # generate_pdf_report(df.copy(),reportpath) 
    # Send email with attachment
    send_email_with_attachment(output_path)
    os.remove(input_path)
    os.remove(output_path)
    # os.remove(reportpath)
def send_email_with_attachment(file_path):
    sender_email = "dineshpsingh@yahoo.com"  # Replace with your Yahoo email
    receiver_email = "dineshpsingh16@gmail.com"
    subject = "Processed CSV File"
    body = "Please find the attached processed CSV file."
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    
    
    try:
        server = smtplib.SMTP_SSL('smtp.mail.yahoo.com', 465)
        yahoo_app_password = secrets.get_variable(key="YAHOO_APP_PASSWORD")

        server.login(sender_email, yahoo_app_password)  # Replace with generated App Password
        text = msg.as_string()
        # Attach CSV file
        with open(file_path, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(file_path)}")
            msg.attach(part)        
        server.sendmail(sender_email, receiver_email, text)
        server.quit()
    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    process_csv()
