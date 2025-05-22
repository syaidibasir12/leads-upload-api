# Recruitment Leads Automation Tool

import certifi
import os
import requests
import pandas as pd
import json
from datetime import datetime

# === Environment Setup ===
CS_API_KEY = os.getenv('CS_API_KEY')  

# === File Paths (Update these as needed) ===
BASE_DIR = os.getenv('RECRUITMENT_DATA_DIR', './data')
OUTPUT_DIR = os.getenv('RECRUITMENT_OUTPUT_DIR', './output')

REGION_MAP_PATH = os.path.join(BASE_DIR, "Zoom & CS.xlsx")
INDEED_PATH = os.path.join(BASE_DIR, "Indeed.csv")
JOBSTREET_PATH = os.path.join(BASE_DIR, "Jobstreet.xlsx")

# === Read Data ===
df_region = pd.read_excel(REGION_MAP_PATH, sheet_name="Region Mapping CS")
dataset = pd.read_excel(REGION_MAP_PATH, sheet_name="Initiative ID")

jobstreet = pd.read_excel(JOBSTREET_PATH)
indeed = pd.read_csv(INDEED_PATH)

# === Clean Indeed Data ===
indeed = pd.merge(indeed, df_region, left_on='job location', right_on='Posted Region', how='left')
indeed['Clean Region'] = indeed.apply(
    lambda row: 'Telemarketing' if 'Telemarketing' in row['job title'] else row['Clean Region'],
    axis=1
)
indeed = indeed[indeed['Clean Region'].notnull()]

include = [
    'Brand Ambassador', 'Communication Executive', 'Event Ambassador', 'Sales & Marketing Executive',
    'Sales Executive', 'Sales Management Trainee', 'Event Sales Consultant',
    'Senior Sales Associate', 'Marketing Campaign', 'Telemarketing Executive'
]
pattern = '|'.join(include)
indeed = indeed[indeed['job title'].str.contains(pattern, case=False, na=False)]

indeed['index-key'] = indeed['name'] + indeed['date'] + indeed['job title'] + indeed['email']
indeed.drop_duplicates(subset='index-key', keep='first', inplace=True)

# === Helper Functions ===
def clean_phone_number(phone):
    phone = ''.join(c for c in str(phone) if c.isdigit())
    if phone.startswith('0'):
        return '60' + phone[1:]
    elif not phone.startswith('60'):
        return '60' + phone
    return phone

def convert_date(date_str):
    date = pd.to_datetime(date_str)
    return date.strftime('%Y-%m-%d')

# === Apply Cleaning ===
indeed['phone_number'] = indeed['phone'].apply(clean_phone_number)
indeed = indeed[["name", "phone_number", "job title", "Clean Region", "date"]]
indeed = indeed.rename(columns={
    'name': 'applicant_name',
    'job title': 'ads_role',
    'date': 'applied_date',
    'Clean Region': 'region'
})
indeed['source_platform'] = "Indeed"

# === Clean Jobstreet Data ===
jobstreet['phone_number'] = jobstreet['Phone Number'].apply(clean_phone_number)
jobstreet['Date Applied'] = jobstreet['Date Applied'].apply(convert_date)
jobstreet = jobstreet.rename(columns={
    'Name': 'applicant_name',
    'Email': 'applied_email',
    'Job Title': 'ads_role',
    'Date Applied': 'applied_date',
    'Region': 'region'
})
jobstreet['index-key'] = jobstreet['applicant_name'] + jobstreet['applied_date'] + jobstreet['ads_role'] + jobstreet['applied_email']
jobstreet.drop_duplicates(subset='index-key', keep='first', inplace=True)
jobstreet = jobstreet[['applicant_name', 'applied_email', 'ads_role', 'region', 'phone_number', 'applied_date']]
jobstreet['source_platform'] = "Jobstreet"

# === Merge and Final Prep ===
fresh_leads = pd.concat([jobstreet, indeed], ignore_index=True)
fresh_leads['upload_date'] = datetime.today().strftime('%Y-%m-%d')
fresh_leads['clean_role'] = "F2F Fundraiser"
fresh_leads['omni_platform'] = 'Contactspace'

today = datetime.today().strftime('%Y-%m-%d')

# === Export Local Copies ===
os.makedirs(OUTPUT_DIR, exist_ok=True)
fresh_leads.to_excel(os.path.join(OUTPUT_DIR, "Data for Today.xlsx"), index=False)

melaka_leads = fresh_leads[fresh_leads["region"].isin(["Melaka", "Seremban"])]
fresh_leads = fresh_leads[~fresh_leads["region"].isin(["Melaka", "Seremban"])]  # Filter out for now

melaka_leads.to_excel(os.path.join(OUTPUT_DIR, f"Melaka Leads {today}.xlsx"), index=False)

print(f"Filtered fresh leads count: {len(fresh_leads)}")

# === Upload to ContactSpace ===
fresh_leads = fresh_leads.merge(dataset[['region', 'Dataset ID']], on='region', how='left')
data_dict = fresh_leads.to_dict(orient='records')

for signup in data_dict:
    dataset_id = signup.get('Dataset ID')
    if not dataset_id:
        continue  # Skip if dataset ID is missing

    filtered_signup = {k: v for k, v in signup.items() if k != 'Dataset ID' and pd.notna(v) and v != ""}
    json_string = json.dumps(filtered_signup, ensure_ascii=False)

    url = 'https://apithunder.makecontact.space/InsertRecord'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'x-api-key': CS_API_KEY
    }
    data = {
        'datasetid': dataset_id,
        'jsondata': json_string
    }

    response = requests.post(url, headers=headers, data=data, verify=certifi.where())

    if response.status_code == 200:
        print(f"✅ Uploaded: {filtered_signup['region']}")
    else:
        print(f"❌ Failed upload: {filtered_signup['region']} — {response.text}")

print("🎉 Upload complete.")
