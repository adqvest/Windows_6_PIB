#Created By Induja - Modified for Microsoft Graph API
import warnings
warnings.filterwarnings('ignore')
import os
import base64
import requests
import pandas as pd
import datetime as datetime
from msal import ConfidentialClientApplication
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

properties = pd.read_csv('adqvest_email_sender_properties.txt', delim_whitespace=True)
TENANT_ID = list(properties.loc[properties['Item'] == 'TENANT_ID'].iloc[:,1])[0]
CLIENT_ID = list(properties.loc[properties['Item'] == 'CLIENT_ID'].iloc[:,1])[0]
CLIENT_SECRET = list(properties.loc[properties['Item'] == 'CLIENT_SECRET'].iloc[:,1])[0]
SENDER_EMAIL = list(properties.loc[properties['Item'] == 'SENDER_EMAIL'].iloc[:,1])[0]
# Microsoft Graph API endpoints
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]
GRAPH_ENDPOINT = "https://graph.microsoft.com/v1.0"

class adqvestemailsender:
    def __init__(self):
        self.app = ConfidentialClientApplication(
            CLIENT_ID,
            authority=AUTHORITY,
            client_credential=CLIENT_SECRET
        )
        self.access_token = None
    
    def get_access_token(self):
        """Get access token for Microsoft Graph API"""
        try:
            result = self.app.acquire_token_silent(SCOPE, account=None)
            if not result:
                result = self.app.acquire_token_for_client(scopes=SCOPE)
            
            if "access_token" in result:
                self.access_token = result["access_token"]
                return True
            else:
                print(f"Error getting access token: {result.get('error_description', 'Unknown error')}")
                return False
        except Exception as e:
            print(f"Exception getting access token: {str(e)}")
            return False
    
    def create_email_message(self, to_emails, cc_emails, subject, body_parts,attachments=None):
        """Create email message in Microsoft Graph format"""
        # Parse email addresses
        to_recipients = [{"emailAddress": {"address": email.strip()}} for email in to_emails.split(",")]
        cc_recipients = [{"emailAddress": {"address": email.strip()}} for email in cc_emails.split(",")]
        
        # Build message body with all parts
        html_content = ""
        for part in body_parts:
            if part['type'] == 'plain':
                # Convert plain text to HTML
                html_content += f"<div><pre>{part['content']}</pre></div><br>"
            elif part['type'] == 'html':
                html_content += f"<div>{part['content']}</div><br>"
        
        message = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": html_content
                },
                "toRecipients": to_recipients,
                "ccRecipients": cc_recipients,
                "from": {
                    "emailAddress": {
                        "address": SENDER_EMAIL
                    }
                }
            }
        }
        # Add attachments if provided
        if attachments:
            graph_attachments = []
            for file_path in attachments:
                if os.path.exists(file_path):
                    with open(file_path, "rb") as f:
                        encoded_content = base64.b64encode(f.read()).decode('utf-8')
                        graph_attachments.append({
                              "@odata.type": "#microsoft.graph.fileAttachment",
                              "name": os.path.basename(file_path),
                              "contentBytes": encoded_content,
                              "contentType": "application/octet-stream"})
                else:
                    print(f"Attachment not found: {file_path}")
            if graph_attachments:
                message["message"]["attachments"] = graph_attachments

        return message
    
    def send_email(self, message):
        """Send email using Microsoft Graph API"""
        if not self.access_token:
            if not self.get_access_token():
                return False
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        # Use sendMail endpoint
        url = f"{GRAPH_ENDPOINT}/users/{SENDER_EMAIL}/sendMail"
        
        try:
            response = requests.post(url, headers=headers, json=message)
            if response.status_code == 202:  # Success
                print("Email sent successfully")
                return True
            else:
                print(f"Error sending email: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"Exception sending email: {str(e)}")
            return False
