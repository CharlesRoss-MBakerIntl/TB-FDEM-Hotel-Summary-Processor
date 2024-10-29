import requests

def fetch_function(url, token):
    
    # Create Header with Token
    headers = {'Authorization': f'token {token}'}
    
    #Attempt to Fetch File
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.text
        
        else:
            raise Exception("Error Submitting Request")
        
    except Exception as e:
        raise Exception(f"Failed to fetch function: {e}")