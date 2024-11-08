import asyncio
import aiohttp
import json
import csv
import os
from fake_useragent import UserAgent
import random

# Define the base API URL
base_api_url = "https://api.aia.org/firm-directory?filter%5Bcountry%5D=&filter%5Bstate%5D=&page%5Bnumber]={}&page%5Bsize%5D=50&q=&sort%5Bcriteria%5D=firm_name&sort%5Border%5D=asc"

# Initialize UserAgent
ua = UserAgent()

# Create the output directory if it doesn't exist
output_dir = "E:\\UpWork\\classic.aia\\full_firms"
os.makedirs(output_dir, exist_ok=True)

async def fetch_firms(session, page_number):
    api_url = base_api_url.format(page_number)
    headers = {
        "User-Agent": ua.random,  # Use a random user agent from fake-useragent
        "X-Fake-Location": "Fake City, Fake State"  # Add a fake location header
    }
    
    async with session.get(api_url, headers=headers) as response:
        if response.status == 200:
            return await response.json()
        elif response.status == 400:
            print(f"Bad request for page {page_number}: {response.status}. Retrying in 1 minute...")
            await asyncio.sleep(60)  # Sleep for 1 minute before retrying
            return await fetch_firms(session, page_number)  # Retry the request
        else:
            print(f"Failed to retrieve data for page {page_number}: {response.status}")
            return None

async def main():
    page_number = 446  # Start from page 45
    tasks = []

    async with aiohttp.ClientSession() as session:
        while True:
            # Create a task for the current page
            tasks.append(fetch_firms(session, page_number))
            page_number += 1
            
            # If we have 5 tasks, or if we are at the last page, gather them
            if len(tasks) == 5:  # Adjust the number of concurrent requests as needed
                responses = await asyncio.gather(*tasks)
                tasks = []  # Reset tasks for the next batch

                for data in responses:
                    if data and 'data' in data and data['data']:
                        firms_list = []
                        for firm in data['data']:
                            attributes = firm['attributes']
                            firm_info = {
                                "Firm Name": attributes['firm_name'],
                                "Address Line 1": attributes['address_line_1'],
                                "City": attributes['city'],
                                "State": attributes['state'],
                                "Zip Code": attributes['zip'],
                                "Country": attributes['country'],
                                "Firm URL": attributes['firm_url']
                            }
                            firms_list.append(firm_info)
                        
                        # Write the list of firms to a CSV file
                        with open(os.path.join(output_dir, f'full_firms{page_number-5}.csv'), 'w', newline='', encoding='utf-8') as csv_file:
                            fieldnames = firms_list[0].keys()  # Get the field names from the first dictionary
                            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                            writer.writeheader()  # Write the header
                            writer.writerows(firms_list)  # Write the data

                        print(f"Data for page {page_number-5} has been written to full_firms{page_number-5}.csv")
                    else:
                        break  # Exit if no more data is available

        # Gather any remaining tasks
        if tasks:
            responses = await asyncio.gather(*tasks)
            for data in responses:
                if data and 'data' in data and data['data']:
                    firms_list = []
                    for firm in data['data']:
                        attributes = firm['attributes']
                        firm_info = {
                            "Firm Name": attributes['firm_name'],
                            "Address Line 1": attributes['address_line_1'],
                            "City": attributes['city'],
                            "State": attributes['state'],
                            "Zip Code": attributes['zip'],
                            "Country": attributes['country'],
                            "Firm URL": attributes['firm_url']
                        }
                        firms_list.append(firm_info)
                    
                    # Write the list of firms to a CSV file
                    with open(os.path.join(output_dir, f'full_firms{page_number-len(tasks)+1}.csv'), 'w', newline='') as csv_file:
                        fieldnames = firms_list[0].keys()  # Get the field names from the first dictionary
                        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                        writer.writeheader()  # Write the header
                        writer.writerows(firms_list)  # Write the data

                    print(f"Data for page {page_number-len(tasks)+1} has been written to full_firms{page_number-len(tasks)+1}.csv")

    print("All data has been written to CSV files.")

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())

