import os
import requests
import gzip
import shutil


# Unformatted URLs
# Paste your raw URLs as a multiline string
raw_urls = """
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD2.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD3.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD4.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD5.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD6.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD7.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD8.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD9.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD10.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD11.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD12.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD13.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD14.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD15.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.OSD16.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.CTD.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.CTD2.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.CTD3.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.CTD4.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.CTD5.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.CTD6.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.CTD7.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT2.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT3.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT4.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT5.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT6.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT7.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT8.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT9.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT10.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT11.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT12.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT13.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT14.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.XBT15.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT2.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT3.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT4.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT5.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT6.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT7.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT8.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT9.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT10.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT11.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MBT12.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL2.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL3.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL4.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL5.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL6.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL7.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL8.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL9.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL10.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL11.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL12.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL13.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL14.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL15.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL16.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL17.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL18.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL19.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL20.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL21.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL22.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL23.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL24.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL25.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL26.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL27.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.PFL28.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.DRB.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.DRB2.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MRB.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MRB2.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MRB3.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MRB4.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MRB5.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MRB6.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MRB7.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.MRB8.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB2.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB3.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB4.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB5.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB6.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB7.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB8.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB9.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB10.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB11.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.APB12.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.UOR.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.SUR.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD2.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD3.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD4.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD5.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD6.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD7.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD8.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD9.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD10.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD11.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD12.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD13.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD14.csv.gz
 https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1751577167.2888210.GLD15.csv.gz
""".strip().splitlines()


# Clean and filter
urls = [url.strip() for url in raw_urls if url.strip()]

# Download directory
download_dir = "/Users/calebbousman/Documents/Git/Ocean_Studies/sources"
os.makedirs(download_dir, exist_ok=True)

# Download and unzip
for url in urls:
    gz_filename = url.split("/")[-1]
    csv_filename = gz_filename.replace(".gz", "")
    gz_path = os.path.join(download_dir, gz_filename)
    csv_path = os.path.join(download_dir, csv_filename)

    print(f"⬇️ Downloading {gz_filename}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Save .gz file
        with open(gz_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✔ Downloaded: {gz_filename}")

        # Unzip to .csv
        print(f"🗜️  Unzipping to {csv_filename}...")
        with gzip.open(gz_path, 'rb') as f_in, open(csv_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        print(f"✔ Unzipped to: {csv_filename}")

        # OPTIONAL: delete the .gz file to save space
        os.remove(gz_path)

    except Exception as e:
        print(f"✘ Failed to process {url}: {e}")

print("\n✅ All downloads and extractions attempted.")