# """To download data, fill out the requested fields, and run this script using the command:

# python run.py

# """

from src.awards import AwardSearchDownload


tas_codes = []
for year in [2024, 2023, 2022, 2021, 2020, 2019]:
    tas_codes.append(f"072-019-{year}/{year+1}-1031-000")
for year in [2024, 2023, 2022, 2021]:
    tas_codes.append(f"072-019-{year}/{year+4}-1031-000")
for year in [2024, 2023, 2022, 2021, 2020]:
    tas_codes.append(f"072-019-{year}/{year+5}-1031-000")
for year in [2022, 2021, 2020, 2019, 2018]:
    tas_codes.append(f"072-{year}/{year+1}-1021-000")
    tas_codes.append(f"072-{year}/{year+5}-1021-000")

for tas_code in tas_codes:

    print(f"Downloading data for TAS {tas_code}")

    downloader = AwardSearchDownload(
        tas_code = tas_code,
    )  # an object that can download the data you want from usaspending

    downloader.run_all()