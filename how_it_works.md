# Methods

The code to download this data can be found at: https://github.com/TBFighterTofu/usaspending	
The main class that we use to handle this data is AwardSearchDownload, which is defined in src/awards.py

Let’s walk through the process to download that data for the TAS code **072-019-2023/2024-1031-000**

To download the data for a single TAS code, we first instantiate an AwardSearchDownload object for that TAS code.

`downloader = AwardSearchDownload(tas_code = “072-019-2023/2024-1031-000”)`

Instantiating this object creates the folder `data/summaries/072-019-2023_2024-1031-000`

Then, we download all our data:

`downloader.run_all()`

The function run_all runs five functions:

## 1. download_program_activity()

The USASpending API has an endpoint where we can look up a summary of spending for a specific TAS code for a specific fiscal year. For each year from 2017 to now, we call the API, and we combine all of the responses into one json file, keyed by fiscal year. Effectively, what it's doing is:

```
    results = {}
    results[2017] = requests.get("https://api.usaspending.gov/api/v2/agency/treasury_account/072-019-2023/2024-1031-000/program_activity?fiscal_year=2017").json()
    results[2018] = requests.get("https://api.usaspending.gov/api/v2/agency/treasury_account/072-019-2023/2024-1031-000/program_activity?fiscal_year=2018").json()
    results[2019] = requests.get("https://api.usaspending.gov/api/v2/agency/treasury_account/072-019-2023/2024-1031-000/program_activity?fiscal_year=2019").json()
    ... etc.
```

We save those results to the file `data/summaries/072-019-2023_2024-1031-000/program_activity_072-019-2023_2024-1031-000.json`

The result looks like this:
```
{
    "2017": [],
    "2018": [],
    "2019": [],
    "2020": [],
    "2021": [],
    "2022": [],
    "2023": [
        {
            "name": "DIRECT GLOBAL HEALTH PROGRAM ACTIVITY",
            "obligated_amount": 290120000.0,
            "gross_outlay_amount": 0.0,
            "type": "PAC/PAN",
            "children": [
                {
                    "name": "Grants and fixed charges",
                    "obligated_amount": 290000000.0,
                    "gross_outlay_amount": 0.0
                },
                {
                    "name": "Contractual services and supplies",
                    "obligated_amount": 120000.0,
                    "gross_outlay_amount": 0.0
                }
            ]
        }
    ],
    "2024": [
        {
            "name": "DIRECT GLOBAL HEALTH PROGRAM ACTIVITY",
            "obligated_amount": 3787630000.0,
            "gross_outlay_amount": 1045793574.57,
            "type": "PAC/PAN",
            "children": [
                {
                    "name": "Grants and fixed charges",
                    "obligated_amount": 3593856631.27,
                    "gross_outlay_amount": 988703880.04
                },
                {
                    "name": "Contractual services and supplies",
                    "obligated_amount": 148976889.71,
                    "gross_outlay_amount": 39037707.4
                },
                {
                    "name": "Personnel compensation and benefits",
                    "obligated_amount": 44647699.85,
                    "gross_outlay_amount": 18030656.71
                },
                {
                    "name": "Acquisition of assets",
                    "obligated_amount": 148779.17,
                    "gross_outlay_amount": 21330.42
                }
            ]
        }
    ],
    "2025": [
        {
            "name": "DIRECT GLOBAL HEALTH PROGRAM ACTIVITY",
            "obligated_amount": 15623092.06,
            "gross_outlay_amount": 0.0,
            "type": "PAC/PAN",
            "children": [
                {
                    "name": "Grants and fixed charges",
                    "obligated_amount": 15623092.06,
                    "gross_outlay_amount": 0.0
                }
            ]
        }
    ],
    "2026": {}
}
```


## 2. search_awards()

search_awards tries to find a list of all of the awards under that TAS code. It uses the USASpending endpoint [https://api.usaspending.gov/api/v2/search/spending_by_award/](https://api.usaspending.gov/api/v2/search/spending_by_award/) This endpoint requires more complicated inputs, so it uses requests.post() instead of requests.get().

The spending_by_award endpoint requires you to specify which award codes you want, and which fields you want back for each award. Because you can request different fields for different award types, we iterate through the award types. In pseudocode, this means we're doing:

```
downloader._search_award_type(award_codes = ["A", "B", "C", "D"])  # contracts
downloader._search_award_type(award_codes = ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"])  # IDV
downloader._search_award_type(award_codes = ["07", "08"]) # loans
downloader._search_award_type(award_codes = ["02", "03", "04", "05"])  # grants
downloader._search_award_type(award_codes = ["06", "10"])  # direct payments
downloader._search_award_type(award_codes = ["09", "11", "-1"])  # other
```

The function `_search_award_type` downloads the list of awards in chunks. Just like how the results table gets paginated when you go to https://www.usaspending.gov/search, the results table gets paginated when we use the API. So we ask for 100 rows at a time, store the rows we received in a dictionary keyed by `generated_internal_id`, which we're going to need later, and then ask for the next page, until the API tells us we're on the last page. An example of the input json for the first 100 grants looks like this:

```
{
    "filters": {
        "tas_codes": {"require": [["072-019-2023/2024-1031-000"]]},
        "award_type_codes": ["02", "03", "04", "05"]
    },
    "fields": [
            "generated_internal_id",
            "prime_award_recipient_id",
            "def_codes",
            "Award ID",
            "Recipient Name",
            "Recipient DUNS Number",
            "recipient_id",
            "Base Obligation Date",
            "Recipient Location",
            "Awarding Agency",
            "Awarding Agency Code",
            "Awarding Sub Agency",
            "Awarding Sub Agency Code",
            "Contract Award Type",
            "Award Type",
            "Funding Agency",
            "Funding Agency Code",
            "Funding Sub Agency",
            "Funding Sub Agency Code",
            "Description",
            "Start Date",
            "End Date",
            "Award Amount",
            "Total Outlays",
            "Award Type",
            "SAI Number",
            "CFDA Number", 
            "Assistance Listings",
            "primary_assistance_listing"
        ],
    "limit": 100, 
    "page": 1  
}
```

We then export all the results to a json file called `awards_072-019-2023_2024-1031-000.json`, which looks like this:

```
{
    "ASST_NON_7200AA18CA00011_072": {
        "Assistance Listings": [
            {
                "cfda_number": "98.001",
                "cfda_program_title": "USAID FOREIGN ASSISTANCE FOR PROGRAMS OVERSEAS"
            }
        ],
        "Award Amount": 265800000.0,
        "Award ID": "7200AA18CA00011",
        "Award Type": "COOPERATIVE AGREEMENT (B)",
        "Awarding Agency": "Agency for International Development",
        "Awarding Agency Code": "072",
        "Awarding Sub Agency": "Agency for International Development",
        "Awarding Sub Agency Code": "7200",
        "Base Obligation Date": "2018-08-26",
        "CFDA Number": "98.001",
        "Contract Award Type": null,
        "Description": "TO ADDRESS CHALLENGES TO NTD CONTROL AND ELIMINATION, THE CONTROL AND ELIMINATION OF NEGLECTED TROPICAL DISEASES (CEP-NTD) WILL BUILD ON USAID\u00bfS PRIOR AND CURRENT NTD INVESTMENTS ACROSS WEST AFRICA. THE FOUNDATION OF THE NTD PORTFOLIO IN WEST AFRICA, ESPECIALLY THE ACHIEVEMENTS GAINED UNDER END IN AFRICA AND ENVISION, ARE IDEAL STEPPING STONES TO ACHIEVING THE ELIMINATION OBJECTIVES FOR LF, TRACHOMA AND ONCHOCERCIASIS, AND TO FURTHER ENHANCING SUSTAINABILITY FOR CONTROL OF SCH, STH AND ONCHOCERCIASIS.",
        "End Date": "2026-07-18",
        "Funding Agency": "Agency for International Development",
        "Funding Agency Code": "072",
        "Funding Sub Agency": "Agency for International Development",
        "Funding Sub Agency Code": "7200",
        "Recipient DUNS Number": null,
        "Recipient Location": {
            "address_line1": "359 BLACKWELL STREET",
            "address_line2": "SUITE 200",
            "address_line3": null,
            "city_name": "DURHAM",
            "congressional_code": "04",
            "country_name": "UNITED STATES",
            "county_code": "063",
            "county_name": "DURHAM",
            "foreign_postal_code": null,
            "foreign_province": null,
            "location_country_code": "USA",
            "state_code": "NC",
            "state_name": "North Carolina",
            "zip4": "2477",
            "zip5": "27701"
        },
        "Recipient Name": "FAMILY HEALTH INTERNATIONAL",
        "SAI Number": "SAI NOT AVAILABLE",
        "Start Date": "2018-07-19",
        "Total Outlays": 218038709.89,
        "agency_slug": "agency-for-international-development",
        "awarding_agency_id": 801,
        "def_codes": [
            "Q"
        ],
        "generated_internal_id": "ASST_NON_7200AA18CA00011_072",
        "internal_id": 251783282,
        "primary_assistance_listing": {
            "cfda_number": "98.001",
            "cfda_program_title": "USAID FOREIGN ASSISTANCE FOR PROGRAMS OVERSEAS"
        },
        "prime_award_recipient_id": null,
        "recipient_id": "ce3945f4-76a8-ff4b-68a5-6562e575fc0c-C"
    },
    "ASST_NON_7200AA18CA00032_072": {
        "Assistance Listings": [
            {
                "cfda_number": "98.001",
                "cfda_program_title": "USAID FOREIGN ASSISTANCE FOR PROGRAMS OVERSEAS"
            }
        ],
        "Award Amount": 26733594.0,
        "Award ID": "7200AA18CA00032",
        "Award Type": "COOPERATIVE AGREEMENT (B)",
        "Awarding Agency": "Agency for International Development",
        "Awarding Agency Code": "072",
        "Awarding Sub Agency": "Agency for International Development",
        "Awarding Sub Agency Code": "7200",
        "Base Obligation Date": "2018-09-28",
        "CFDA Number": "98.001",
        "Contract Award Type": null,
        "Description": "THE PARTNERSHIPS PLUS TEAM SUPPORTS THIS GOAL THROUGH THE MANAGEMENT OF GRANTS AWARDED TO GRANTEES SELECTED IN FULL COLLABORATION WITH USAID TECHNICAL EVALUATION COMMITTEE (TEC) MEMBERS.",
        "End Date": "2027-03-31",
        "Funding Agency": "Agency for International Development",
        "Funding Agency Code": "072",
        "Funding Sub Agency": "Agency for International Development",
        "Funding Sub Agency Code": "7200",
        "Recipient DUNS Number": null,
        "Recipient Location": {
            "address_line1": "44 FARNSWORTH ST",
            "address_line2": null,
            "address_line3": null,
            "city_name": "BOSTON",
            "congressional_code": "08",
            "country_name": "UNITED STATES",
            "county_code": "025",
            "county_name": "SUFFOLK",
            "foreign_postal_code": null,
            "foreign_province": null,
            "location_country_code": "USA",
            "state_code": "MA",
            "state_name": "Massachusetts",
            "zip4": "1209",
            "zip5": "02210"
        },
        "Recipient Name": "JSI RESEARCH & TRAINING INSTITUTE INC",
        "SAI Number": "SAI NOT AVAILABLE",
        "Start Date": "2018-09-28",
        "Total Outlays": 21888010.63,
        "agency_slug": "agency-for-international-development",
        "awarding_agency_id": 801,
        "def_codes": [
            "3",
            "6",
            "Q"
        ],
        "generated_internal_id": "ASST_NON_7200AA18CA00032_072",
        "internal_id": 251783302,
        "primary_assistance_listing": {
            "cfda_number": "98.001",
            "cfda_program_title": "USAID FOREIGN ASSISTANCE FOR PROGRAMS OVERSEAS"
        },
        "prime_award_recipient_id": null,
        "recipient_id": "b94dbdfc-d93e-aa6c-5c2c-ffc2a5c9f020-C"
    },
    "ASST_NON_7200AA18CA00040_072": {
        "Assistance Listings": [
            {
                "cfda_number": "98.001",
                "cfda_program_title": "USAID FOREIGN ASSISTANCE FOR PROGRAMS OVERSEAS"
            }
        ],
        "Award Amount": 276998810.0,
        "Award ID": "7200AA18CA00040",
        "Award Type": "COOPERATIVE AGREEMENT (B)",
        "Awarding Agency": "Agency for International Development",
        "Awarding Agency Code": "072",
        "Awarding Sub Agency": "Agency for International Development",
        "Awarding Sub Agency Code": "7200",
        "Base Obligation Date": "2018-09-17",
        "CFDA Number": "98.001",
        "Contract Award Type": null,
        "Description": "TO ADDRESS CHALLENGES AND GAPS, RTI\u00bfS SUSTAIN TEAM HAS DESIGNED A CAPACITY STRENGTHENING FOR SUSTAINABILITY APPROACH FOR CEP-NTD ELEMENT 2. THROUGH SEVEN IMPLEMENTATION STRATEGIES, SUSTAIN WILL SUPPORT COUNTRIES TO ACCELERATE PROGRESS NOW; ADDRESS BARRIERS WITH INNOVATION; DOCUMENT AND SHARE SUCCESSES; IDENTIFY LONG-TERM CONTROL PLATFORMS; AND STRENGTHEN LOCAL CAPACITY FOR PLANNING, BUDGETING, AND DELIVERY SO THAT NTD PROGRAMS CAN CONTINUE WITH HIGH LEVELS OF EFFECTIVENESS. SUSTAIN EXPERTS AND COUNTRY TEAMS WILL TAILOR THIS CAPACITY STRENGTHENING APPROACH AND STRATEGIES THROUGH REGULAR COMMUNICATION WITH USAID\u00bfS NTD STAFF AND MINISTRIES OF HEALTH (MOHS). DATA AND CONTEXT WILL BE INCREASINGLY IMPORTANT AT THE COUNTRY LEVEL AS ELIMINATION APPROACHES; COVERAGE CHALLENGES IN THE LAST ENDEMIC AREAS ARE UNIQUE, REQUIRING INNOVATIVE STRATEGIES BASED ON ACCURATE DATA IN A SPECIFIC CONTEXT.",
        "End Date": "2026-09-16",
        "Funding Agency": "Agency for International Development",
        "Funding Agency Code": "072",
        "Funding Sub Agency": "Agency for International Development",
        "Funding Sub Agency Code": "7200",
        "Recipient DUNS Number": null,
        "Recipient Location": {
            "address_line1": "3040 CORNWALLIS RD",
            "address_line2": null,
            "address_line3": null,
            "city_name": "RESEARCH TRIANGLE PARK",
            "congressional_code": "04",
            "country_name": "UNITED STATES",
            "county_code": "063",
            "county_name": "DURHAM",
            "foreign_postal_code": null,
            "foreign_province": null,
            "location_country_code": "USA",
            "state_code": "NC",
            "state_name": "North Carolina",
            "zip4": "0155",
            "zip5": "27709"
        },
        "Recipient Name": "RESEARCH TRIANGLE INSTITUTE",
        "SAI Number": "SAI NOT AVAILABLE",
        "Start Date": "2018-09-17",
        "Total Outlays": 237323844.78,
        "agency_slug": "agency-for-international-development",
        "awarding_agency_id": 801,
        "def_codes": [
            "Q"
        ],
        "generated_internal_id": "ASST_NON_7200AA18CA00040_072",
        "internal_id": 251783310,
        "primary_assistance_listing": {
            "cfda_number": "98.001",
            "cfda_program_title": "USAID FOREIGN ASSISTANCE FOR PROGRAMS OVERSEAS"
        },
        "prime_award_recipient_id": null,
        "recipient_id": "8303ed9b-9fb4-3e90-ca81-50bbcf400948-C"
    },
    ... etc.
```

## 3. download_awards()

Now we have a list of all of the awards that we need to look at. download_awards() imports the file `awards_072-019-2023_2024-1031-000.json`, just to get the list of those generated_internal_id values. Then, we break up the list of awards into chunks of 10, and we make a separate request to [https://api.usaspending.gov/api/v2/download/contract](https://api.usaspending.gov/api/v2/download/contract) for each award.

For the award **CONT_AWD_72069619S00002_7200_-NONE-_-NONE-**, the request json will look like:

```
{ "award_id": "CONT_AWD_72069619S00002_7200_-NONE-_-NONE-" }
```

Note that we are not adding in our TAS code. What comes back will be all of the transactions that that award has had, which could cover many TAS codes.

The contract download endpoint responds with a link to find the status of our download and a link where the downloaded result will be. We then ping that link every 5 seconds to see if the file is ready. Usually, each award takes about 8 seconds to be ready. Sometimes, when there is a lot of traffic, we get pushed back in line, and one award's download can stretch to thousands of seconds. We're also rate-limited, so if we request too many awards at a time, it'll cut us off, so we wait 5 minutes and try again. In all, it will take a least several hours to download 200 awards.

Once the download is ready, we download the zip file from the file url that the api gave us. We unzip that file and save it to data/downloads. For the award **CONT_AWD_72069619S00002_7200_-NONE-_-NONE-**, that folder will look like:

```
    data\downloads\CONT_AWD_72069619S00002_7200_-NONE-_-NONE-
        >Contract_72069619S00002_FederalAccountFunding_1.csv
        >Contract_72069619S00002_Sub-Awards_1.csv
        >Contract_72069619S00002_TransactionHistory_1.csv
        >ContractAwardSummary_download_readme.txt
        >Data_Dictionary_Crosswalk.xlsx
```

We also add one more file after unzipping, called downloaded.txt. This tells us when we downloaded the folder, so if we come back later, we know if we need to re-download to get new data. 

## 4. combine_awards()

Now that we've finished the hard part of downloading all of those folders, we need to extract out just the rows for this TAS code. `combine_awards` imports that list of awards again from `awards_072-019-2023_2024-1031-000.json`. For each award, it looks in the downloaded folder and imports the FederalAccountFunding table, which has the following columns:

```
owning_agency_name,reporting_agency_name,submission_period,allocation_transfer_agency_identifier_code,agency_identifier_code,beginning_period_of_availability,ending_period_of_availability,availability_type_code,main_account_code,sub_account_code,treasury_account_symbol,treasury_account_name,agency_identifier_name,allocation_transfer_agency_identifier_name,budget_function,budget_subfunction,federal_account_symbol,federal_account_name,program_activity_code,program_activity_name,object_class_code,object_class_name,direct_or_reimbursable_funding_source,disaster_emergency_fund_code,disaster_emergency_fund_name,transaction_obligated_amount,gross_outlay_amount_FYB_to_period_end,USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig,USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig,award_unique_key,award_id_piid,parent_award_id_piid,award_id_fain,award_id_uri,award_base_action_date,award_base_action_date_fiscal_year,award_latest_action_date,award_latest_action_date_fiscal_year,period_of_performance_start_date,period_of_performance_current_end_date,ordering_period_end_date,award_type_code,award_type,idv_type_code,idv_type,prime_award_base_transaction_description,awarding_agency_code,awarding_agency_name,awarding_subagency_code,awarding_subagency_name,awarding_office_code,awarding_office_name,funding_agency_code,funding_agency_name,funding_sub_agency_code,funding_sub_agency_name,funding_office_code,funding_office_name,recipient_uei,recipient_duns,recipient_name,recipient_name_raw,recipient_parent_uei,recipient_parent_duns,recipient_parent_name,recipient_parent_name_raw,recipient_country,recipient_state,recipient_county,recipient_city,prime_award_summary_recipient_cd_original,prime_award_summary_recipient_cd_current,recipient_zip_code,primary_place_of_performance_country,primary_place_of_performance_state,primary_place_of_performance_county,prime_award_summary_place_of_performance_cd_original,prime_award_summary_place_of_performance_cd_current,primary_place_of_performance_zip_code,cfda_number,cfda_title,product_or_service_code,product_or_service_code_description,naics_code,naics_description,national_interest_action_code,national_interest_action,usaspending_permalink,last_modified_date
```

We then:
1. filter down that table to only rows where treasury_account_symbol="awards_072-019-2023_2024-1031-000"
2. sort by submission period (e.g. FY2024P3)
3. add a column called `fiscal_year`, computed from the submission period (e.g. 2024)
4. add a column called `fiscal_period`, computed from the submission period (e.g. P3)
5. add a column called `pa_code`, computed from object_class_code. the object class code gives us a granular category, e.g. 11.8, which tells us that kind of transaction this is. pa_code is just the first number of that code, e.g. 1.
6. add a column called `pa_title`, which looks up the English meaning of that pa_code, e.g. Personnel compensation and benefits. That title will match up with the titles that we downloaded to program_activity.json.
7. add a column called `transaction_outlay_amount`. The downloaded file comes with a column called `gross_outlay_amount_FYB_to_period_end`. That column is cumulative for the given fiscal year. To get the gross outlay for just that period, we group the table by fiscal year, then get the difference in gross outlay amount from the previous period for each row.

We combine all of our augmented FederalAccountFunding tables into one big table and save it to `data\summaries\072-019-2023_2024-1031-000\combined_FederalAccountFunding_072-019-2023_2024-1031-000.csv`.

Similarly, we combine all of the Sub-Awards and TransactionHistory files. However, those files do not label each row by TAS, instead lumping activity from all TAS together.

## 5. check_summaries()

We have our data, now we should validate it. First, we import `data/summaries/072-019-2023_2024-1031-000/program_activity_072-019-2023_2024-1031-000.json`. Then, we import `data\summaries\072-019-2023_2024-1031-000\combined_FederalAccountFunding_072-019-2023_2024-1031-000.csv`.

For each fiscal year:
1. get the totals from the program activity file for that fiscal year
2. filter down the transactions from the FederalAccountFunding table for that fiscal year.
3. compare the sum of the transaction_obligated_amount column in the FederalAccountFunding table to the Obligated value in the program activity file.
4. compare the sum of the transaction_outlay_amount column in the FederalAccountFunding table to the Gross Outlay value in the program activity file.
5. Repeat for any program activity categories found in either file.
6. Save the results to a text file, `data\summaries\072-019-2023_2024-1031-000\summary_check_072-019-2023_2024-1031-000.txt`

The results look like this:

```
TAS code: 072-019-2023/2024-1031-000

FY2023
  Total
    Obligated:     PA:   $290,120,000 / FA:    $290,120,000 / Missing:             $0 ( 0%)
    Gross Outlay:  PA:             $0 / FA:              $0 / Missing:             $0 (--%)
  2X: Contractual services and supplies
    Obligated:     PA:       $120,000 / FA:        $120,000 / Missing:             $0 ( 0%)
    Gross Outlay:  PA:             $0 / FA:              $0 / Missing:             $0 (--%)
  4X: Grants and fixed charges
    Obligated:     PA:   $290,000,000 / FA:    $290,000,000 / Missing:             $0 ( 0%)
    Gross Outlay:  PA:             $0 / FA:              $0 / Missing:             $0 (--%)

FY2024
  Total
    Obligated:     PA: $3,787,630,000 / FA:  $2,343,202,262 / Missing: $1,444,427,738 (38%)
    Gross Outlay:  PA: $1,045,793,574 / FA:    $770,591,144 / Missing:   $275,202,430 (26%)
  1X: Personnel compensation and benefits
    Obligated:     PA:    $44,647,699 / FA:      $7,670,263 / Missing:    $36,977,436 (82%)
    Gross Outlay:  PA:    $18,030,656 / FA:        $264,874 / Missing:    $17,765,782 (98%)
  2X: Contractual services and supplies
    Obligated:     PA:   $148,976,889 / FA:    $114,230,344 / Missing:    $34,746,545 (23%)
    Gross Outlay:  PA:    $39,037,707 / FA:     $25,724,985 / Missing:    $13,312,722 (34%)
  3X: Acquisition of assets
    Obligated:     PA:       $148,779 / FA:        $121,540 / Missing:        $27,239 (18%)
    Gross Outlay:  PA:        $21,330 / FA:          $8,432 / Missing:        $12,898 (60%)
  4X: Grants and fixed charges
    Obligated:     PA: $3,593,856,631 / FA:  $2,221,180,113 / Missing: $1,372,676,518 (38%)
    Gross Outlay:  PA:   $988,703,880 / FA:    $744,592,852 / Missing:   $244,111,028 (24%)

FY2025
  Total
    Obligated:     PA:    $15,623,092 / FA:              $0 / Missing:    $15,623,092 (100%)
    Gross Outlay:  PA:             $0 / FA:              $0 / Missing:             $0 (--%)
  4X: Grants and fixed charges
    Obligated:     PA:    $15,623,092 / FA:              $0 / Missing:    $15,623,092 (100%)
    Gross Outlay:  PA:             $0 / FA:              $0 / Missing:             $0 (--%)
```

From these results, we can see that our FederalAccountFunding table is missing some data that ProgramActivity is including. So now the question remains: where is that extra data?


