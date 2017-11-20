from numpy import genfromtxt
import csv
import datetime
import math


"""
For each of the tickers
Calculate 3mth change, 6mth change
calculate 3mth benchmark, 6mth benchmark
write to file
"""

class Company:

    def __init__(self, gd_name, bb_name, ticker, sector_name, industry_bench, sub_bench):
        self.gd_name = gd_name
        self.bb_name = bb_name
        self.ticker = ticker
        self.sector_name = sector_name
        self.industry_bench = industry_bench
        self.sub_bench = sub_bench


def calculate_perf(filepath, starting_year, ending_year, starting_q, ending_q):

    initial_value = math.inf
    ending_value = math.inf

    try:
        with open(filepath, 'r', encoding='utf-16') as perf_file:
            csvreader = csv.reader(perf_file)
            next(csvreader, None)

            # find the starting Q value
            for row in csvreader:
                if row[0] is "":
                    break
                date = datetime.datetime.strptime(row[0], '%m/%d/%y')
                year = date.year
                quarter = (date.month - 1) // 3 + 1

                if year == starting_year and quarter == starting_q:
                    initial_value = float(row[1])

                if year == ending_year and quarter == ending_q:
                    ending_value = float(row[1])

    except Exception as e:
        print(filepath)

    finally:
        if not ending_value == math.inf and not initial_value == math.inf:
            return (ending_value - initial_value)/initial_value
        else:
            return "ERROR"


companies = {}

# load companies into dict
with open(r'../csvs/mstr_lkup_tbl.csv','r') as f:
    csvreader = csv.reader(f)
    next(csvreader, None)
    for row in csvreader:
        companies[row[0]] = Company(row[0], row[1], row[2], row[3], row[5], row[7])

# in the sentiment sheet, find the next 3-mth, 6-mth company performance, benchmark performance
with open(r'../csvs/mstr_sentiment.csv', 'r') as infile, open('../csvs/sentiment_final.csv', 'w', newline='') as outfile:
    csvreader = csv.reader(infile)
    csvwriter = csv.writer(outfile)
    next(csvreader, None)

    for row in csvreader:

        company = row[0]
        if company not in companies:
            continue

        result = [] + row + [companies[company].sector_name]
        quarter = int(row[1])
        year = int(row[2])


        three_mth_quarter = (quarter + 1)  if not quarter == 4 else 1
        three_mth_year = year if quarter <= 3 else year + 1
        six_mth_quarter = (quarter + 1)  if not quarter == 4 else 1
        six_mth_year = year if quarter <= 2 else year + 1


        company_file = '../csvs/company-historical-performance/' + companies[company].ticker + '.txt'
        industry_file = '../csvs/benchmarks-historical-performance/' + companies[company].industry_bench + '.txt'
        sub_industry_file = '../csvs/benchmarks-historical-performance/' + companies[company].sub_bench + '.txt'

        files = [company_file, industry_file, sub_industry_file]

        for file in files:
            result.append(calculate_perf(file,year, three_mth_year, quarter, three_mth_quarter))
            result.append(calculate_perf(file, year, six_mth_year, quarter, six_mth_quarter))
            result.append(calculate_perf(file, year, year+1, quarter, quarter))
        csvwriter.writerow(result)