# sqlite3 edgar.db
# .mode csv
# .import ./data/edgar-combined.csv edgar

# CREATE INDEX idx_edgar_symbol ON edgar (symbol, form, taxonomy, end, start);


import pandas as pd
from ipydatagrid import DataGrid, TextRenderer, BarRenderer, Expr
from IPython.display import display
import argparse
import requests
import time
import os
import csv
import json
import sqlite3
import subprocess

from tqdm import tqdm

datadir = os.path.expanduser("~/Dropbox/Family Room/data")
combined_db_file = os.path.join(
    datadir,
    f"data-sec-edgar/edgar-combined.db"
)
combined_csv_file = os.path.join(
    datadir,
    f"data-sec-edgar/edgar-combined.csv"
)
report_file = os.path.join(
    datadir,
    f"data-sec-edgar/report.xlsx"
)
stocks_json = os.path.join(
    datadir,
    "opszero/capital_stocks.json"
)

conn = sqlite3.connect(combined_db_file)


def stocks():
    return json.load(open(stocks_json))


def is_file_over_3_days_old(file_path):
    # Get the modification time of the file
    mod_time = os.path.getmtime(file_path)

    # Get the current time
    current_time = time.time()

    # Calculate the time difference in seconds
    time_diff = current_time - mod_time

    # Calculate the time difference in days
    time_diff_days = time_diff / (24 * 60 * 60)

    # Check if the file is over 3 days old
    return time_diff_days > 3

def edgar_combine_to_csv():
    with open(combined_csv_file, "w", newline='') as csvfile:
        fieldnames = ['symbol', 'accn', 'form', 'fiscal_year', 'fiscal_period',
                      'start', 'end', 'filed', 'frame', 'taxonomy', 'unit', 'val']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        files_path = os.path.join(datadir, 'data-sec-edgar/edgar')
        for filename in tqdm(os.listdir(files_path)):
            if not filename.endswith(".json"):
                continue

            with open(os.path.join(files_path, filename)) as f:
                try:
                    edgar_data = json.load(f)
                except:
                    print(filename)
                    continue

                if 'facts' in edgar_data and 'us-gaap' in edgar_data['facts']:
                    us_gaap = edgar_data['facts']['us-gaap']

                    for taxonomy, units in us_gaap.items():
                        for unit_type, unit_vals in units['units'].items():
                            for unit in unit_vals:
                                writer.writerow(
                                    {
                                        'symbol': filename.replace(".json", ""),
                                        'accn': unit['accn'],
                                        'form': unit['form'],
                                        'fiscal_year': unit['fy'],
                                        'fiscal_period': unit['fp'],
                                        'start': unit.get('start'),
                                        'end': unit.get('end'),
                                        'filed': unit.get('filed'),
                                        'frame': unit.get('frame'),
                                        'taxonomy': taxonomy,
                                        'unit': unit_type,
                                        'val': unit['val'],
                                    }
                                )


def edgar_combine_to_sqlite():
    subprocess.getoutput(
        f"rm {combined_db_file}"
    )
    subprocess.getoutput(
        f"sqlite3 {combined_db_file} '.mode csv' '.import {combined_csv_file} edgar' 'CREATE INDEX idx_edgar_symbol ON edgar (symbol, form, taxonomy, end, start);'"
    )


def has_form_type(symbol, form_type):
    # Start / End
    cur = conn.cursor()
    cur.execute(
        "select count(*) from edgar where form = ? and symbol = ?;",
        (form_type, symbol,)
    )

    if row := cur.fetchone():
        return int(row[0]) > 0

    return False


def edgar_get_value(symbol, taxonomy, year):
    # Start / End
    cur = conn.cursor()
    cur.execute(
        f"select val from edgar where start = '{year}-01-01' and end = '{year}-12-31' and taxonomy = ? and form='10-K' and symbol = ?;",
        (taxonomy, symbol,)
    )

    if row := cur.fetchone():
        return float(row[0])

    # End
    cur = conn.cursor()
    cur.execute(
        f"select val from edgar where end = '{year}-12-31' and taxonomy = ? and form='10-K' and symbol = ?;",
        (taxonomy, symbol,)
    )

    if row := cur.fetchone():
        return float(row[0])

    # Frame
    cur = conn.cursor()
    cur.execute(
        f"select val from edgar where frame = 'CY{year}' and taxonomy = ? and form = '10-K' and symbol = ?;",
        (taxonomy, symbol,)
    )

    if row := cur.fetchone():
        return float(row[0])

    # None
    cur = conn.cursor()
    cur.execute(
        f"select val from edgar where frame = '' and taxonomy = ? and form = '10-K' and symbol = ? and fiscal_year = '{year}';",
        (taxonomy, symbol,)
    )

    if row := cur.fetchone():
        return float(row[0])

    return None


def edgar_calculate(year):
    calculations_csv_file = os.path.join(
        datadir,
        f"data-sec-edgar/edgar-calculations-{year}.csv"
    )
    with open(calculations_csv_file, 'w', newline='') as csvfile:
        fieldnames = [
            'symbol',
            'name',
            'industry',
            'total_stock',
            'assets',
            'liabilities',
            'capex',
            'revenue',
            'operating_income',
            'net_income',
            'tax_rate',
            'dividend',
            'dividend_payout_ratio',
            'share_repurchase',
            'free_cash_flow',
            'roic',
            'return_on_equity',
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for stock in tqdm(stocks()):
            symbol = stock['Symbol']

            if stock['ETF']:
                continue

            if not has_form_type(symbol, '10-K'):
                continue

            writer.writerow({
                'symbol': symbol,
                'name': stock['Name'],
                'industry': stock['Industry'],
                'total_stock': get_total_stock(symbol, year),
                'assets': get_current_assets(symbol, year),
                'liabilities': get_current_liabilities(symbol, year),
                'capex': get_capex(symbol, year),
                'revenue': get_revenue(symbol, year),
                'operating_income': get_operating_income(symbol, year),
                'net_income': get_net_income(symbol, year),
                'tax_rate': get_tax_rate(symbol, year),
                'dividend': edgar_get_value(symbol, 'PaymentsOfDividendsCommonStock', year),
                'dividend_payout_ratio': get_dividend_payout_ratio(symbol, year),
                'share_repurchase': get_share_repurchase(symbol, year),
                'free_cash_flow': get_free_cash_flow(symbol, year),
                'roic': get_return_on_invested_capital(symbol, year),
                'return_on_equity': get_return_on_equity(symbol, year),
            })

            # get_debt_to_equity(symbol)
            # get_stock_repurchase()
            # get_dividend_payout_ratio()


def get_current_assets(symbol, year):
    if r := edgar_get_value(symbol, 'AssetsCurrent', year):
        return r


def get_current_liabilities(symbol, year):
    if r := edgar_get_value(symbol, 'LiabilitiesCurrent', year):
        return r


def get_revenue(symbol, year):
    if revenue := edgar_get_value(symbol, 'Revenues', year):
        return revenue

    if revenue := edgar_get_value(symbol, 'RevenueFromContractWithCustomerExcludingAssessedTax', year):
        return revenue

    if revenue := edgar_get_value(symbol, 'RevenueFromContractWithCustomerIncludingAssessedTax', year):
        return revenue


def get_operating_income(symbol, year):
    if operating_profit := edgar_get_value(symbol, 'OperatingIncomeLoss', year):
        return operating_profit

    if operating_profit := edgar_get_value(symbol, 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest', year):
        return operating_profit

    if operating_profit := edgar_get_value(symbol, 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments', year):
        return operating_profit


def get_net_income(symbol, year):
    if net_profit := edgar_get_value(symbol, 'NetIncomeLoss', year):
        return net_profit

    if net_profit := edgar_get_value(symbol, 'NetIncomeLossAvailableToCommonStockholdersBasic', year):
        return net_profit


def get_tax_rate(symbol, year):
    if tax_rate := edgar_get_value(symbol, 'EffectiveIncomeTaxRateContinuingOperations', year):
        return tax_rate

    try:
        return edgar_get_value(
            symbol,
            'IncomeTaxExpenseBenefit',
            year
        ) / edgar_get_value(
            symbol,
            'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
            year
        )
    except:
        return None


def get_total_stock(symbol, year):
    if val := edgar_get_value(symbol, 'EntityCommonStockSharesOutstanding', year) and val > 0:
        return val
    else:
        return edgar_get_value(symbol, 'WeightedAverageNumberOfSharesOutstandingBasic', year)


def get_depreciation_and_amortization(symbol, year):
    if d := edgar_get_value(symbol, 'AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment', year):
        return d

    if d := edgar_get_value(symbol, 'DepreciationDepletionAndAmortization', year):
        return d


def get_capex(symbol, year):
    if d := edgar_get_value(symbol, 'NetCashProvidedByUsedInInvestingActivities', year):
        return d

    if d := edgar_get_value(symbol, 'PaymentsToAcquirePropertyPlantAndEquipment', year):
        return d

    if d := edgar_get_value(symbol, 'PaymentsToAcquireProductiveAssets', year):
        return d


def get_share_repurchase(symbol, year):
    if d := edgar_get_value(symbol, 'PaymentsForRepurchaseOfCommonStock', year):
        return d


def get_free_cash_flow(symbol, year):
    '''
    Free cash flow is a measure of a company's financial performance that
    represents the cash that a company generates after accounting for the
    costs of making and maintaining its capital investments. The formula for
    calculating free cash flow is:

    Free Cash Flow = Net Income + Depreciation and Amortization - Changes in Working Capital - Capital Expenditures

    Here is a breakdown of each term in the formula:

    Net income: This is the company's total profits, calculated by
    subtracting expenses from revenues.

    Depreciation and amortization: These are non-cash expenses that
    represent the decline in value of a company's assets over time.

    Changes in working capital: This is the difference between the company's
    current assets and current liabilities. Working capital represents the
    resources a company has available to fund its operations, so a positive
    change in working capital means the company has more resources
    available.

    Capital expenditures: These are the funds a company uses to acquire or
    upgrade physical assets such as property, plant, and equipment.

    By subtracting these costs from net income, the formula for free cash
    flow shows the amount of cash a company has available after accounting
    for the investments it has made in its business. This can be a useful
    measure for investors and analysts, as it shows the company's ability to
    generate cash that can be used to pay dividends, pay off debt, or fund
    future growth.
    '''

    '''
        (Revenue - Cost) * (1 - tax_rate) - CapEx - net_working_capital + tax_rate * depreciation

        This is also refered to as Free Operational Cash Flow
    '''

    try:
        net_income = get_net_income(symbol, year) or 0
        tax_rate = get_tax_rate(symbol, year) or 0
        depreciation = get_depreciation_and_amortization(symbol, year) or 0
        capex = abs(get_capex(symbol, year)) or 0

        assets = get_current_assets(symbol, year) or 0
        liabilities = get_current_liabilities(symbol, year) or 0
        assets_prev = get_current_assets(symbol, year-1) or 0
        liabilities_prev = get_current_liabilities(symbol, year-1) or 0

        change_net_working_capital = (
            assets_prev - liabilities_prev
        ) - (
            assets - liabilities
        )

        return net_income * (1 - tax_rate) - capex - change_net_working_capital + tax_rate * depreciation
    except:
        return None


def get_return_on_invested_capital(symbol, year):
    '''
    Return on invested capital (ROIC) is a financial ratio that measures the
    profitability of a company's investments in its business. It is
    calculated by dividing the company's after-tax operating profit by its
    invested capital. The formula for calculating ROIC is:

    ROIC = (After-tax Operating Profit / Invested Capital)

    Here is a breakdown of each term in the formula:

    After-tax operating profit: This is the company's total profits from its
    operations, after accounting for taxes and non-operating expenses such
    as interest expense.

    Invested capital: This is the total amount of capital that the company
    has invested in its business, including both debt and equity.

    By dividing the company's after-tax operating profit by its invested
    capital, the ROIC formula shows the percentage of each dollar of
    invested capital that is being earned as profit. This can be a useful
    measure for investors and analysts, as it shows the company's ability to
    generate returns on the capital it has invested in its business.
    '''

    try:
        tax_rate = get_tax_rate(symbol, year) or 0

        # NOPAT = (operating profit) x (1 – effective tax rate)
        nopat = get_operating_income(symbol, year) * (1 - tax_rate)

        liabilities_and_equity = edgar_get_value(
            symbol,
            'LiabilitiesAndStockholdersEquity',
            year
        )
        if liabilities_and_equity is None:
            return None

        # ROIC = NOPAT / (debt + equity)
        return nopat / liabilities_and_equity
    except:
        return None


def get_return_on_equity(symbol, year):
    try:
        net_income = edgar_get_value(symbol, 'NetIncomeLoss', year)
        equity = edgar_get_value(symbol, 'StockholdersEquity', year)

        return_on_equity = net_income / equity

        if return_on_equity < 0:
            return None

        return return_on_equity
    except:
        return None


def get_debt_to_equity(symbol):
    # us-gaap: Liabilities / us-gaap: StockholdersEquity
    liabilities = edgar_get_value(
        symbol,
        'Liabilities'
    )
    equity = edgar_get_value(
        symbol,
        'StockholdersEquity'
    )

    return liabilities / equity


def price_to_free_cash_flow(year):
    '''
    To calculate the price-to-free-cash-flow (P/FCF) ratio, you need to
    divide the current market price of a stock by the company's free cash
    flow per share. Here's the formula:

    P/FCF ratio = Market price per share / Free cash flow per share

    To calculate the free cash flow per share, you need to first calculate
    the company's free cash flow. Free cash flow is the cash that a company
    generates after accounting for capital expenditures, such as investments
    in property, plant, and equipment. It is calculated by subtracting
    capital expenditures from the company's operating cash flow. Here's the
    formula:

    Free cash flow = Operating cash flow - Capital expenditures

    To calculate the free cash flow per share, you then divide the company's
    free cash flow by the number of outstanding shares. Here's the formula:

    Free cash flow per share = Free cash flow / Number of outstanding shares
    '''

    try:
        free_cash_flow = self.get_value('FreeCashFlow', year)
        net_income = self.get_value('NetIncomeLoss', year)

        self.free_cash_flow = net_income / free_cash_flow
    except:
        self.free_cash_flow = None


def get_dividend_payout_ratio(symbol, year):
    # us-gaap:PaymentsOfDividendsCommonStock / ProfitLoss
    try:
        dividend = edgar_get_value(
            symbol, 'PaymentsOfDividendsCommonStock', year)
        if dividend is None:
            return None

        net_income = edgar_get_value(symbol, 'NetIncomeLoss', year)
        if net_income is None:
            return None

        return dividend / net_income
    except ZeroDivisionError:
        return None


def tax_rate(self):
    # Current: IncomeTaxesPaid
    # TaxesPayableCurrent / taxable income
    pass


def criteria(df):
    # Find companies with a high ROIC
    df = df.dropna(subset=['roic'])
    df = df[df['roic'] > 0.15]
    df = df[df['free_cash_flow'] > 0]
    df = df[df['share_repurchase'] > 0]
    df = df[df['dividend'] > 0]
    df = df.sort_values(by='symbol', ascending=True)
    return df


def generate_report():
    with pd.ExcelWriter('~/Dropbox/Family Room/data/data-sec-edgar/report.xlsx') as writer:
        for year in [2022, 2021, 2020, 2019]:
            df = pd.read_csv(
                f'~/Dropbox/Family Room/data/data-sec-edgar/edgar-calculations-{year}.csv'
            )
            df = criteria(df)
            df.to_excel(writer, sheet_name=str(year))

            ws = writer.sheets[str(year)]
            for col in ws.columns:
                max_length = 0
                column = col[0].column_letter  # Get the column name
                for cell in col:
                    try:  # Necessary to avoid error on empty cells
                        if len(str(cell.value)) > max_length:
                            max_length = len(cell.value)
                    except:
                        pass
                adjusted_width = (max_length + 20)
                ws.column_dimensions[column].width = adjusted_width


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('command',
                        choices=[
                            'download',
                            'combine',
                            'years',
                            'calculate',
                            'report',
                        ])
    parser.add_argument('-y', '--year')
    args = parser.parse_args()
    year = int(args.year) if args.year else 2021

    match args.command:
        case 'download':
            edgar_download()
        case 'combine':
            edgar_combine_to_csv()
            edgar_combine_to_sqlite()
        case 'years':
            print(subprocess.getoutput(
                "sqlite3 {combined_db_file} 'select distinct(fiscal_year) from edgar where fiscal_year > 2000 and fiscal_year < 2050;'"))
        case 'calculate':
            edgar_calculate(year)
        case 'report':
            generate_report()
        case _:
            print('Invalid command')
