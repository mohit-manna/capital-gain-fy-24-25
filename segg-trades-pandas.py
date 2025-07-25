# This is AI generated code.
import pandas as pd
import numpy as np

cut_off_date = '2024-07-23'
before_str = f"Before {cut_off_date}"
after_str = f"After {cut_off_date}"
type_long_term = 'long_term'
type_short_term = 'short_term'

stcg_rate_before_cutoff = 0.15
stcg_rate_after_cutoff = 0.20
ltcg_rate_before_cutoff = 0.10
ltcg_rate_after_cutoff = 0.125

def read_csv(file_path, sep=","):
    df = pd.read_csv(file_path, sep=sep)
    df.columns = [col.lower() for col in df.columns]  # Standardize to lowercase
    return df

def transform(tradebook, taxpnl_shortterm, taxpnl_longterm):
    tradebook['symbol'] = tradebook['symbol'].str.replace('-BE', '', regex=False)
    tradebook['trade_value'] = tradebook['price'] * tradebook['quantity']
    tradebook['trade_date'] = pd.to_datetime(tradebook['trade_date'])

    # Group tradebook by symbol and date_group
    tradebook['date_group'] = np.where(
        tradebook['trade_date'] < pd.to_datetime(cut_off_date),
        before_str, after_str
    )
    tradebook_grp = tradebook[tradebook['trade_type'] == 'sell'].groupby(['symbol', 'date_group']).agg(
        count=('symbol', 'count'),
        last_trade_date=('trade_date', 'max'),
        total_quantity=('quantity', 'sum'),
        total_trade_value=('trade_value', 'sum')
    ).reset_index()

    # Join short term with tradebook_grp
    short_term_dated = pd.merge(
        taxpnl_shortterm,
        tradebook_grp,
        left_on=['symbol', 'sell value'],
        right_on=['symbol', 'total_trade_value'],
        how='left'
)
    short_term_dated = short_term_dated[~short_term_dated['last_trade_date'].isna()]
    short_term_dated['trade_date'] = short_term_dated['last_trade_date']
    short_term_dated['date_group'] = short_term_dated['date_group']

    # Short term trades not matched
    short_term_non_dated = pd.merge(
        taxpnl_shortterm,
        tradebook_grp,
        left_on=['symbol', 'sell value'],
        right_on=['symbol', 'total_trade_value'],
        how='left',
        indicator=True
    )
    short_term_non_dated = short_term_non_dated[short_term_non_dated['_merge'] == 'left_only']
    short_term_non_dated = short_term_non_dated.drop(columns=tradebook_grp.columns.difference(['symbol']), errors='ignore')

    # Group tradebook by symbol for leftovers
    tradebook_grouped = tradebook_grp.groupby('symbol').agg(
        last_trade_date=('last_trade_date', 'max'),
        past_trade_date=('last_trade_date', 'min'),
        total_quantity=('total_quantity', 'sum'),
        total_trade_value=('total_trade_value', 'sum')
    ).reset_index()

    # Add dates to short_term_non_dated
    short_term_non_dated_df = pd.merge(
        short_term_non_dated,
        tradebook_grouped,
        on='symbol',
        how='left'
    )
    short_term_non_dated_df['trade_date'] = short_term_non_dated_df['past_trade_date'].combine_first(short_term_non_dated_df['last_trade_date'])
    short_term_non_dated_df['date_group'] = np.where(
        short_term_non_dated_df['trade_date'] < pd.to_datetime(cut_off_date),
        before_str, after_str
    )

    # Update tradebook_grouped for long term
    tradebook_grouped_updated = pd.merge(
        tradebook_grouped,
        short_term_non_dated_df[['symbol', 'quantity', 'sell value']],
        on='symbol',
        how='left'
    )
    tradebook_grouped_updated['updated_quantity'] = tradebook_grouped_updated['total_quantity'] - tradebook_grouped_updated['quantity'].fillna(0)
    tradebook_grouped_updated['updated_value'] = tradebook_grouped_updated['total_trade_value'] - tradebook_grouped_updated['sell value'].fillna(0)
    print(tradebook_grouped_updated)
    # Join long term with updated tradebook
    # Round 'sell value' and 'updated_value' to 2 decimal places before merging
    taxpnl_longterm['sell value'] = taxpnl_longterm['sell value'].round(2)
    tradebook_grouped_updated['updated_value'] = tradebook_grouped_updated['updated_value'].round(2)
    taxpnl_longterm_df = pd.merge(
        taxpnl_longterm,
        tradebook_grouped_updated,
        left_on=['symbol', 'sell value'],
        right_on=['symbol', 'updated_value'],
        how='left'
    )
    taxpnl_longterm_df['date_group'] = np.where(
        taxpnl_longterm_df['last_trade_date'] < pd.to_datetime(cut_off_date),
        before_str, after_str
    )
    taxpnl_longterm_df['trade_date'] = taxpnl_longterm_df['last_trade_date'].combine_first(taxpnl_longterm_df['past_trade_date'])
    taxpnl_longterm_df.drop(columns=['quantity_y', 'sell value_y'], inplace=True, errors='ignore')
    taxpnl_longterm_df.rename(columns={'quantity_x': 'quantity', 'sell value_x': 'sell value'}, inplace=True)
    # Add type columns and select relevant columns
    short_term_dated['type'] = type_short_term
    short_term_non_dated_df['type'] = type_short_term
    taxpnl_longterm_df['type'] = type_long_term

    # Select columns to match output
    cols = ['symbol', 'quantity', 'buy value', 'sell value', 'realized p&l', 'trade_date', 'date_group', 'type']
    result_df = pd.concat([
        taxpnl_longterm_df[cols],
        short_term_dated[cols],
        short_term_non_dated_df[cols]
    ], ignore_index=True)
    print(taxpnl_longterm_df)
    return result_df

def get_trade_tax_rate_pd():
    columns = ['date_group', 'type', 'rate']
    data = [
        (before_str, type_short_term, stcg_rate_before_cutoff),
        (after_str, type_short_term, stcg_rate_after_cutoff),
        (before_str, type_long_term, ltcg_rate_before_cutoff),
        (after_str, type_long_term, ltcg_rate_after_cutoff)
    ]
    return pd.DataFrame(data, columns=columns)

def show_stats_pd(result_df):
    print("Profit and Loss Before and After 23-July-2024")
    trade_tax_rate = get_trade_tax_rate_pd()
    # Ensure columns are in correct case
    result_df = result_df.rename(columns={
        'Sell Value': 'sell value',
        'Realized P&L': 'realized p&l'
    })
    agg_tax = result_df.groupby(['date_group', 'type']).agg(
        trade_count=('symbol', 'count'),
        total_sell_value=('sell value', 'sum'),
        total_realized_profit_and_loss=('realized p&l', 'sum'),
        total_profit=('realized p&l', lambda x: x[x > 0].sum()),
        total_loss=('realized p&l', lambda x: x[x < 0].sum())
    ).reset_index()

    merged = pd.merge(agg_tax, trade_tax_rate, on=['date_group', 'type'], how='left')
    merged['rate_percent'] = (merged['rate'] * 100).round(2).astype(str) + '%'
    merged['tax_calculated'] = np.where(
        merged['total_realized_profit_and_loss'] > 0,
        (merged['total_realized_profit_and_loss'] * merged['rate']).round(2),
        0
    )
    print(merged[['date_group', 'type', 'trade_count', 'total_sell_value', 'total_realized_profit_and_loss', 'total_profit', 'total_loss', 'rate_percent', 'tax_calculated']])

    print("\nProfit and Loss in Whole FY 2024-25")
    agg_tax_fy = result_df.groupby(['type']).agg(
        trade_count=('symbol', 'count'),
        total_sell_value=('sell value', 'sum'),
        total_realized_profit_and_loss=('realized p&l', 'sum'),
        total_profit=('realized p&l', lambda x: x[x > 0].sum()),
        total_loss=('realized p&l', lambda x: x[x < 0].sum())
    ).reset_index()

    # Use before_str rates for FY summary
    trade_tax_rate_fy = trade_tax_rate[trade_tax_rate['date_group'] == before_str][['type', 'rate']]
    merged_fy = pd.merge(agg_tax_fy, trade_tax_rate_fy, on='type', how='left')
    merged_fy['rate_percent'] = (merged_fy['rate'] * 100).round(2).astype(str) + '%'
    merged_fy['tax_calculated'] = np.where(
        merged_fy['total_realized_profit_and_loss'] > 0,
        (merged_fy['total_realized_profit_and_loss'] * merged_fy['rate']).round(2),
        0
    )
    print(merged_fy[['type', 'trade_count', 'total_sell_value', 'total_realized_profit_and_loss', 'total_profit', 'total_loss', 'rate_percent', 'tax_calculated']])

if __name__ == "__main__":
    tradebook_file = "data/tradebook.csv"
    short_term_file = "data/taxPnL-shortTerm.csv"
    long_term_file = "data/taxPnL-longTerm.csv"

    tradebook_df = read_csv(tradebook_file)
    short_term_df = read_csv(short_term_file, sep="\t")
    long_term_df = read_csv(long_term_file, sep="\t")

    result_df = transform(tradebook_df, short_term_df, long_term_df)
    result_df.to_csv("output/pandas_result.csv", index=False)
    # print(result_df.head())
    print(show_stats_pd(result_df))