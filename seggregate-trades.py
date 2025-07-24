from statistics import mode
import findspark
findspark.init()
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

cut_off_date = '2024-07-23'
before_str = f"Before {cut_off_date}"
after_str = f"After {cut_off_date}"
type_long_term = 'long_term'
type_short_term = 'short_term'

stcg_rate_before_cutoff = 0.15
stcg_rate_after_cutoff = 0.20
ltcg_rate_before_cutoff = 0.10 # on gains exceeding ₹1 lakh
ltcg_rate_after_cutoff = 0.125 # on gains exceeding ₹1.25 lakh

def get_spark():
    return SparkSession.builder.master("local[*]").appName("Pytest-PySpark").getOrCreate()

def read_csv(spark, file_path, header=True, inferSchema=True, sep=","):
    return spark.read.csv(file_path, header=header, inferSchema=inferSchema, sep=sep)

def transform(spark, tradebook, taxpnl_shortterm, taxpnl_longterm):
    # Grouping tradebook into Before and After 23-July-2024
    tradebook = tradebook.withColumn('symbol', F.regexp_replace('symbol', '-BE', ''))
    tradebook_grp=tradebook.where(F.col('trade_type') == 'sell')\
        .withColumn(
            'date_group',
            F.when(F.col('trade_date') < cut_off_date, before_str)
            .otherwise(after_str)
        )\
        .withColumn('trade_value', F.col('price') * F.col('quantity'))\
        .groupBy('symbol','date_group')\
        .agg(
            F.count('symbol').alias('count'),
            F.max(F.col('trade_date')).alias('last_trade_date'),
            F.sum(F.col('quantity')).alias('total_quantity'),
            F.sum(F.round(F.col('trade_value'), 2)).alias('total_trade_value')
        )\
        .orderBy('symbol', 'date_group')

    # adding dates in short term trades
    short_term_dated = taxpnl_shortterm.join(tradebook_grp, \
        on=(taxpnl_shortterm['symbol']==tradebook_grp['symbol']) \
            &(taxpnl_shortterm['Sell Value']==F.round(tradebook_grp['total_trade_value'], 2)) \
        ,how='left') \
    .select(taxpnl_shortterm['*'], tradebook_grp['last_trade_date'].alias('trade_date'), tradebook_grp['date_group'].alias('date_group'))\
    .filter(F.col('trade_date').isNotNull())

    # short term trades which are leftover after joining with tradebook_grp
    short_term_non_dated = taxpnl_shortterm.join(tradebook_grp, \
            on=(taxpnl_shortterm['symbol']==tradebook_grp['symbol']) \
                &(taxpnl_shortterm['Sell Value']==F.round(tradebook_grp['total_trade_value'], 2)) \
            ,how='leftanti')

    # Merging the tradebook symbol wise
    tradebook_grouped = tradebook_grp.groupBy('symbol')\
        .agg(
            F.max(F.col('last_trade_date')).alias('last_trade_date'),
            F.min(F.col('last_trade_date')).alias('past_trade_date'),
            F.sum(F.col('total_quantity')).alias('total_quantity'),
            F.sum(F.round(F.col('total_trade_value'), 2)).alias('total_trade_value')
        )\
        .orderBy('symbol')

    # short_term leftover trades with added dates
    short_term_non_dated_df = short_term_non_dated.join(tradebook_grouped, \
        on=(short_term_non_dated['symbol']==tradebook_grouped['symbol']) \
        ,how='left')\
        .withColumn('trade_date', F.coalesce('past_trade_date','last_trade_date'))\
        .withColumn('date_group', F.when(F.col('trade_date') < cut_off_date, before_str)\
            .otherwise(after_str))\
        .select(short_term_non_dated['*'],'trade_date','date_group')
  
    # Trades leftover after subtracting short term trades from tradebook_grp
    tradebook_grouped_updated = tradebook_grouped.join(short_term_non_dated_df, \
            on=(short_term_non_dated['symbol']==tradebook_grouped['symbol']) \
            ,how='left')\
            .withColumn('updated_quantity',F.coalesce((tradebook_grouped['total_quantity'] - short_term_non_dated['Quantity']),tradebook_grouped['total_quantity']))\
            .withColumn('updated_value', F.coalesce((tradebook_grouped['total_trade_value'] - short_term_non_dated['Sell Value']),tradebook_grouped['total_trade_value']))\
            .select(tradebook_grouped['symbol'],tradebook_grouped['last_trade_date'],tradebook_grouped['past_trade_date'], F.col('updated_quantity').alias('total_quantity'), F.col('updated_value').alias('total_trade_value'))

    # Long term trades with dates
    taxpnl_longterm_df  = taxpnl_longterm.join(tradebook_grouped_updated, \
        on=(taxpnl_longterm['symbol']==tradebook_grouped_updated['symbol']) \
            &(F.round(taxpnl_longterm['Sell Value'], 2)==F.round(tradebook_grouped_updated['total_trade_value'], 2)) \
        ,how='left') \
    .withColumn('date_group', F.when(F.col('last_trade_date') < cut_off_date, before_str)\
            .otherwise(after_str))\
    .withColumn('trade_date', F.coalesce('last_trade_date','past_trade_date'))\
    .select(taxpnl_longterm['*'], F.col('trade_date'), F.col('date_group'))

    # Final result DataFrame with type column
    result_df = taxpnl_longterm_df.withColumn('type', F.lit(type_long_term))\
        .unionAll(short_term_dated.withColumn('type', F.lit(type_short_term))\
            .unionAll(short_term_non_dated_df.withColumn('type', F.lit(type_short_term))))
    return result_df

def compare_dataframes(expected_df, actual_df):
    expected_cols = set(expected_df.columns)
    actual_cols = set(actual_df.columns)
    if expected_cols != actual_cols:
        print(f"Column mismatch: expected {expected_cols}, actual {actual_cols}")
        return False

    sort_columns = list(expected_df.columns)
    expected_sorted = expected_df.orderBy(*sort_columns)
    actual_sorted = actual_df.orderBy(*sort_columns)

    expected_rows = expected_sorted.collect()
    actual_rows = actual_sorted.collect()

    if len(expected_rows) != len(actual_rows):
        print(f"Row count mismatch: expected {len(expected_rows)}, actual {len(actual_rows)}")
        return False

    for idx, (exp_row, act_row) in enumerate(zip(expected_rows, actual_rows)):
        for col in sort_columns:
            # Uncomment the next line to see detailed comparison output
            # print(f"Comparing row {idx}, column '{col}': expected {exp_row[col]}, actual {act_row[col]} {'✅' if exp_row[col] == act_row[col] else '❌'}")
            if exp_row[col] != act_row[col]:
                print(f"Mismatch at row {idx}, column '{col}': expected {exp_row[col]}, actual {act_row[col]}")
                input("Press Enter to continue...")
                return False
    print("Comparison Completed")
    return True

def get_trade_tax_rate(spark):
    columns = ['date_group','type','rate']
    data = [
        (before_str,type_short_term, stcg_rate_before_cutoff),
        (after_str, type_short_term, stcg_rate_after_cutoff),
        (before_str, type_long_term, ltcg_rate_before_cutoff),
        (after_str, type_long_term, ltcg_rate_after_cutoff)
    ]
    return spark.createDataFrame(data, columns)

def show_stats(spark, result_df):
    print("Profit and Loss Before and After 23-July-2024")
    trade_tax_rate = get_trade_tax_rate(spark)
    agg_tax = result_df.groupBy('date_group','type').agg(
        F.count('*').alias('trade_count'),
        F.sum('Sell Value').alias('total_sell_value'),
        F.sum('Realized P&L').alias('total_realized_profit_and_loss'),
        F.sum(F.when(F.col('Realized P&L') > 0, F.col('Realized P&L')).otherwise(0)).alias('total_profit'),
        F.sum(F.when(F.col('Realized P&L') < 0, F.col('Realized P&L')).otherwise(0)).alias('total_loss'),
    )

    agg_tax.join(trade_tax_rate, on=(agg_tax.date_group == trade_tax_rate.date_group) &
            (agg_tax.type == trade_tax_rate.type),how='left')\
    .withColumn('rate_percent', F.concat(F.round(trade_tax_rate.rate * 100, 2).cast("string"), F.lit('%')))\
    .withColumn('tax_calculated', F.when(F.col('total_realized_profit_and_loss')>0, F.round(F.col('total_realized_profit_and_loss') * F.col('rate'), 2)).otherwise(0))\
    .select(agg_tax.date_group, agg_tax.type, agg_tax.trade_count, agg_tax.total_sell_value, agg_tax.total_realized_profit_and_loss, agg_tax.total_profit, agg_tax.total_loss,
            'rate_percent','tax_calculated').show()

    print("Profit and Loss in Whole FY 2024-25")
    agg_tax= result_df.groupBy('type').agg(
        F.count('*').alias('trade_count'),
        F.sum('Sell Value').alias('total_sell_value'),
        F.sum('Realized P&L').alias('total_realized_profit_and_loss'),
        F.sum(F.when(F.col('Realized P&L') > 0, F.col('Realized P&L')).otherwise(0)).alias('total_profit'),
        F.sum(F.when(F.col('Realized P&L') < 0, F.col('Realized P&L')).otherwise(0)).alias('total_loss')
    )
    trade_tax_rate_df = trade_tax_rate.where(F.col('date_group') == before_str)
    agg_tax.join(trade_tax_rate_df, on=(agg_tax.type == trade_tax_rate.type),how='left')\
    .withColumn('rate_percent', F.concat(F.round(trade_tax_rate.rate * 100, 2).cast("string"), F.lit('%')))\
    .withColumn('tax_calculated', F.when(F.col('total_realized_profit_and_loss')>0, F.round(F.col('total_realized_profit_and_loss') * F.col('rate'), 2)).otherwise(0))\
    .select(agg_tax.type, agg_tax.trade_count, agg_tax.total_sell_value, agg_tax.total_realized_profit_and_loss, agg_tax.total_profit, agg_tax.total_loss,
            'rate_percent','tax_calculated').show()

def test_transformations(spark, result_df=None):

    expected_output_df = short_term_df\
        .withColumn('type', F.lit(type_short_term))\
        .unionAll(long_term_df\
            .withColumn('type', F.lit(type_long_term))
        )
    # expected_output_df.orderBy('symbol').show(n=100, truncate=False)
    # result_df.orderBy('symbol').show(n=100, truncate=False)
    try:
        sort_columns = expected_output_df.columns
        expected_output_df = expected_output_df.orderBy(*sort_columns)
        result_df = result_df.select(*sort_columns).orderBy(*sort_columns)
    except Exception as e:
        print(e)
        pass

    # Assert that the SQL transformation matches the expected output
    res = compare_dataframes(expected_output_df,result_df)
    assert res
    return res


if __name__=="__main__":
    current_file = os.path.splitext(os.path.basename(__file__))[0]
    tradebook_file = f"data/tradebook.csv"
    short_term_file = f"data/taxPnL-shortTerm.csv"
    long_term_file = f"data/taxPnL-longTerm.csv"
    spark = get_spark()

    tradebook_df = read_csv(spark, tradebook_file)
    short_term_df = read_csv(spark, short_term_file, sep="\t")
    long_term_df = read_csv(spark, long_term_file, sep="\t")

    result_df = transform(spark, tradebook_df, short_term_df, long_term_df)
    res= test_transformations(spark, result_df)
    if res == True:
        print("All tests passed!")
        result_df.coalesce(1).write.csv(f"output", header=True, mode="overwrite")
        show_stats(spark,result_df)
    else:
        print("Some tests failed.")