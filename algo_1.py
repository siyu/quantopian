from quantopian.pipeline.filters.morningstar import Q1500US
from quantopian.pipeline import CustomFactor, Pipeline
from quantopian.research import run_pipeline
from quantopian.pipeline.data import morningstar
from itertools import chain
import numpy


# returns the oldest data given history windows
class HistFactor(CustomFactor):
    def compute(self, today, asset_ids, out, values):
        out[:] = values[0]
        
        
# returns the oldest ratio data given denominator field and numerator field and history windows
class HistRatioFactor(CustomFactor):
    def compute(self, today, asset_ids, out, numerator, denominator):
        out[:] = numerator[0] / denominator[0]        
        
        
# returns historical quarterly data given a field and number of quarter history
def make_hist_factor(field, num_qtr):
    hist_data = [HistFactor(inputs=[field], window_length=(i*64+1)) for i in range(num_qtr)] 
    hist_name = ['{}_Q{}'.format(field.name.split('.')[-1], str(i).zfill(2)) for i in range(num_qtr)] 
    return dict(zip(hist_name, hist_data))


# returns historical ratio quarterly data given a denominator field and numerator field 
# and number of quarter history
def make_hist_ratio_factor(field_name, field_numerator, field_denominator, num_qtr):
    hist_data = [HistRatioFactor(inputs=[field_numerator, field_denominator], window_length=(i*64+1)) for i in range(num_qtr)] 
    hist_name = ['{}_Q{}'.format(field_name, str(i).zfill(2)) for i in range(num_qtr)] 
    return dict(zip(hist_name, hist_data))


def make_pipeline():
    
    # industry code
    # Airline 31053108
    # Semiconductor 31169147
    industry = morningstar.asset_classification.morningstar_industry_code.latest 
    base_universe = Q1500US() & industry.eq(31053108)
    
    rev_growth_hist = make_hist_factor(morningstar.operation_ratios.revenue_growth, 12)
    de_ratio_hist = make_hist_factor(morningstar.operation_ratios.long_term_debt_equity_ratio, 12)
    eps_growth_hist = make_hist_factor(morningstar.earnings_ratios.diluted_cont_eps_growth, 12)
    buy_back_yield_hist = make_hist_factor(morningstar.valuation_ratios.buy_back_yield, 12)
    debt_cash_ratio_hist = make_hist_ratio_factor('debt_cash_ratio', morningstar.balance_sheet.long_term_debt, morningstar.balance_sheet.cash_and_cash_equivalents, 12)
    
    basic_columns = {'_ev_to_ebitda': morningstar.valuation_ratios.ev_to_ebitda.latest,
                     '_pe_ratio':morningstar.valuation_ratios.pe_ratio.latest,   
                     '_ev_sales_ratio':morningstar.valuation.enterprise_value.latest / morningstar.income_statement.total_revenue.latest,
                     '_payout_ratio':morningstar.valuation_ratios.payout_ratio.latest}
        
    return Pipeline(
        screen = base_universe,
        columns=dict(chain(basic_columns.items(),
                           rev_growth_hist.items(), 
                           de_ratio_hist.items(),
                           eps_growth_hist.items(),
                           buy_back_yield_hist.items(),
                           debt_cash_ratio_hist.items()))
    )        

result = run_pipeline(make_pipeline(), '2017-07-26', '2017-07-26')
result.T
