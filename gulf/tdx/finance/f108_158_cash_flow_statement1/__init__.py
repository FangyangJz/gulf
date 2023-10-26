# !/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Datetime : 2020/10/20 1:46
# @Author   : Fangyang
# @Software : PyCharm


from gulf.tdx.finance.f108_158_cash_flow_statement1.f120_128_financing import f120_128_financing_dict
from gulf.tdx.finance.f108_158_cash_flow_statement1.f108_119_investment import f108_119_investment_dict
from gulf.tdx.finance.f108_158_cash_flow_statement1.f108_119_operating import f098_107_operating_dict


f098_158_cash_flow_statement_dict = {
    # 4. 现金流量表
    # 4.1 经营活动 Operating
    **f098_107_operating_dict,
    # 4.2 投资活动 Investment
    **f108_119_investment_dict,
    # 4.3 筹资活动 Financing
    **f120_128_financing_dict,
    # 4.4 汇率变动
    '129四、汇率变动对现金的影响': 'effectOfForeignExchangRateChangesOnCash',
    '130四(2)、其他原因对现金的影响': 'effectOfOtherReasonOnCash',
    # 4.5 现金及现金等价物净增加
    '131五、现金及现金等价物净增加额': 'netIncreaseInCashAndCashEquivalents',
    '132期初现金及现金等价物余额': 'initialCashAndCashEquivalentsBalance',
    # 4.6 期末现金及现金等价物余额
    '133期末现金及现金等价物余额': 'theFinalCashAndCashEquivalentsBalance',
    # 4.x 补充项目 Supplementary Schedule：
    # 现金流量附表项目    Indirect Method
    # 4.x.1 将净利润调节为经营活动现金流量 Convert net profit to cash flow from operating activities
    '134净利润': 'netProfitFromOperatingActivities',
    '135资产减值准备': 'provisionForAssetsLosses',
    '136固定资产折旧、油气资产折耗、生产性生物资产折旧': 'depreciationForFixedAssets',
    '137无形资产摊销': 'amortizationOfIntangibleAssets',
    '138长期待摊费用摊销': 'amortizationOfLong-termDeferredExpenses',
    '139处置固定资产、无形资产和其他长期资产的损失': 'lossOfDisposingFixedAssetsIntangibleAssetsAndOtherLong-termAssets',
    '140固定资产报废损失': 'scrapLossOfFixedAssets',
    '141公允价值变动损失': 'lossFromFairValueChange',
    '142财务费用': 'financialExpenses',
    '143投资损失': 'investmentLosses',
    '144递延所得税资产减少': 'decreaseOfDeferredTaxAssets',
    '145递延所得税负债增加': 'increaseOfDeferredTaxLiabilities',
    '146存货的减少': 'decreaseOfInventory',
    '147经营性应收项目的减少': 'decreaseOfOperationReceivables',
    '148经营性应付项目的增加': 'increaseOfOperationPayables',
    '149其他': 'others',
    '150经营活动产生的现金流量净额2': 'netCashFromOperatingActivities2',
    # 4.x.2 不涉及现金收支的投资和筹资活动 Investing and financing activities not involved in cash
    '151债务转为资本': 'debtConvertedToCSapital',
    '152一年内到期的可转换公司债券': 'convertibleBondMaturityWithinOneYear',
    '153融资租入固定资产': 'leaseholdImprovements',
    # 4.x.3 现金及现金等价物净增加情况 Net increase of cash and cash equivalents
    '154现金的期末余额': 'cashEndingBal',
    '155现金的期初余额': 'cashBeginingBal',
    '156现金等价物的期末余额': 'cashEquivalentsEndingBal',
    '157现金等价物的期初余额': 'cashEquivalentsBeginningBal',
    '158现金及现金等价物净增加额': 'netIncreaseOfCashAndCashEquivalents',
}

if __name__ == '__main__':
    pass
