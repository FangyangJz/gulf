# !/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Datetime : 2020/10/19 23:00
# @Author   : Fangyang
# @Software : PyCharm


f008_021_current_assets_dict = {
    # 2.1.1 流动资产
    '008货币资金': 'moneyFunds',
    '009交易性金融资产': 'tradingFinancialAssets',
    '010应收票据': 'billsReceivables',
    '011应收账款': 'accountsReceivables',
    '012预付款项': 'prepayments',
    '013其他应收款': 'otherReceivables',
    '014应收关联公司款': 'interCompanyReceivables',
    '015应收利息': 'interestReceivables',
    '016应收股利': 'dividendsReceivables',
    '017存货': 'inventory',
    '018其中：消耗性生物资产': 'expendableBiologicalAssets',
    '019一年内到期的非流动资产': 'noncurrentAssetsDueWithinOneYear',
    '020其他流动资产': 'otherLiquidAssets',
    '021流动资产合计': 'totalLiquidAssets',
}
f022_039_noncurrent_assets_dict = {
    # 2.1.2 非流动资产
    '022可供出售金融资产': 'availableForSaleSecurities',
    '023持有至到期投资': 'heldToMaturityInvestments',
    '024长期应收款': 'longTermReceivables',
    '025长期股权投资': 'longTermEquityInvestment',
    '026投资性房地产': 'investmentRealEstate',
    '027固定资产': 'fixedAssets',
    '028在建工程': 'constructionInProgress',
    '029工程物资': 'engineerMaterial',
    '030固定资产清理': 'fixedAssetsCleanUp',
    '031生产性生物资产': 'productiveBiologicalAssets',
    '032油气资产': 'oilAndGasAssets',
    '033无形资产': 'intangibleAssets',
    '034开发支出': 'developmentExpenditure',
    '035商誉': 'goodwill',
    '036长期待摊费用': 'longTermDeferredExpenses',
    '037递延所得税资产': 'deferredIncomeTaxAssets',
    '038其他非流动资产': 'otherNonCurrentAssets',
    '039非流动资产合计': 'totalNonCurrentAssets',

}
f008_040_assets_dict = {
    # 2.1 资产
    # 2.1.1 流动资产
    **f008_021_current_assets_dict,
    # 2.1.2 非流动资产
    **f022_039_noncurrent_assets_dict,
    '040资产总计': 'totalAssets'
}

if __name__ == '__main__':
    pass
