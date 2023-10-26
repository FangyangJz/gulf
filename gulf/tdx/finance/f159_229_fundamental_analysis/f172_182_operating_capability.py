# !/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Datetime : 2020/10/20 2:21
# @Author   : Fangyang
# @Software : PyCharm

f172_182_operating_capability_dict = {
    # 6. 经营效率分析
    # 销售收入÷平均应收账款=销售收入\(0.5 x(应收账款期初+期末))
    '172应收帐款周转率': 'turnoverRatioOfReceivable',
    '173存货周转率': 'turnoverRatioOfInventory',
    # (存货周转天数+应收帐款周转天数-应付帐款周转天数+预付帐款周转天数-预收帐款周转天数)/365
    '174运营资金周转率': 'turnoverRatioOfOperatingAssets',
    '175总资产周转率': 'turnoverRatioOfTotalAssets',
    '176固定资产周转率': 'turnoverRatioOfFixedAssets',  # 企业销售收入与固定资产净值的比率
    '177应收帐款周转天数': 'daysSalesOutstanding',  # 企业从取得应收账款的权利到收回款项、转换为现金所需要的时间
    '178存货周转天数': 'daysSalesOfInventory',  # 企业从取得存货开始，至消耗、销售为止所经历的天数
    '179流动资产周转率': 'turnoverRatioOfCurrentAssets',  # 流动资产周转率(次)=主营业务收入/平均流动资产总额
    '180流动资产周转天数': 'daysSalesofCurrentAssets',
    '181总资产周转天数': 'daysSalesofTotalAssets',
    '182股东权益周转率': 'equityTurnover',  # 销售收入/平均股东权益
}

if __name__ == '__main__':
    pass
