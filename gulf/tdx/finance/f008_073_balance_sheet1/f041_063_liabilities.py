# !/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Datetime : 2020/10/19 23:01
# @Author   : Fangyang
# @Software : PyCharm


f041_054_current_liabilities = {
    # 2.2.1 流动负债
    '041短期借款': 'shortTermLoan',
    '042交易性金融负债': 'tradingFinancialLiabilities',
    '043应付票据': 'billsPayable',
    '044应付账款': 'accountsPayable',
    '045预收款项': 'advancedReceivable',
    '046应付职工薪酬': 'employeesPayable',
    '047应交税费': 'taxPayable',
    '048应付利息': 'interestPayable',
    '049应付股利': 'dividendPayable',
    '050其他应付款': 'otherPayable',
    '051应付关联公司款': 'interCompanyPayable',
    '052一年内到期的非流动负债': 'noncurrentLiabilitiesDueWithinOneYear',
    '053其他流动负债': 'otherCurrentLiabilities',
    '054流动负债合计': 'totalCurrentLiabilities',
}

f055_062_noncurrent_liabilities_dict = {
    # 2.2.2 非流动负债
    '055长期借款': 'longTermLoans',
    '056应付债券': 'bondsPayable',
    '057长期应付款': 'longTermPayable',
    '058专项应付款': 'specialPayable',
    '059预计负债': 'estimatedLiabilities',
    '060递延所得税负债': 'defferredIncomeTaxLiabilities',
    '061其他非流动负债': 'otherNonCurrentLiabilities',
    '062非流动负债合计': 'totalNonCurrentLiabilities',
}

f041_063_liabilities_dict = {
    # 2.2 负债
    **f041_054_current_liabilities,
    **f055_062_noncurrent_liabilities_dict,
    '063负债合计': 'totalLiabilities',
}

if __name__ == '__main__':
    pass
