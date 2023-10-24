# !/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Datetime : 2022/10/15 2:00
# @Author   : Fangyang
# @Software : PyCharm


# 计算Alpha的时候注意要按照 trade_date 和 jj_code 降序排列, 在后续合并因子的时候可以使用concat提高效率
def finish_res(func):
    def inner(*args, **kwargs) -> str:

        res, table_name, alpha_name, top_n = func(*args, **kwargs)
        # TODO 注意! dolphindb中将nan排序在最小值之后,
        #  所以这里用降序排列, 选取topk
        #  等价于 pandas nlargest
        if top_n:
            res += f"""
            t1 = select top {top_n} * from {table_name} context by trade_date csort {alpha_name} desc;
            select * from t1 order by trade_date, jj_code;
            """
        else:
            res += f"""
            select * from {table_name} order by jj_code, trade_date;
            """
        return res

    return inner


if __name__ == '__main__':
    pass
