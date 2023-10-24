# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2022/11/8 15:40
# @Author   : Fangyang
# @Software : PyCharm

from typing import Tuple
from gulf.dolphindb.pyfuncs import finish_res


@finish_res
def wq_alpha1(table_name: str, alpha_name: str = "wq_a1", top_n: int = None) -> Tuple[str, str, str, int]:
    """
    rank() from table context by trade_date 就等价于宽表的 rowRank

    ts = mimax(pow(iif(ratios(close) - 1 < 0, mstd(ratios(close) - 1, 20), close), 2.0), 5)
    return rowRank(X=ts, percent=true) - 0.5
    """

    res = f"""
        update {table_name} 
        set maxIndex=mimax(pow(iif(ratios(close) - 1 < 0, mstd(ratios(close) - 1, 20), close), 2.0), 5) 
        context by jj_code;
        
        update {table_name} 
        set {alpha_name}=rank(maxIndex)-0.5 
        context by trade_date;
        
        {table_name}.drop!("maxIndex");
    """
    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha2(table_name: str, alpha_name: str = "wq_a2", top_n: int = None) -> Tuple[str, str, str, int]:
    """
    dolphindb top_n 效率测试约为 pandas 的 100 倍
    :param alpha_name:
    :param table_name:
    :param top_n:
    :return:
    """
    res = f"""
            update {table_name} 
            set delta=log(volume) - log(mfirst(volume, 3)) 
            context by jj_code;
            
            update {table_name} 
            set rank1=rank(delta, percent=true) , rank2=rank((close - open) / open, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set {alpha_name}= -mcorr(rank1, rank2, 6)  
            context by jj_code; 
            
            {table_name}.drop!(`delta`rank1`rank2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha3(table_name: str, alpha_name: str = "wq_a3", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name}
        set rank1=rank(open, percent=true), rank2=rank(volume, percent=true) 
        context by trade_date;
        
        update {table_name}
        set {alpha_name}=-mcorr(rank1, rank2, 10)  
        context by jj_code;
        
        {table_name}.drop!(`rank1`rank2);
    """
    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha4(table_name: str, alpha_name: str = "wq_a4", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set rank1=rank(low, percent=true) 
        context by trade_date;
        
        update {table_name}
        set {alpha_name}=-mrank(rank1, true, 9)  
        context by jj_code;
        
        {table_name}.drop!(`rank1);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha5(table_name: str, alpha_name: str = "wq_a5", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set o_vwap=(open - (msum(vwap, 10) / 10)), c_vwap=(close - vwap) 
        context by jj_code;
        
        update {table_name} 
        set rank1=rank(o_vwap, percent=true), rank2=rank(c_vwap, percent=true) 
        context by trade_date;

        update {table_name}
        set {alpha_name}=rank1 * (-1 * abs(rank2));

        {table_name}.drop!(`o_vwap`c_vwap`rank1`rank2);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha6(table_name: str, alpha_name: str = "wq_a6", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set {alpha_name}=-mcorr(open, volume, 10) 
        context by jj_code;
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha7(table_name: str, alpha_name: str = "wq_a7", top_n: int = None) -> Tuple[str, str, str, int]:
    # TODO 单因子分析size不满足要求, 怀疑是 !=1 的数据太少了

    res = f"""
        update {table_name} 
        set delta=close - mfirst(close, 8) 
        context by jj_code;
        
        update {table_name} 
        set {alpha_name} = iif(mavg(volume, 20) < volume, -mrank(abs(delta), true, 60) * sign(delta), -1) 
        context by jj_code;
        
        {table_name}.drop!(`delta);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha8(table_name: str, alpha_name: str = "wq_a8", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set sums = msum(open, 5) * msum((ratios(close) - 1), 5) 
        context by jj_code;
        
        update {table_name} 
        set sums_diff = (sums - mfirst(sums, 11)) 
        context by jj_code;
        
        update {table_name} 
        set {alpha_name} = -rank(sums_diff, percent=true) 
        context by trade_date;

        {table_name}.drop!(`sums`sums_diff);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha9(table_name: str, alpha_name: str = "wq_a9", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set delta = close - mfirst(close, 2) 
        context by jj_code;
        
        update {table_name} 
        set iffalse = iif(mmax(delta, 5) < 0, delta, -delta) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = iif(0 < mmin(delta, 5), delta, iffalse) 
        context by jj_code;

        {table_name}.drop!(`delta`iffalse);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha10(table_name: str, alpha_name: str = "wq_a10", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set delta = close - mfirst(close, 2) 
        context by jj_code;

        update {table_name} 
        set iffalse = iif(mmax(delta, 4) < 0, delta, -delta), cond = mmin(delta, 4) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = rank(iif(0 < cond, delta, iffalse), percent=true) 
        context by trade_date;

        {table_name}.drop!(`delta`iffalse`cond);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha11(table_name: str, alpha_name: str = "wq_a11", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set delta = volume - mfirst(volume, 2), m1=mmax((vwap - close), 3), m2=mmin((vwap - close), 3) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = (rank(m1, percent=true) + rank(m2, percent=true)) * rank(delta, percent=true) 
        context by trade_date;

        {table_name}.drop!(`delta`m1`m2);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha12(table_name: str, alpha_name: str = "wq_a12", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set {alpha_name} = sign((volume - mfirst(volume, 2))) * (-1 * (close - mfirst(close, 2))) 
        context by jj_code;
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha13(table_name: str, alpha_name: str = "wq_a13", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set rank1=rank(close, percent=true), rank2=rank(volume, percent=true) 
        context by trade_date;

        update {table_name} 
        set covar1=mcovar(rank1, rank2, 5) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = -rank(covar1, percent=true) 
        context by trade_date;

        {table_name}.drop!(`rank1`rank2`covar1);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha14(table_name: str, alpha_name: str = "wq_a14", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set delta=pct_chg - mfirst(pct_chg, 4), mcovar1=mcovar(open, volume, 10) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = -rank(delta, percent=true)*mcovar1 
        context by trade_date;

        {table_name}.drop!(`delta`mcovar1);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha15(table_name: str, alpha_name: str = "wq_a15", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set rank1=rank(high, percent=true), rank2=rank(volume, percent=true) 
        context by trade_date;

        update {table_name}
        set {alpha_name}=-msum(mcorr(rank1, rank2, 3), 3) 
        context by jj_code;

        {table_name}.drop!(`rank1`rank2);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha16(table_name: str, alpha_name: str = "wq_a16", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set rank1=rank(high, percent=true), rank2=rank(volume, percent=true) 
        context by trade_date;

        update {table_name}
        set mcovar5=mcovar(rank1, rank2, 5)
        context by jj_code;
        
        update {table_name} 
        set {alpha_name}=-rank(mcovar5, percent=true) 
        context by trade_date;

        {table_name}.drop!(`rank1`rank2`mcovar5);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha17(table_name: str, alpha_name: str = "wq_a17", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set m1 = mrank(close, true, 10), 
        m2=(close - mfirst(close, 2)) - mfirst((close - mfirst(close, 2)), 2), 
        m3=mrank((volume / mavg(volume, 20)), true, 5) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = -rank(m1, percent=true) * rank(m2, percent=true) * rank(m3, percent=true) 
        context by trade_date;

        {table_name}.drop!(`m1`m2`m3);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha18(table_name: str, alpha_name: str = "wq_a18", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set m1 = (mstd(abs(close - open), 5) + close - open + mcorr(close, open, 10)) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = -rank(m1, percent=true)  
        context by trade_date;

        {table_name}.drop!(`m1);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha19(table_name: str, alpha_name: str = "wq_a19", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set m1 = -sign(close - mfirst(close, 8) + close - mfirst(close, 8)),
        m2=(1 + msum((ratios(close) - 1), 250))  
        context by jj_code;

        update {table_name} 
        set {alpha_name} = m1 * (1+rank(m2, percent=true))  
        context by trade_date;

        {table_name}.drop!(`m1`m2);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha20(table_name: str, alpha_name: str = "wq_a20", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set m1 = (open - mfirst(high, 2)), 
        m2=(open - mfirst(close, 2)), 
        m3=(open - mfirst(low, 2)) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = -rank(m1, percent=true) * rank(m2, percent=true) * rank(m3, percent=true) 
        context by trade_date;

        {table_name}.drop!(`m1`m2`m3);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha21(table_name: str, alpha_name: str = "wq_a21", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set cond1 = (msum(close, 8) / 8 + mstd(close, 8)) < (msum(close, 2) / 2),
        cond2 = (msum(close, 2) / 2) < (msum(close, 8) / 8 - mstd(close, 8)),
        cond3 = (1 < (volume / mavg(volume, 20))) || (volume / mavg(volume, 20) == 1) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = iif(cond1, -1, iif(cond2, 1, iif(cond3, 1, -1))); 

        {table_name}.drop!(`cond1`cond2`cond3);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha22(table_name: str, alpha_name: str = "wq_a22", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set delta = mcorr(high, volume, 5) - mfirst(mcorr(high, volume, 5), 6), 
        m1 = mstd(close, 20) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = -delta * rank(m1, percent=true)  
        context by trade_date;

        {table_name}.drop!(`delta`m1);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha23(table_name: str, alpha_name: str = "wq_a23", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set v = mfirst(high, 3) - high,
        cond = (msum(high, 20) / 20 < high) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = iif(cond, v, 0); 

        {table_name}.drop!(`cond`v);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha24(table_name: str, alpha_name: str = "wq_a24", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set cond = (msum(close, 100) / 100 - mfirst(msum(close, 100) / 100, 101)) / mfirst(close, 101) <= 0.05,
        v1 = -(close - mmin(close, 100)),
        v2 = -(close - mfirst(close, 4)) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = iif(cond, v1, v2); 

        {table_name}.drop!(`cond`v1`v2);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha25(table_name: str, alpha_name: str = "wq_a25", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set v = (-(ratios(close) - 1) * mavg(volume, 20) * vwap * (high -close)) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = rank(v, percent=true)  
        context by trade_date; 

        {table_name}.drop!(`v);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha26(table_name: str, alpha_name: str = "wq_a26", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set {alpha_name} = -mmax(mcorr(mrank(volume, true, 5), mrank(high, true, 5), 5), 3)  
        context by jj_code; 
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha27(table_name: str, alpha_name: str = "wq_a27", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set rank1=rank(volume, percent=true), rank2=rank(vwap, percent=true) 
        context by trade_date; 
        
        update {table_name} 
        set v = (msum(mcorr(rank1, rank2, 6), 2) / 2.0) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = iif(0.5 < rank(v, percent=true), -1, 1) 
        context by trade_date; 

        {table_name}.drop!(`rank1`v);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha28(table_name: str, alpha_name: str = "wq_a28", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set toscale = mcorr(mavg(volume, 20), low, 5) + ((high + low) / 2) - close  
        context by jj_code; 
        
        update {table_name} 
        set {alpha_name} = toscale / sum(abs(toscale)) 
        context by trade_date; 

        {table_name}.drop!(`toscale);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha29(table_name: str, alpha_name: str = "wq_a29", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set inner1 = (close - 1 - mfirst(close - 1, 6)) 
        context by jj_code; 
        
        update {table_name} 
        set inner2=rank(rank((-rank(inner1, percent=true)), percent=true), percent=true) 
        context by trade_date; 
        
        update {table_name} 
        set toscale=log(mmin(inner2, 2)) 
        context by jj_code;
        
        update {table_name} 
        set ranks=rank(rank(toscale / sum(abs(toscale)), percent=true), percent=true) 
        context by trade_date; 
        
        update {table_name} 
        set {alpha_name}=mmin(ranks, 5) + mrank(mfirst(-(ratios(close) - 1), 7), true, 5) 
        context by jj_code; 

        {table_name}.drop!(`inner1`inner2`toscale`ranks);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha30(table_name: str, alpha_name: str = "wq_a30", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set v = sign(close - mfirst(close, 2)) + sign(mfirst(close, 2) - mfirst(close, 3)) + sign(mfirst(close, 3) - mfirst(close, 4))  
        context by jj_code; 
        
        update {table_name} 
        set rank1 = rank(v, percent=true) 
        context by trade_date; 

        update {table_name} 
        set {alpha_name} = (1.0 - rank1) * msum(volume, 5) / msum(volume, 20) 
        context by jj_code; 

        {table_name}.drop!(`v`rank1);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha31(table_name: str, alpha_name: str = "wq_a31", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set v1=(close - mfirst(close, 11)), 
        v2=-(close - mfirst(close, 4)), 
        toscale=mcorr(mavg(volume, 20), low, 12) 
        context by jj_code; 

        update {table_name} 
        set v11 = -rank(rank(v1, percent=true), percent=true), 
        rank2=rank(v2, percent=true), 
        scale = toscale / sum(abs(toscale))  
        context by trade_date; 
        
        update {table_name} 
        set decay_linear=mavg(v11, 1..10) 
        context by jj_code;
        
        update {table_name} 
        set rank1=rank(rank(rank(decay_linear, percent=true), percent=true), percent=true) 
        context by trade_date; 

        update {table_name} 
        set {alpha_name} = rank1 + rank2 + sign(scale); 

        {table_name}.drop!(`v1`v2`toscale`v11`rank1`rank2`scale`decay_linear);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha32(table_name: str, alpha_name: str = "wq_a32", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set toscale1=msum(close, 7) / 7 - close, 
        toscale2=mcorr(vwap, mfirst(close, 6), 230) 
        context by jj_code; 

        update {table_name} 
        set scale1 = toscale1 / sum(abs(toscale1)),
        scale2 = toscale2 / sum(abs(toscale2)) 
        context by trade_date; 
        
        update {table_name} 
        set {alpha_name} = scale1 + 20 * scale2; 
        
        {table_name}.drop!(`toscale1`toscale2`scale1`scale2);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha33(table_name: str, alpha_name: str = "wq_a33", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set {alpha_name} = rank((open / close - 1), percent=true)  
        context by trade_date; 
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha34(table_name: str, alpha_name: str = "wq_a34", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set v1=(mstd(ratios(close) - 1, 2) / mstd(ratios(close) - 1, 5)), 
        v2=(close - mfirst(close, 2)) 
        context by jj_code; 

        update {table_name} 
        set {alpha_name} = rank(1 - rank(v1, percent=true) + 1 - rank(v2, percent=true), percent=true) 
        context by trade_date; 
        
        {table_name}.drop!(`v1`v2);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha35(table_name: str, alpha_name: str = "wq_a35", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set {alpha_name} = mrank(volume, true, 32) * (1 - mrank((close + high - low), true, 16)) * (1 - mrank((ratios(close) - 1), true, 32))  
        context by jj_code; 
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha36(table_name: str, alpha_name: str = "wq_a36", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set v1 = mcorr((close - open), mfirst(volume, 2), 15),
        v2 = (open - close),
        v3 = mrank(mfirst(-(ratios(close) - 1), 7), true, 5),
        v4 = abs(mcorr(vwap, mavg(volume, 20), 6)),
        v5 = (msum(close, 200) / 200 - open) * (close - open) 
        context by jj_code;
        
        update {table_name} 
        set {alpha_name} = 2.21 * rank(v1, percent=true) + 0.7 * rank(v2, percent=true) + 0.73 * rank(v3, percent=true) + rank(v4, percent=true) + 0.6 * rank(v5, percent=true) 
        context by trade_date; 
        
        {table_name}.drop!(`v1`v2`v3`v4`v5); 
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha37(table_name: str, alpha_name: str = "wq_a37", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set v1 = mcorr(mfirst((open - close), 2), close, 200), 
        v2 = (open - close) 
        context by jj_code; 

        update {table_name} 
        set {alpha_name} = rank(v1, percent=true) + rank(v2, percent=true) 
        context by trade_date; 

        {table_name}.drop!(`v1`v2);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha38(table_name: str, alpha_name: str = "wq_a38", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set v1 = mrank(close, true, 10), 
        v2 = (close / open) 
        context by jj_code; 

        update {table_name} 
        set {alpha_name} = -rank(v1, percent=true) * rank(v2, percent=true) 
        context by trade_date; 

        {table_name}.drop!(`v1`v2);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha39(table_name: str, alpha_name: str = "wq_a39", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set decay_linear = mavg((volume / mavg(volume, 20)), 1..9), 
        v1 = (close - mfirst(close, 8)),
        v2 = msum(ratios(close - 1), 250) 
        context by jj_code; 

        update {table_name} 
        set {alpha_name} = -rank(v1 * (1 - rank(decay_linear, percent=true)), percent=true) * (1 + rank(v2, percent=true)) 
        context by trade_date; 

        {table_name}.drop!(`decay_linear`v1`v2);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha40(table_name: str, alpha_name: str = "wq_a40", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set v1 = mstd(high, 10), 
        v2 = mcorr(high, volume, 10) 
        context by jj_code; 

        update {table_name} 
        set {alpha_name} = -rank(v1, percent=true) * v2 
        context by trade_date; 

        {table_name}.drop!(`v1`v2);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha41(table_name: str, alpha_name: str = "wq_a41", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set {alpha_name} = pow(high * low, 0.5) - vwap; 
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha42(table_name: str, alpha_name: str = "wq_a42", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set {alpha_name} = rank((vwap - close), percent=true) / rank((vwap + close), percent=true) 
        context by trade_date; 
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha43(table_name: str, alpha_name: str = "wq_a43", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set {alpha_name} = mrank((volume / mavg(volume, 20)), true, 20) * mrank(-(close - mfirst(close, 8)), true, 8) 
        context by jj_code; 
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha44(table_name: str, alpha_name: str = "wq_a44", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set v = rank(volume, percent=true) 
        context by trade_date;

        update {table_name} 
        set {alpha_name} = -mcorr(high, v, 5) 
        context by jj_code; 

        {table_name}.drop!(`v);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha45(table_name: str, alpha_name: str = "wq_a45", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set v1 = msum(mfirst(close, 6), 20) / 20,
        v2 = mcorr(close, volume, 2),
        v3 = mcorr(msum(close, 5), msum(close, 20), 2) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = -rank(v1, percent=true) * v2 * rank(v3, percent=true)  
        context by trade_date; 

        {table_name}.drop!(`v1`v2`v3);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha46(table_name: str, alpha_name: str = "wq_a46", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set cond = (mfirst(close, 21) - mfirst(close, 11)) / 10 - (mfirst(close, 11) - close) / 10 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = iif(0.25 < cond, -1, iif(cond < 0, 1, (mfirst(close, 2) - close)))  
        context by jj_code; 

        {table_name}.drop!(`cond);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha47(table_name: str, alpha_name: str = "wq_a47", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set m1 = volume / mavg(volume, 20),
        m2 = msum(high, 5) / 5, 
        m3 = vwap - mfirst(vwap, 6) 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = rank(1 / close, percent=true) * m1 * high * rank(high - close, percent=true) / m2 - rank(m3, percent=true) 
        context by trade_date; 

        {table_name}.drop!(`m1`m2`m3);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha49(table_name: str, alpha_name: str = "wq_a49", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set cond = ((mfirst(close, 21) - mfirst(close, 11)) / 10 - (mfirst(close, 11) - close) / 10) < -0.1 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = iif(cond, 1, mfirst(close, 2) - close) 
        context by jj_code; 

        {table_name}.drop!(`cond);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha50(table_name: str, alpha_name: str = "wq_a50", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set m1=rank(volume, percent=true), 
        m2=rank(vwap, percent=true) 
        context by trade_date;
        
        update {table_name} 
        set mcorr1 = mcorr(m1, m2, 5) 
        context by jj_code;
        
        update {table_name} 
        set rank1=rank(mcorr1, percent=true)  
        context by trade_date;

        update {table_name} 
        set {alpha_name} = -mmax(rank1, 5) 
        context by jj_code; 

        {table_name}.drop!(`m1`m2`mcorr1`rank1);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha51(table_name: str, alpha_name: str = "wq_a51", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set cond = (mfirst(close, 21) - mfirst(close, 11)) / 10 - (mfirst(close, 11) - close) / 10 < -0.05 
        context by jj_code;

        update {table_name} 
        set {alpha_name} = iif(cond, 1, -(close - mfirst(close, 2))) 
        context by jj_code; 

        {table_name}.drop!(`cond);
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha52(table_name: str, alpha_name: str = "wq_a52", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = (-mmin(low, 5) + mfirst(mmin(low, 5), 6)),
            m2 = (msum(ratios(close) - 1, 240) - msum(ratios(close) - 1, 220)) / 220, 
            m3 = mrank(volume, true, 5) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = m1 * rank(m2, percent=true) * m3 
            context by trade_date; 

            {table_name}.drop!(`m1`m2`m3);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha53(table_name: str, alpha_name: str = "wq_a53", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set {alpha_name} = -(((close - low) - (high - close)) / (close - low) - mfirst(((close - low) - (high - close)) / (close - low), 10)) 
        context by jj_code; 
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha54(table_name: str, alpha_name: str = "wq_a54", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set {alpha_name} = -(low - close) * pow(open, 5) / ((low - high) * pow(close, 5)) 
        context by jj_code; 
    """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha55(table_name: str, alpha_name: str = "wq_a55", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = (close - mmin(low, 12)) / (mmax(high, 12) - mmin(low, 12)) 
            context by jj_code;

            update {table_name} 
            set rank1 = rank(m1, percent=true), 
            rank2 = rank(volume, percent=true) 
            context by trade_date;
            
            update {table_name} 
            set {alpha_name} = -mcorr(rank1, rank2, 6) 
            context by jj_code;  

            {table_name}.drop!(`m1`rank1`rank2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha57(table_name: str, alpha_name: str = "wq_a57", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mimax(close, 30) 
            context by jj_code;

            update {table_name} 
            set rank1 = rank(m1, percent=true) 
            context by trade_date;

            update {table_name} 
            set {alpha_name} = -(close - vwap) / mavg(rank1, 1..2) 
            context by jj_code;  

            {table_name}.drop!(`m1`rank1);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha60(table_name: str, alpha_name: str = "wq_a60", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set toscale1 = rank(((close - low) - (high - close)) / (high - low) * volume, percent=true) 
            context by trade_date;
            
            update {table_name} 
            set scale1 = toscale1 / sum(abs(toscale1)) 
            context by trade_date;
            
            update {table_name} 
            set m1 = mimax(close, 10) 
            context by jj_code;
            
            update {table_name} 
            set toscale2 = rank(m1, percent=true) 
            context by trade_date;
            
            update {table_name} 
            set scale2 = toscale2 / sum(abs(toscale2)) 
            context by trade_date;

            update {table_name} 
            set {alpha_name} = -(2 * scale1 - scale2) 
            context by jj_code;  

            {table_name}.drop!(`toscale1`scale1`m1`toscale2`scale2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha61(table_name: str, alpha_name: str = "wq_a61", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = vwap - mmin(vwap, 16),
            m2 = mcorr(vwap, mavg(volume, 180), 18)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) < rank(m2, percent=true) 
            context by trade_date;  

            {table_name}.drop!(`m1`m2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha62(table_name: str, alpha_name: str = "wq_a62", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr(vwap, msum(mavg(volume, 20), 22), 10) 
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(m1, percent=true), 
            rank21 = (rank(open, percent=true) + rank(open, percent=true)),
            rank22 = (rank((high + low) / 2, percent=true) + rank(high, percent=true)) 
            context by trade_date;

            update {table_name} 
            set {alpha_name} = (rank1 < rank(rank21<rank22, percent=true)) * (-1) 
            context by trade_date; 

            {table_name}.drop!(`m1`rank1`rank21`rank22);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha64(table_name: str, alpha_name: str = "wq_a64", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr(msum(open * 0.178404 + low * (1 - 0.178404), 13), msum(mavg(volume, 120), 13), 17),
            m2 = mfirst(deltax, 5),
            deltax = (high + low) / 2 * 0.178404 + vwap * (1 - 0.178404)  
            context by jj_code;

            update {table_name} 
            set rank1 = rank(m1, percent=true), 
            rank2 = rank(deltax - m2, percent=true)  
            context by trade_date;

            update {table_name} 
            set {alpha_name} = (rank1 < rank2) * (-1) 
            context by trade_date; 

            {table_name}.drop!(`m1`m2`deltax`rank1`rank2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha65(table_name: str, alpha_name: str = "wq_a65", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr((open * 0.00817205 + vwap * (1 - 0.00817205)), msum(mavg(volume, 60), 9), 6),
            m2 = open - mmin(open, 14)  
            context by jj_code;

            update {table_name} 
            set rank1 = rank(m1, percent=true), 
            rank2 = rank(m2, percent=true)  
            context by trade_date;

            update {table_name} 
            set {alpha_name} = (rank1 < rank2) * (-1) 
            context by trade_date; 

            {table_name}.drop!(`m1`m2`rank1`rank2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha66(table_name: str, alpha_name: str = "wq_a66", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(vwap - mfirst(vwap, 5), 1..7),
            m2 = mrank(mavg((low - vwap) / (open - (high - low) / 2), 1..11), true, 11)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (rank(m1, percent=true) + m2) * (-1) 
            context by trade_date; 

            {table_name}.drop!(`m1`m2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha68(table_name: str, alpha_name: str = "wq_a68", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(volume, 15),
            deltax = close * 0.518371 + low * (1 - 0.518371)  
            context by jj_code;
            
            update {table_name} 
            set m2 = deltax - mfirst(deltax, 2)   
            context by jj_code;

            update {table_name} 
            set rank1 = rowRank(high, percent=true), 
            rank2 = rank(m1, percent=true),
            rank3 = rank(m2, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m3 = mrank(mcorr(rank1, rank2, 9), true, 14)
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (m3 < rank3) * (-1) 
            context by trade_date; 

            {table_name}.drop!(`m1`m2`m3`deltax`rank1`rank2`rank3);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha71(table_name: str, alpha_name: str = "wq_a71", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set decay_linear1 = mavg(mcorr(mrank(close, true, 3), mrank(mavg(volume, 180), true, 12), 18), 1..4)  
            context by jj_code;
            
            update {table_name} 
            set rank1 = mrank(decay_linear1, true, 16),
            decay_linear2 = mavg(pow(rowRank(low + open - (vwap + vwap), percent=true), 2), 1..16)  
            context by jj_code;
            
            update {table_name} 
            set rank2 = mrank(decay_linear2, true, 4)  
            context by jj_code;
            
            update {table_name} 
            set {alpha_name} = max(rank1, rank2) 
            context by jj_code; 

            {table_name}.drop!(`decay_linear1`rank1`decay_linear2`rank2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha72(table_name: str, alpha_name: str = "wq_a72", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(mcorr((high + low) / 2, mavg(volume, 40), 9), 1..10),
            m2 = mavg(mcorr(mrank(vwap, true, 4), mrank(volume, true, 19), 7), 1..3)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) / rank(m2, percent=true)
            context by trade_date; 

            {table_name}.drop!(`m1`m2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha73(table_name: str, alpha_name: str = "wq_a73", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(vwap - mfirst(vwap, 6), 1..3),
            deltax = open * 0.147155 + low * (1 - 0.147155)  
            context by jj_code;
            
            update {table_name} 
            set delta = deltax - mfirst(deltax, 3)   
            context by jj_code;
            
            update {table_name} 
            set rank2 = mrank(mavg(delta / deltax * (-1), 1..3), true, 17)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = max(rank(m1, percent=true), rank2) * (-1)  
            context by trade_date; 

            {table_name}.drop!(`m1`deltax`delta`rank2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha74(table_name: str, alpha_name: str = "wq_a74", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr(close, msum(mavg(volume, 30), 37), 15),
            m2 = high * 0.0261661 + vwap * (1 - 0.0261661)  
            context by jj_code;

            update {table_name} 
            set rank1 = rank(m1, percent=true),
            rank2 = rank(rank(m2, percent=true), percent=true),
            rank3 = rank(volume, percent=true)   
            context by trade_date;

            update {table_name} 
            set m3 = mcorr(rank2, rank3, 11)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (rank1 < rank(m3, percent=true)) * (-1)  
            context by trade_date; 

            {table_name}.drop!(`m1`m2`m3`rank1`rank2`rank3);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha75(table_name: str, alpha_name: str = "wq_a75", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr(vwap, volume, 4),
            m2 = mavg(volume, 50)  
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(low, percent=true), 
            rank2 = rank(m2, percent=true),
            rank3 = rank(m1, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m3 = mcorr(rank1, rank2, 12)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (rank3 < rank(m3, percent=true)) 
            context by trade_date; 

            {table_name}.drop!(`m1`m2`rank1`rank2`rank3);
        """

    return res, table_name, alpha_name, top_n

@finish_res
def wq_alpha77(table_name: str, alpha_name: str = "wq_a77", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg((high + low) / 2 + high - (vwap + high), 1..20),
            m2 = mavg(mcorr((high + low) / 2, mavg(volume, 40), 3), 1..6)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = min(rank(m1, percent=true), rank(m2, percent=true))  
            context by trade_date; 

            {table_name}.drop!(`m1`m2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha78(table_name: str, alpha_name: str = "wq_a78", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(vwap, percent=true), 
            rank2 = rank(volume, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m1 = mcorr(msum(low * 0.352233 + vwap * (1 - 0.352233), 20), msum(mavg(volume, 40), 20), 7),
            m2 = mcorr(rank1, rank2, 6)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = pow(rank(m1, percent=true), rank(m2, percent=true))  
            context by trade_date; 

            {table_name}.drop!(`m1`m2`rank1`rank2);
        """

    return res, table_name, alpha_name, top_n

@finish_res
def wq_alpha81(table_name: str, alpha_name: str = "wq_a81", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr(vwap, msum(mavg(volume, 10), 49), 8)  
            context by jj_code;

            update {table_name} 
            set rank1 = rank(pow(rank(m1, percent=true), 4), percent=true), 
            rank2 = rank(vwap, percent=true),
            rank3 = rank(volume, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m2 = log(mprod(rank1, 15)),
            m3 = mcorr(rank2, rank3, 5) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (rank(m2, percent=true) < rank(m3, percent=true)) * (-1) 
            context by trade_date; 

            {table_name}.drop!(`m1`m2`m3`rank1`rank2`rank3);
        """

    return res, table_name, alpha_name, top_n

@finish_res
def wq_alpha83(table_name: str, alpha_name: str = "wq_a83", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mfirst((high - low) / (msum(close, 5) / 5), 3),
            m2 = (((high - low) / (msum(close, 5) / 5)) / (vwap - close))  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) * rank(rank(volume, percent=true), percent=true) / m2   
            context by trade_date; 

            {table_name}.drop!(`m1`m2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha84(table_name: str, alpha_name: str = "wq_a84", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
        update {table_name} 
        set {alpha_name} = pow(mrank(vwap - mmax(vwap, 15), true, 20), close - mfirst(close, 6)) 
        context by jj_code; 
    """

    return res, table_name, alpha_name, top_n

@finish_res
def wq_alpha85(table_name: str, alpha_name: str = "wq_a85", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr(high * 0.876703 + close * (1 - 0.876703), mavg(volume, 30), 10),
            m2 = mcorr(mrank((high + low) / 2, true, 4), mrank(volume, true, 10), 7)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = pow(rank(m1, percent=true), rank(m2, percent=true))  
            context by trade_date; 

            {table_name}.drop!(`m1`m2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha86(table_name: str, alpha_name: str = "wq_a86", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(open + close - (vwap + open), percent=true)  
            context by trade_date;

            update {table_name} 
            set m1 = mrank(mcorr(close, msum(mavg(volume, 20), 15), 6), true, 20)  
            context by jj_code;


            update {table_name} 
            set {alpha_name} = (m1 < rank1) * (-1)  
            context by jj_code; 

            {table_name}.drop!(`m1`rank1);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha88(table_name: str, alpha_name: str = "wq_a88", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(open, percent=true) + rank(low, percent=true) - (rank(high, percent=true) + rank(close, percent=true))  
            context by trade_date;
            
            update {table_name} 
            set m1 = mavg(rank1, 1..8),
            m2 = mrank(mavg(mcorr(mrank(close, true, 8), mrank(mavg(volume, 60), true, 21), 8), 1..7), true, 3)  
            context by jj_code;


            update {table_name} 
            set {alpha_name} = min(rank(m1, percent=true), m2)  
            context by trade_date; 

            {table_name}.drop!(`m1`m2`rank1);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha92(table_name: str, alpha_name: str = "wq_a92", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mrank(mavg(((high + low) / 2 + close) < (low + open), 1..15), true, 19),
            m2 = mavg(volume, 30)  
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(low, percent=true), 
            rank2 = rank(m2, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m3 = mrank(mavg(mcorr(rank1, rank2, 8), 1..7), true, 7)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = min(m1, m3)  
            context by trade_date; 

            {table_name}.drop!(`m1`m2`m3`rank1`rank2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha94(table_name: str, alpha_name: str = "wq_a94", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = vwap - mmin(vwap, 12),
            m2 = mrank(mcorr(mrank(vwap, true, 20), mrank(mavg(volume, 60), true, 4), 18), true, 3)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = pow(rank(m1, percent=true), m2) * (-1)  
            context by trade_date; 

            {table_name}.drop!(`m1`m2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha95(table_name: str, alpha_name: str = "wq_a95", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = open - mmin(open, 12),
            m2 = mcorr(msum((high + low) / 2, 19), msum(mavg(volume, 40), 19), 13)  
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(m1, percent=true), 
            rank2 = rank(m2, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m3 =  mrank(pow(rank2, 5), true, 12)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank1 < m3  
            context by jj_code; 

            {table_name}.drop!(`m1`m2`m3`rank1`rank2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha96(table_name: str, alpha_name: str = "wq_a96", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(vwap, percent=true), 
            rank2 = rank(volume, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m1 = = mrank(mavg(mcorr(rank1, rank2, 4), 1..4), true, 8),
            m2 = mrank(mavg(mimax(mcorr(mrank(close, true, 7), mrank(mavg(volume, 60), true, 4), 4), 13), 1..14), true, 13)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * max(m1, m2)  
            context by jj_code; 

            {table_name}.drop!(`m1`m2`rank1`rank2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha98(table_name: str, alpha_name: str = "wq_a98", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(mcorr(vwap, msum(mavg(volume, 5), 26), 5), 1..7),
            m2 = mavg(volume, 15)  
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(m1, percent=true), 
            rank2 = rank(open, percent=true),  
            rank3 = rank(m2, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m3 = mavg(mrank(9 - mimin(mcorr(rank2, rank3, 21), 9), true, 7), 1..8)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank1 - rank(m3, percent=true) 
            context by trade_date; 

            {table_name}.drop!(`m1`m2`m3`rank1`rank2`rank3);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha99(table_name: str, alpha_name: str = "wq_a99", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr(msum((high + low) / 2, 20), msum(mavg(volume, 60), 20), 9),
            m2 = mcorr(low, volume, 6)  
            context by jj_code;

            update {table_name} 
            set rank1 = rank(m1, percent=true), 
            rank2 = rank(m2, percent=true)  
            context by trade_date;

            update {table_name} 
            set {alpha_name} = (rank1 < rank2) * (-1) 
            context by trade_date; 

            {table_name}.drop!(`m1`m2`rank1`rank2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def wq_alpha101(table_name: str, alpha_name: str = "wq_a101", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (close - open) / (high - low + 0.001)  
            context by jj_code; 
        """

    return res, table_name, alpha_name, top_n


if __name__ == '__main__':
    r0 = wq_alpha1(table_name="t4")
    r1 = wq_alpha1(table_name="t4", top_n=19)
    r2 = wq_alpha1(table_name="t4", alpha_name="param4", top_n=19)
    print(1)

    r0 = wq_alpha4(table_name="t4")
    r1 = wq_alpha4(table_name="t4", top_n=19)
    r2 = wq_alpha4(table_name="t4", alpha_name="param4", top_n=19)
    print(1)
