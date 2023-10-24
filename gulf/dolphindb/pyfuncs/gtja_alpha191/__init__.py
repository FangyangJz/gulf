# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2022/12/2 14:51
# @Author   : Fangyang
# @Software : PyCharm

from typing import Tuple
from gulf.dolphindb.pyfuncs import finish_res


@finish_res
def gtja_alpha1(table_name: str, alpha_name: str = "gtja_a1", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set delta = log(volume) - mfirst(log(volume), 2)  
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(delta, percent=true), 
            rank2 = rank((close - open) / open, percent=true) 
            context by trade_date;
            
            update {table_name} 
            set {alpha_name} = -1 * mcorr(rank1, rank2, 6) 
            context by jj_code;

            {table_name}.drop!(`delta`rank1`rank2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha2(table_name: str, alpha_name: str = "gtja_a2", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set tmp = (close - low - (high - close)) / (high - low)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * (tmp - mfirst(tmp, 2)) 
            context by jj_code;

            {table_name}.drop!(`tmp);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha3(table_name: str, alpha_name: str = "gtja_a3", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set tmp = iif(close > mfirst(close, 2), min(low, mfirst(close, 2)), max(high, mfirst(close, 2)))  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = msum(iif(close == mfirst(close, 2), 0, close - tmp), 6) 
            context by jj_code;

            {table_name}.drop!(`tmp);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha4(table_name: str, alpha_name: str = "gtja_a4", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set cond1 = ((msum(close, 8) / 8 + mstd(close, 8)) < (msum(close, 2) / 2)),
            cond2 = ((msum(close, 2) / 2) < (msum(close, 8) / 8 - mstd(close, 8))),
            iffalse2 = iif((1 < (volume / mavg(volume, 20))) || (volume / mavg(volume, 20) == 1), 1, -1)  
            context by jj_code;
            
            update {table_name} 
            set iffalse1 = iif(cond2, 1, iffalse2)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = iif(cond1, -1, iffalse1) 
            context by jj_code;

            {table_name}.drop!(`cond1`cond2`iffalse2`iffalse1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha5(table_name: str, alpha_name: str = "gtja_a5", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = -1 * mmax(mcorr(mrank(volume, true, 5), mrank(high, true, 5), 5), 3) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha6(table_name: str, alpha_name: str = "gtja_a6", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set delta = (open * 0.85 + high * 0.15) - mfirst((open * 0.85 + high * 0.15), 5)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(sign(delta), percent=true) 
            context by trade_date;

            {table_name}.drop!(`delta);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha7(table_name: str, alpha_name: str = "gtja_a7", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mmax(vwap - close, 3),
            m2 = mmin(vwap - close, 3),
            m3 = volume - mfirst(volume, 4)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) + rank(m2, percent=true) * rank(m3, percent=true) 
            context by trade_date;

            {table_name}.drop!(`m1`m2`m3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha8(table_name: str, alpha_name: str = "gtja_a8", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set tmp = (high + low) / 2 * 0.2 + vwap * 0.8 
            context by jj_code;
            
            update {table_name} 
            set rank1 = (tmp - mfirst(tmp, 5)) * (-1) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(rank1, percent=true) 
            context by trade_date;

            {table_name}.drop!(`tmp`rank1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha9(table_name: str, alpha_name: str = "gtja_a9", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = ((high + low) / 2 - (mfirst(high, 2) + mfirst(low, 2)) / 2) * (high - low) / volume 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A, alpha=2\\7) 
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha10(table_name: str, alpha_name: str = "gtja_a10", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = mmax(pow(iif((ratios(close) - 1) < 0, mstd(ratios(close) - 1, 20), close), 2), 5) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(rank1, percent=true) 
            context by trade_date;

            {table_name}.drop!(`rank1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha11(table_name: str, alpha_name: str = "gtja_a11", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum((close - low - (high - close)) / (high - low) * volume, 6) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha12(table_name: str, alpha_name: str = "gtja_a12", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = open - msum(vwap, 10) / 10 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(rank1, percent=true) * (-1 * rank(abs(close - vwap), percent=true)) 
            context by trade_date;

            {table_name}.drop!(`rank1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha13(table_name: str, alpha_name: str = "gtja_a13", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = pow((high * low), 0.5) - vwap 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha14(table_name: str, alpha_name: str = "gtja_a14", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = close - mfirst(close, 6) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha15(table_name: str, alpha_name: str = "gtja_a15", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = open / mfirst(close, 2) - 1 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha16(table_name: str, alpha_name: str = "gtja_a16", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(volume, percent=true), 
            rank2 = rank(vwap, percent=true) 
            context by trade_date;
            
            update {table_name} 
            set m1 = mcorr(rank1, rank2, 5) 
            context by jj_code;
            
            update {table_name} 
            set rank3 = rank(m1, percent=true) 
            context by trade_date;

            update {table_name} 
            set {alpha_name} = -1 * mmax(rank3, 5) 
            context by jj_code;

            {table_name}.drop!(`rank1`rank2`m1`rank3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha17(table_name: str, alpha_name: str = "gtja_a17", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = vwap - mmax(vwap, 15),
            m2 = close - mfirst(close, 6) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = pow(rank(m1, percent=true), m2) 
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha18(table_name: str, alpha_name: str = "gtja_a18", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = close / mfirst(close, 6) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha19(table_name: str, alpha_name: str = "gtja_a19", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = iif(close < mfirst(close, 6), (close - mfirst(close, 6)) / mfirst(close, 6), iif(close == mfirst(close, 6), 0, (close - mfirst(close, 6)) / close)) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha20(table_name: str, alpha_name: str = "gtja_a20", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (close - mfirst(close, 7)) / mfirst(close, 7) * 100 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha21(table_name: str, alpha_name: str = "gtja_a21", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = linearTimeTrend(close,6)[1] 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha22(table_name: str, alpha_name: str = "gtja_a22", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = (close - mavg(close,6)) / mavg(close,6) - move((close - mavg(close,6)) / mavg(close,6),3) 
            context by jj_code;
            
            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\12) 
            context by jj_code;
            
            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha23(table_name: str, alpha_name: str = "gtja_a23", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = iif(close > move(close,1), mstd(close,20), 0),
            B = iif(close <= move(close,1), mstd(close,20), 0) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\20)/(ewmMean(A,alpha=1\\20) + ewmMean(B,alpha=1\\20))*100 
            context by jj_code;
            
            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha24(table_name: str, alpha_name: str = "gtja_a24", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = close - move(close,5) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\5) 
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha25(table_name: str, alpha_name: str = "gtja_a25", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = (close - mfirst(close, 8)),
            m2 = mavg(volume / mavg(volume, 20), 1..9),
            m3 = msum(ratios(close) - 1, 250) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank( m1 * (1 - rank(m2, percent=true)), percent=true) * (1 + rank(m3, percent=true)) 
            context by trade_date;

            {table_name}.drop!(`m1`m2`m3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha26(table_name: str, alpha_name: str = "gtja_a26", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(close, 7) / 7 - close + mcorr(vwap, mfirst(close, 6), 230) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha27(table_name: str, alpha_name: str = "gtja_a27", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mavg((close-move(close,3))/move(close,3)*100 + (close-move(close,6))/move(close,6)*100, 1..12 * 0.9, 12)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha28(table_name: str, alpha_name: str = "gtja_a28", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = (close - mmin(low,9))/(mmax(high,9)-mmin(low,9)*100) 
            context by jj_code;
            
            update {table_name} 
            set B = ewmMean(A,alpha=1\\3) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = 3*B - 2*ewmMean(B,alpha=1\\3) 
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha29(table_name: str, alpha_name: str = "gtja_a29", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (close - mfirst(close, 7)) / mfirst(close, 7) * volume 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha31(table_name: str, alpha_name: str = "gtja_a31", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (close - mavg(close, 12)) / mavg(close, 12) * 100 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha32(table_name: str, alpha_name: str = "gtja_a32", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(high, percent=true), 
            rank2 = rank(volume, percent=true) 
            context by trade_date;
            
            update {table_name} 
            set m1 = mcorr(rank1, rank2, 3) 
            context by jj_code;
            
            update {table_name} 
            set rank3 = rank(m1, percent=true) 
            context by trade_date;

            update {table_name} 
            set {alpha_name} = -1 * msum(rank3, 3) 
            context by jj_code;

            {table_name}.drop!(`rank1`rank2`m1`rank3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha33(table_name: str, alpha_name: str = "gtja_a33", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = (-1 * mmin(low, 5) + mfirst(mmin(low, 5), 6)),
            m2 = msum(ratios(close) - 1, 240) - msum(ratios(close) - 1, 20) / 220,
            m3 = mrank(volume, true, 5) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = m1 * rank(m2, percent=true) * m3 
            context by trade_date;

            {table_name}.drop!(`m1`m2`m3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha34(table_name: str, alpha_name: str = "gtja_a34", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mavg(close, 12) / close 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha35(table_name: str, alpha_name: str = "gtja_a35", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(open - mfirst(open, 2), 1..15),
            m2 = mavg(mcorr(volume, open * 0.65 + open * 0.35, 17), 1..7) 
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(m1, percent=true),
            rank2 = rank(m2, percent=true) 
            context by trade_date;

            update {table_name} 
            set {alpha_name} = min(rank1, rank2) * (-1) 
            context by trade_date;

            {table_name}.drop!(`m1`m2`rank1`rank2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha36(table_name: str, alpha_name: str = "gtja_a36", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(volume, percent=true),
            rank2 = rank(vwap, percent=true) 
            context by trade_date;
            
            update {table_name} 
            set m1 = msum(mcorr(rank1, rank2, 6), 2) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) 
            context by trade_date;

            {table_name}.drop!(`m1`rank1`rank2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha37(table_name: str, alpha_name: str = "gtja_a37", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = msum(open, 5) * msum(ratios(close) - 1, 5) - mfirst(msum(open, 5) * msum(ratios(close) - 1, 5), 11) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(m1, percent=true) 
            context by trade_date;

            {table_name}.drop!(`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha38(table_name: str, alpha_name: str = "gtja_a38", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = iif((msum(high, 20) / 20) < high, -1 * (high - mfirst(high, 3)), 0) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha39(table_name: str, alpha_name: str = "gtja_a39", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(close - mfirst(close, 3), 1..8),
            m2 = mavg(mcorr(vwap * 0.3 + open * 0.7, msum(mavg(volume, 180), 37), 14), 1..12) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m2, percent=true) - rank(m1, percent=true) 
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha40(table_name: str, alpha_name: str = "gtja_a40", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(iif(close > mfirst(close, 2), volume, 0), 26) / msum(iif(close <= mfirst(close, 2), volume, 0), 26) * 100 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha41(table_name: str, alpha_name: str = "gtja_a41", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mmax(vwap - mfirst(vwap, 4), 5) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) * (-1) 
            context by trade_date;

            {table_name}.drop!(`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha42(table_name: str, alpha_name: str = "gtja_a42", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mstd(high, 10),
            m2 = mcorr(high, volume, 10) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (-1) * rank(m1, percent=true) * m2 
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha43(table_name: str, alpha_name: str = "gtja_a43", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(iif(close > mfirst(close, 2), volume, iif(close < mfirst(close, 2), -volume, 0)), 6) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha44(table_name: str, alpha_name: str = "gtja_a44", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mrank(mavg(mcorr(low, mavg(volume, 10), 7), 1..6), true, 4) + mrank(mavg(vwap - mfirst(vwap, 4), 1..10), true, 15) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha45(table_name: str, alpha_name: str = "gtja_a45", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = close * 0.6 + open * 0.4 - mfirst(close * 0.6 + open * 0.4, 2),
            m2 = mcorr(vwap, mavg(volume, 150), 15) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) * rank(m2, percent=true) 
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha46(table_name: str, alpha_name: str = "gtja_a46", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (mavg(close, 3) + mavg(close, 6) + mavg(close, 12) + mavg(close, 24)) / (4 * close) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha47(table_name: str, alpha_name: str = "gtja_a47", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = (mmax(high,6)-close)/(mmax(high,6)-mmin(low,6))*100 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\9) 
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha48(table_name: str, alpha_name: str = "gtja_a48", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = sign(close - mfirst(close, 2)) + sign(mfirst(close, 2) - mfirst(close, 3)) + sign(mfirst(close, 3) - mfirst(close, 4)),
            m2 = msum(volume, 5) / msum(volume, 20) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(m1, percent=true) * m2  
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha49(table_name: str, alpha_name: str = "gtja_a49", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set sum1 = msum(iif((high + low) >= (mfirst(high, 2) + mfirst(low, 2)), 0, max(abs(high - mfirst(high, 2)), abs(low - mfirst(low, 2)))), 12),
            sum2 = msum(iif((high + low) <= (mfirst(high, 2) + mfirst(low, 2)), 0, max(abs(high - mfirst(high, 2)), abs(low - mfirst(low, 2)))), 12)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = sum1 / (sum1 + sum2)  
            context by jj_code;

            {table_name}.drop!(`sum1`sum2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha50(table_name: str, alpha_name: str = "gtja_a50", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set sum1 = msum(iif((high + low) <= (mfirst(high, 2) + mfirst(low, 2)), 0, max(abs(high - mfirst(high, 2)), abs(low - mfirst(low, 2)))), 12),
            sum2 = msum(iif((high + low) >= (mfirst(high, 2) + mfirst(low, 2)), 0, max(abs(high - mfirst(high, 2)), abs(low - mfirst(low, 2)))), 12)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = sum1 / (sum1 + sum2) - sum2 / (sum1 + sum2)  
            context by jj_code;

            {table_name}.drop!(`sum1`sum2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha51(table_name: str, alpha_name: str = "gtja_a51", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set sum1 = msum(iif((high + low) <= (mfirst(high, 2) + mfirst(low, 2)), 0, max(abs(high - mfirst(high, 2)), abs(low - mfirst(low, 2)))), 12),
            sum2 = msum(iif((high + low) >= (mfirst(high, 2) + mfirst(low, 2)), 0, max(abs(high - mfirst(high, 2)), abs(low - mfirst(low, 2)))), 12)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = sum1 / (sum1 + sum2)  
            context by jj_code;

            {table_name}.drop!(`sum1`sum2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha52(table_name: str, alpha_name: str = "gtja_a52", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(max(0, high - mfirst((high + low + close) / 3, 2)), 26) / msum(max(0, mfirst((high + low + close) / 3, 2) - low), 26) * 100  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha53(table_name: str, alpha_name: str = "gtja_a53", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mcount(iif(close > mfirst(close, 2), 1, NULL), 12) / 12 * 100  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha54(table_name: str, alpha_name: str = "gtja_a54", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mstd(abs(close - open),10) + close - open + mcorr(close, open, 10) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (-1) * rank(m1, percent=true)
            context by trade_date;

            {table_name}.drop!(`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha55(table_name: str, alpha_name: str = "gtja_a55", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set tmp1 = 16 * (close - mfirst(close, 2) + (close - open) / 2 + mfirst(close, 2) - mfirst(open, 2)),
            cond = abs(high - mfirst(close, 2)) > abs(low - mfirst(close, 2)) && abs(high - mfirst(close, 2)) > abs(high - mfirst(low, 2)),
            iftrue = abs(high - mfirst(close, 2)) + abs(low - mfirst(close, 2)) / 2 + abs(mfirst(close, 2) - mfirst(open, 2)) / 4,
            cond2 = abs(low - mfirst(close, 2)) > abs(high - mfirst(low, 2)) && abs(low - mfirst(close, 2)) > abs(high - mfirst(close, 2)),
            iftrue2 = abs(low - mfirst(close, 2)) + abs(high - mfirst(low, 2)) / 2 + abs(mfirst(close, 2) - mfirst(open, 2)) / 4,
            iffalse2 = abs(high - mfirst(low, 2)) + abs(mfirst(close, 2) - mfirst(open, 2)) / 4  
            context by jj_code;
            
            update {table_name} 
            set iffalse = iif(cond2, iftrue2, iffalse2)  
            context by jj_code;
            
            update {table_name} 
            set tmp2 = iif(cond, iftrue, iffalse),
            tmp3 = max(abs(high - mfirst(close, 2)), abs(low - mfirst(close, 2))) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = msum(tmp1 / tmp2 * tmp3, 20)
            context by jj_code;

            {table_name}.drop!(`tmp1`cond`iftrue`cond2`iftrue2`iffalse2`iffalse`tmp2`tmp3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha56(table_name: str, alpha_name: str = "gtja_a56", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = open - mmin(open, 12),
            m2 = mcorr(msum((high + low) / 2, 19), msum(mavg(volume, 40), 19), 13)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) < rank(pow(rank(m2, percent=true), 5), percent=true)
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha57(table_name: str, alpha_name: str = "gtja_a57", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = (close - mmin(low,9))/(mmax(high,9) - mmin(low,9))*100 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\3) 
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha58(table_name: str, alpha_name: str = "gtja_a58", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mcount(iif(close > mfirst(close, 2), 1, NULL), 20) / 20 * 100  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha59(table_name: str, alpha_name: str = "gtja_a59", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(iif(close == mfirst(close, 2), 0, close - iif(close > mfirst(close, 2), min(low, mfirst(close, 2)), max(high, mfirst(close, 2)))), 20)   
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha60(table_name: str, alpha_name: str = "gtja_a60", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum((close - low - (high - close)) / (high - low) * volume, 20)   
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha61(table_name: str, alpha_name: str = "gtja_a61", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(vwap - mfirst(vwap, 2), 1..12),
            m2 = mcorr(low, mavg(volume, 80), 8) 
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(m1, percent=true),
            rank2 = rank(m2, percent=true) 
            context by trade_date;

            update {table_name} 
            set m3 = mavg(rank2, 1..17)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * max(rank1, rank(m3, percent=true)) 
            context by trade_date;

            {table_name}.drop!(`m1`m2`m3`rank1`rank2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha62(table_name: str, alpha_name: str = "gtja_a62", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(volume, percent=true)  
            context by trade_date;

            update {table_name} 
            set {alpha_name} = -1 * mcorr(high, rank1, 5) 
            context by jj_code;

            {table_name}.drop!(`rank1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha63(table_name: str, alpha_name: str = "gtja_a63", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = max((close - move(close,1)),0),
            B = abs(close - move(close,1))  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\6) / ewmMean(B,alpha=1\\6) * 100  
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha64(table_name: str, alpha_name: str = "gtja_a64", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(volume, 60) 
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(vwap, percent=true),
            rank2 = rank(volume, percent=true),
            rank3 = rank(close, percent=true),
            rank4 = rank(m1, percent=true)  
            context by trade_date;

            update {table_name} 
            set m2 = mavg(mcorr(rank1, rank2, 4), 1..4),
            m3 = mavg(max(mcorr(rank3, rank4, 4), 13), 1..14)  
            context by jj_code;
            
            update {table_name} 
            set rank5 = rank(m2, percent=true),
            rank6 = rank(m3, percent=true)  
            context by trade_date;

            update {table_name} 
            set {alpha_name} = -1 * max(rank5, rank6) 
            context by trade_date;

            {table_name}.drop!(`m1`m2`m3`rank1`rank2`rank3`rank4`rank5`rank6);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha65(table_name: str, alpha_name: str = "gtja_a65", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mavg(close, 6) / close   
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha66(table_name: str, alpha_name: str = "gtja_a66", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (close - mavg(close, 6)) / mavg(close, 6) * 100  
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha67(table_name: str, alpha_name: str = "gtja_a67", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = max((close - move(close,1)),0),
            B = abs(close - move(close,1))  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\24) / ewmMean(B,alpha=1\\24) * 100  
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha68(table_name: str, alpha_name: str = "gtja_a68", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = ((high + low)/2 - (move(high,1)+move(low,1))/2)*(high-low)/volume  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=2\\15)  
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha69(table_name: str, alpha_name: str = "gtja_a69", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set DTM = iif(open <= mfirst(open, 2), 0, max(high - open, open - mfirst(open, 2))),
            DBM = iif(open >= mfirst(open, 2), 0, max(open - low, open - mfirst(open, 2)))   
            context by jj_code;

            update {table_name} 
            set {alpha_name} = iif(msum(DTM, 20) > msum(DBM, 20), (msum(DTM,20) - msum(DBM,20)) / msum(DTM,20), iif(msum(DTM,20) == msum(DBM,20), 0, (msum(DTM,20) - msum(DBM,20)) / msum(DBM,20)))   
            context by jj_code;

            {table_name}.drop!(`DTM`DBM);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha70(table_name: str, alpha_name: str = "gtja_a70", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mstd(volume * vwap, 6)  
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha71(table_name: str, alpha_name: str = "gtja_a71", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (close - mavg(close, 24)) / mavg(close, 24) * 100  
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha72(table_name: str, alpha_name: str = "gtja_a72", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = (mmax(high,6)-close)/(mmax(high,6)-mmin(low,6))*100  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\15)  
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha73(table_name: str, alpha_name: str = "gtja_a73", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mrank(mavg(mavg(mcorr(close, volume, 10), 1..16), 1..4), true, 5),
            m2 = mavg(mcorr(vwap, mavg(volume, 30), 4), 1..3)   
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * (m1 - rank(m2, percent=true)) 
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha74(table_name: str, alpha_name: str = "gtja_a74", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(vwap, percent=true), 
            rank2 = rank(volume, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m1 = mcorr(msum(low * 0.35 + vwap * 0.65, 20), msum(mavg(volume, 40), 20), 7),
            m2 = mcorr(rank1, rank2, 6)    
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) + rank(m2, percent=true)  
            context by trade_date;

            {table_name}.drop!(`rank1`rank2`m1`m2);
        """

    return res, table_name, alpha_name, top_n


# TODO 涉及 index 指数
@finish_res
def gtja_alpha75(table_name: str, alpha_name: str = "gtja_a75", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mcount(iif(close>open,1,0) * iif(index_close<index_open,1,0), 50) / mcount(iif(index_close<index_open,1,0), 50)   
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha76(table_name: str, alpha_name: str = "gtja_a76", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} =  mstd(abs(close / mfirst(close, 2) - 1) / volume, 20) / mavg(abs(close / mfirst(close, 2) - 1) / volume, 20)   
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha77(table_name: str, alpha_name: str = "gtja_a77", top_n: int = None) -> Tuple[str, str, str, int]:
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
def gtja_alpha78(table_name: str, alpha_name: str = "gtja_a78", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = ((high + low + close) / 3 - mavg((high + low + close) / 3, 12)) / (0.015 * mavg(abs(close - mavg((high + low + close) / 3, 12)), 12))   
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha79(table_name: str, alpha_name: str = "gtja_a79", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = max((close - move(close,1)),0),
            B = abs(close - move(close,1))  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\12) / ewmMean(B,alpha=1\\12) * 100  
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha80(table_name: str, alpha_name: str = "gtja_a80", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (volume - move(volume, 5)) / move(volume, 5) * 100   
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha81(table_name: str, alpha_name: str = "gtja_a81", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = ewmMean(volume,alpha=1\\21)   
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha82(table_name: str, alpha_name: str = "gtja_a82", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = (mmax(high,6)-close)/(mmax(high,6)-mmin(low,6))*100  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\20)  
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha83(table_name: str, alpha_name: str = "gtja_a83", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(high, percent=true), 
            rank2 = rank(volume, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m1 = mcovar(rank1, rank2, 5)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(m1, percent=true) 
            context by trade_date;

            {table_name}.drop!(`rank1`rank2`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha84(table_name: str, alpha_name: str = "gtja_a84", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} =  msum(iif(close > mfirst(close, 2), volume, iif(close < mfirst(close, 2), -volume, 0)), 20)   
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha85(table_name: str, alpha_name: str = "gtja_a85", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mrank(volume / mavg(volume, 20), true, 20) * mrank(-1 * (close - mfirst(close, 8)), true, 8)   
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha86(table_name: str, alpha_name: str = "gtja_a86", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set cond1 = (0.25 < ((mfirst(close, 21) - mfirst(close, 11)) / 10 - (mfirst(close, 11) - close) / 10)),
            cond2 = (((mfirst(close, 21) - mfirst(close, 11)) / 10 - (mfirst(close, 11) - close) / 10) < 0)   
            context by jj_code;
            
            update {table_name} 
            set iffalse = iif(cond2, 1, -1 * 1 * (close - mfirst(close, 2)))  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = iif(cond1, -1 * 1, iffalse)  
            context by jj_code;

            {table_name}.drop!(`cond1`cond2`iffalse);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha87(table_name: str, alpha_name: str = "gtja_a87", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(vwap - mfirst(vwap, 5), 1..7),
            m2 = mrank(mavg((low * 0.9 + low * 0.1 - vwap) / (open - (high + low) / 2), 1..11), true, 7)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * (rank(m1, percent=true) + m2) 
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha88(table_name: str, alpha_name: str = "gtja_a88", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (close - mfirst(close, 21)) / mfirst(close, 21) * 100 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha89(table_name: str, alpha_name: str = "gtja_a89", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = ewmMean(close,alpha=2\\13)  
            context by jj_code;
            
            update {table_name} 
            set B = ewmMean(A,alpha=2\\27)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = 2*(A - B - ewmMean(A-B,alpha=2\\10))  
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha90(table_name: str, alpha_name: str = "gtja_a90", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(vwap, percent=true), 
            rank2 = rank(volume, percent=true)  
            context by trade_date;

            update {table_name} 
            set m1 = mcorr(rank1, rank2, 5)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(m1, percent=true) 
            context by trade_date;

            {table_name}.drop!(`rank1`rank2`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha91(table_name: str, alpha_name: str = "gtja_a91", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = close - max(close, 5),
            m2 = mcorr(mavg(volume, 40), low, 5)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(m1, percent=true) * rank(m2, percent=true)  
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha92(table_name: str, alpha_name: str = "gtja_a92", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(close * 0.35 + vwap * 0.65 - mfirst(close * 0.35 + vwap * 0.65, 3), 1..3),
            m2 = mrank(mavg(abs(mcorr(mavg(volume, 180), close, 13)), 1..5), true, 15)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} =  max(rank(m1, percent=true), m2)  
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha93(table_name: str, alpha_name: str = "gtja_a93", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(iif(open >= mfirst(open, 2), 0, max(open - low, open - mfirst(open, 2))), 20)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha94(table_name: str, alpha_name: str = "gtja_a94", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(iif(close > mfirst(close, 2), volume, iif(close < mfirst(close, 2), -volume, 0)), 30)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha95(table_name: str, alpha_name: str = "gtja_a95", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mstd(volume * vwap, 20)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha96(table_name: str, alpha_name: str = "gtja_a96", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = (close - mmin(low,9))/(mmax(high,9) - mmin(low,9))*100   
            context by jj_code;
            
            update {table_name} 
            set B = ewmMean(A,alpha=1\\3)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(B,alpha=1\\3)  
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha97(table_name: str, alpha_name: str = "gtja_a97", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mstd(volume, 10)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha98(table_name: str, alpha_name: str = "gtja_a98", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set cond1 = ((msum(close, 100) / 100 - mfirst(msum(close, 100) / 100, 101)) / mfirst(close, 101) < 0.05),
            cond2 = ((msum(close, 100) / 100 - mfirst(msum(close, 100) / 100, 101)) / mfirst(close, 101) == 0.05)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = iif(cond1 || cond2, -1 * (close - mmin(close, 100)), -1 * (close - mfirst(close, 4)))  
            context by jj_code;

            {table_name}.drop!(`cond1`cond2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha99(table_name: str, alpha_name: str = "gtja_a99", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(close, percent=true), 
            rank2 = rank(volume, percent=true)  
            context by trade_date;

            update {table_name} 
            set m1 = mcovar(rank1, rank2, 5)   
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(m1, percent=true)   
            context by trade_date;

            {table_name}.drop!(`rank1`rank2`m1);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha100(table_name: str, alpha_name: str = "gtja_a100", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mstd(volume, 20)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha101(table_name: str, alpha_name: str = "gtja_a101", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(high * 0.1 + vwap * 0.9, percent=true), 
            rank2 = rank(volume, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m1 = mcorr(close, msum(mavg(volume, 30), 37), 15),
            m2 = mcorr(rank1, rank2, 11)    
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (rank(m1, percent=true) < rank(m2, percent=true)) * (-1) 
            context by trade_date;

            {table_name}.drop!(`rank1`rank2`m1`m2);
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha102(table_name: str, alpha_name: str = "gtja_a102", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set  A = max((volume-move(volume,1)),0),
            B = abs(volume-move(volume,1))  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\6) \\ ewmMean(B,alpha=1\\6) * 100  
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha103(table_name: str, alpha_name: str = "gtja_a103", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (20 - (19 - mimin(low, 20))) \\ 20 * 100   
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha104(table_name: str, alpha_name: str = "gtja_a104", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = (mcorr(high, volume, 5) - mfirst(mcorr(high, volume, 5), 5)),
            m2 = mstd(close, 20)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * m1 * rank(m2, percent=true)  
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha105(table_name: str, alpha_name: str = "gtja_a105", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(open, percent=true), 
            rank2 = rank(volume, percent=true)  
            context by trade_date;

            update {table_name} 
            set {alpha_name} = -1 * mcorr(rank1, rank2, 10)   
            context by jj_code;

            {table_name}.drop!(`rank1`rank2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha106(table_name: str, alpha_name: str = "gtja_a106", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = close - mfirst(close, 21)   
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha107(table_name: str, alpha_name: str = "gtja_a107", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = open - mfirst(high, 2),
            m2 = open - mfirst(close, 2),
            m3 = open - mfirst(low, 2)   
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(m1, percent=true) * rank(m2, percent=true) * rank(m3, percent=true)  
            context by trade_date;

            {table_name}.drop!(`m1`m2`m3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha108(table_name: str, alpha_name: str = "gtja_a108", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = high - min(high, 2),
            m2 = mcorr(vwap, mavg(volume, 120), 6)   
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * pow(rank(m1, percent=true), rank(m2, percent=true))  
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha109(table_name: str, alpha_name: str = "gtja_a109", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = ewmMean(high-low,alpha=2\\10)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = A / ewmMean(A,alpha=2\\10)  
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha110(table_name: str, alpha_name: str = "gtja_a110", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(max(0, high - mfirst(close, 2)), 20) / msum(max(0, mfirst(close, 2) - low), 20) * 100   
            context by jj_code;
        """

    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha111(table_name: str, alpha_name: str = "gtja_a111", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A =  volume*((close-low)-(high-close))/(high-low)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=2\\11) - ewmMean(A,alpha=2\\4)  
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha112(table_name: str, alpha_name: str = "gtja_a112", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set sum1 = msum(iif((close - mfirst(close, 2)) > 0, close - mfirst(close, 2), 0), 12),
            sum2 = msum(iif((close - mfirst(close, 2)) < 0, abs(close - mfirst(close, 2)), 0), 12)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (sum1 - sum2) / (sum1 + sum2) * 100  
            context by jj_code;

            {table_name}.drop!(`sum1`sum2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha113(table_name: str, alpha_name: str = "gtja_a113", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = msum(mfirst(close, 6), 20) \\ 20,
            m2 = mcorr(close, volume, 2),
            m3 = mcorr(msum(close, 5), msum(close, 20), 2)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(m1, percent=true) * m2 * rank(m3, percent=true)  
            context by trade_date;

            {table_name}.drop!(`m1`m2`m3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha114(table_name: str, alpha_name: str = "gtja_a114", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mfirst((high - low) / (msum(close, 5) / 5), 3),
            m2 =  ((high - low) / (msum(close, 5) / 5) / (vwap - close))  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) * rank(rank(volume, percent=true), percent=true) / m2  
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha115(table_name: str, alpha_name: str = "gtja_a115", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr(high * 0.9 + close * 0.1, mavg(volume, 30), 10),
            m2 = mcorr(mrank((high + low) / 2, true, 4), mrank(volume, true, 10), 7)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = pow(rank(m1, percent=true), rank(m2, percent=true)) 
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha116(table_name: str, alpha_name: str = "gtja_a116", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = linearTimeTrend(close,20)[1] 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha117(table_name: str, alpha_name: str = "gtja_a117", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} =  mrank(volume, true, 32) * (1 - mrank(close + high - low, true, 16)) * (1 - mrank(ratios(close) - 1, true, 32)) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha118(table_name: str, alpha_name: str = "gtja_a118", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(high - open, 20) / msum(open - low, 20) * 100 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha119(table_name: str, alpha_name: str = "gtja_a119", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(volume, 15) 
            context by jj_code;
    
            update {table_name} 
            set rank1 = rank(open, percent=true), 
            rank2 = rank(m1, percent=true) 
            context by trade_date;

            update {table_name} 
            set m2 = mavg(mrank(min(mcorr(rank1, rank2, 21), 9), true, 7), 1..8),
            m3 = mavg(mcorr(vwap, msum(mavg(volume, 5), 26), 5), 1..7) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m3, percent=true) - rank(m2, percent=true) 
            context by trade_date;

            {table_name}.drop!(`rank1`rank2`m1`m2`m3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha120(table_name: str, alpha_name: str = "gtja_a120", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} =  rank(vwap - close, percent=true) / rank(vwap + close, percent=true) 
            context by trade_date;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha121(table_name: str, alpha_name: str = "gtja_a121", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(vwap - min(vwap, 12), percent=true) 
            context by trade_date;

            update {table_name} 
            set m1 = mrank(mcorr(mrank(vwap, true, 20), mrank(mavg(volume, 60), true, 2), 18), true, 3)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * pow(rank1, m1) 
            context by jj_code;

            {table_name}.drop!(`rank1`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha122(table_name: str, alpha_name: str = "gtja_a122", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = ewmMean(ewmMean(ewmMean(log(close),alpha=2\\13),alpha=2\\13),alpha=2\\13) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (A - move(A,1)) / move(A,1) 
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha123(table_name: str, alpha_name: str = "gtja_a123", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr(msum((high + low) / 2, 20), msum(mavg(volume, 60), 20), 9),
            m2 = mcorr(low, volume, 6)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * (rank(m1, percent=true) < rank(m2, percent=true)) 
            context by trade_date;

            {table_name}.drop!(`m2`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha124(table_name: str, alpha_name: str = "gtja_a124", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mmax(close, 30)  
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(m1, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set {alpha_name} = (close - vwap) / mavg(rank1, 1..2)  
            context by jj_code;

            {table_name}.drop!(`rank1`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha125(table_name: str, alpha_name: str = "gtja_a125", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(mcorr(vwap, mavg(volume, 80), 17), 1..20),
            m2 = mavg(close * 0.5 + vwap * 0.5 - mfirst(close * 0.5 + vwap * 0.5, 4), 1..16)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) / rank(m2, percent=true) 
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha126(table_name: str, alpha_name: str = "gtja_a126", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (close + high + low) / 3 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha127(table_name: str, alpha_name: str = "gtja_a127", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = pow(mavg(pow(100 * (close - max(close, 12)) \\ max(close, 12), 2),12), 0.5) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha128(table_name: str, alpha_name: str = "gtja_a128", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = 100 - (100 / (1 + msum(iif((high + low + close) \\ 3 > mfirst((high + low + close) \\ 3, 2), (high + low + close) \\ 3 * volume, 0), 14) \\ msum(iif((high + low + close) \\ 3 < mfirst((high + low + close) \\ 3, 2), (high + low + close) \\ 3 * volume, 0), 14)))   
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha129(table_name: str, alpha_name: str = "gtja_a129", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(iif((close - move(close, 1)) < 0, abs(close - move(close, 1)), 0), 12) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha130(table_name: str, alpha_name: str = "gtja_a130", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(vwap, percent=true), 
            rank2 = rank(volume, percent=true) 
            context by trade_date;

            update {table_name} 
            set m1 = mavg(mcorr((high + low) \\ 2, mavg(volume, 40), 9), 1..10),
            m2 = mavg(mcorr(rank1, rank2, 7), 1..3) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) \\ rank(m2, percent=true) 
            context by trade_date;

            {table_name}.drop!(`rank1`rank2`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha131(table_name: str, alpha_name: str = "gtja_a131", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = vwap - mfirst(vwap, 2),
            m2 = mrank(mcorr(close, mavg(volume, 50), 18), true, 18)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = pow(rank(m1, percent=true), m2) 
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha132(table_name: str, alpha_name: str = "gtja_a132", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} =  mavg(volume * vwap, 20) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha133(table_name: str, alpha_name: str = "gtja_a133", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} =  (20 - (19 - mimax(high, 20))) \\ 20 * 100 - (20 - (19 - mimin(low, 20))) \\ 20 * 100 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha134(table_name: str, alpha_name: str = "gtja_a134", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} =  (close - mfirst(close, 13)) \\ mfirst(close, 13) * volume  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha135(table_name: str, alpha_name: str = "gtja_a135", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = move(close/move(close,20),1) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\20) 
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha136(table_name: str, alpha_name: str = "gtja_a136", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = ratios(close) - 1 - mfirst(ratios(close) - 1, 4),
            m2 = mcorr(open, volume, 10)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(m1, percent=true) * m2 
            context by trade_date;

            {table_name}.drop!(`m2`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha137(table_name: str, alpha_name: str = "gtja_a137", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set tmp1 = 16 * (close - mfirst(close, 2) + (close - open) \\ 2 + mfirst(close, 2) - mfirst(open, 2)),
            con1 = abs(high - mfirst(close, 2)) > abs(low - mfirst(close, 2)) && abs(high-move(close,1)) > abs(high - move(low,1)),
            con2 = abs(low - move(close,1)) > abs(high - move(low,1)) && abs(low - move(close,1)) > abs(high - move(close,1))  
            context by jj_code;
            
            update {table_name} 
            set tmp2 = iif(con1, abs(high - mfirst(close, 2)) + abs(low - mfirst(close, 2)) \\ 2 + abs(mfirst(close, 2) - mfirst(open, 2)) \\ 4, iif(con2, abs(low - mfirst(close, 2)) + abs(high - mfirst(close, 2)) \\ 2 + abs(mfirst(close, 2) - mfirst(open, 2)) \\ 4, abs(high - mfirst(low, 2)) + abs(mfirst(close, 2) - mfirst(open, 2)) \\ 4)),
            tmp3 = max(abs(high - mfirst(close, 2)), abs(low - mfirst(close, 2))) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = tmp1 \\ tmp2 * tmp3  
            context by trade_date;

            {table_name}.drop!(`tmp1`con1`con2`tmp2`tmp3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha138(table_name: str, alpha_name: str = "gtja_a138", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(low * 0.7 + vwap * 0.3 - mfirst(low * 0.7 + vwap * 0.3, 4), 1..20),
            m2 = mrank(mavg(mrank(mcorr(mrank(low, true, 8), mrank(mavg(volume, 60), true, 17), 5), true, 19), 1..16), true, 7)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * (rank(m1, percent=true) - m2) 
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha139(table_name: str, alpha_name: str = "gtja_a139", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = -1 * mcorr(open, volume, 10) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha140(table_name: str, alpha_name: str = "gtja_a140", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(open, percent=true) + rank(low, percent=true) - (rank(high, percent=true) + rank(close, percent=true))  
            context by trade_date;
            
            update {table_name} 
            set m1 = mavg(rank1, 1..8),
            m2 = mrank(mavg(mcorr(mrank(close, true, 8), mrank(mavg(volume, 60), true, 20), 8), 1..7), true, 3)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = min(rank(m1, percent=true), m2) 
            context by trade_date;

            {table_name}.drop!(`rank1`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha141(table_name: str, alpha_name: str = "gtja_a141", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set rank1 = rank(high, percent=true), 
            rank2 = rank(mavg(volume, 15), percent=true)  
            context by trade_date;

            update {table_name} 
            set m1 = mcorr(rank1, rank2, 9) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(m1, percent=true)  
            context by trade_date;

            {table_name}.drop!(`rank1`rank2`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha142(table_name: str, alpha_name: str = "gtja_a142", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mrank(close, true, 10),
            m2 = close - mfirst(close, 2) - mfirst(close - mfirst(close, 2), 2),
            m3 = mrank(volume / mavg(volume, 20), true, 5) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * rank(m1, percent=true) * rank(m2, percent=true) * rank(m3, percent=true)  
            context by trade_date;

            {table_name}.drop!(`m1`m2`m3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha143(table_name: str, alpha_name: str = "gtja_a143", top_n: int = None) -> Tuple[str, str, str, int]:
    """

    参考 https://github.com/wukan1986/ta_cn/blob/main/ta_cn/alphas/alpha191.py

    def alpha_143(CLOSE, **kwargs):
    # Alpha143 CLOSE>DELAY(CLOSE,1)?(CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)*SELF:SELF
    t1 = DELAY(CLOSE, 1)
    t2 = IF(CLOSE > t1, CLOSE / t1 - 1., 1.)
    return CUMPROD(t2)
    """
    res = f"""
            update {table_name} 
            set t1 = close/move(close,1) 
            context by jj_code;
            
            update {table_name} 
            set t2 = iif(t1 > 1, (t1-1)*100, 1) 
            context by jj_code;
            
            update {table_name} 
            set {alpha_name} = cumprod(t2) 
            context by jj_code;
            
            {table_name}.drop!(`t1`t2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha144(table_name: str, alpha_name: str = "gtja_a144", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(iif(close < mfirst(close, 2), abs(close \\ mfirst(close, 2) - 1) \\ (volume * vwap), 0), 20) \\ mcount(iif(close < mfirst(close, 2), 1, NULL), 20) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha145(table_name: str, alpha_name: str = "gtja_a145", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (mavg(volume, 9) - mavg(volume, 26)) \\ mavg(volume, 12) * 100 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha146(table_name: str, alpha_name: str = "gtja_a146", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = (close-move(close,1))/move(close,1) 
            context by jj_code;
            
            update {table_name} 
            set B = ewmMean(A,alpha=2\\61) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = mavg((A - B),20) / mavg(pow(B,2),60) 
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha147(table_name: str, alpha_name: str = "gtja_a147", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = linearTimeTrend(mavg(close,12),12)[1] 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha148(table_name: str, alpha_name: str = "gtja_a148", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr(open, msum(mavg(volume, 60), 9), 6),
            m2 = open - mmin(open, 14)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * (rank(m1, percent=true) < rank(m2, percent=true)) 
            context by trade_date;

            {table_name}.drop!(`m2`m1);
        """
    return res, table_name, alpha_name, top_n


# TODO 涉及 index 指数
@finish_res
def gtja_alpha149(table_name: str, alpha_name: str = "gtja_a149", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = condition = iif(index_close<move(index_close,1),1,NULL)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = mbeta((close/move(close,1)-1)*condition, ((index_close/move(index_close,1)-1)*condition), 252)  
            context by jj_code;

            {table_name}.drop!(`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha150(table_name: str, alpha_name: str = "gtja_a150", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (close + high + low) / 3 * volume 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha151(table_name: str, alpha_name: str = "gtja_a151", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = close-move(close,20) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\20) 
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha152(table_name: str, alpha_name: str = "gtja_a152", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = ewmMean(move((close \\ move(close, 9)), 1), alpha=1\\9) 
            context by jj_code;

            update {table_name} 
            set B = mavg(move(A,1),12) - mavg(move(A,1),26) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(B, alpha=1\\9) 
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha153(table_name: str, alpha_name: str = "gtja_a153", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (mavg(close, 3) + mavg(close, 6) + mavg(close, 12) + mavg(close, 24)) \\ 4 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha154(table_name: str, alpha_name: str = "gtja_a154", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (vwap - min(vwap, 16)) < mcorr(vwap, mavg(volume, 180), 18) 
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha155(table_name: str, alpha_name: str = "gtja_a155", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A =  ewmMean(volume,alpha=2\\13) 
            context by jj_code;

            update {table_name} 
            set B =  ewmMean(A,alpha=2\\27) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = A - B - ewmMean(A-B,alpha=2\\10) 
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha156(table_name: str, alpha_name: str = "gtja_a156", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mavg(vwap - mfirst(vwap, 6), 1..3),
            m2 = mavg((open * 0.15 + low * 0.85 - mfirst(open * 0.15 + low * 0.85, 3)) \\ (open * 0.15 + low * 0.85) * (-1), 1..3) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = -1 * max(rank(m1, percent=true), rank(m2, percent=true)) 
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha157(table_name: str, alpha_name: str = "gtja_a157", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = close - 1 - mfirst(close - 1, 6),
            m2 = mrank(mfirst(-1 * (ratios(close) - 1), 7), true, 5) 
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(rank(-1 * rank(m1, percent=true), percent=true), percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m3 = log(mmin(rank1, 2)) 
            context by jj_code;
            
            update {table_name} 
            set rank2 = rank(rank(m3, percent=true), percent=true)  
            context by trade_date;

            update {table_name} 
            set {alpha_name} = min(rank1, 5) + m2 
            context by jj_code;

            {table_name}.drop!(`m1`m2`m3`rank1`rank2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha158(table_name: str, alpha_name: str = "gtja_a158", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = ewmMean(close,alpha=2\\15) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ((high - A) - (low - A)) \\ close 
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha159(table_name: str, alpha_name: str = "gtja_a159", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set tmp1 = (close - msum(min(low, mfirst(close, 2)), 6)) / msum(max(high, mfirst(close, 2)) - min(low, mfirst(close, 2)), 6) * 12 * 24,
            tmp2 = (close - msum(min(low, mfirst(close, 2)), 12)) / msum(max(high, mfirst(close, 2)) - min(low, mfirst(close, 2)), 12) * 6 * 24,
            tmp3 = (close - msum(min(low, mfirst(close, 2)), 24)) / msum(max(high, mfirst(close, 2)) - min(low, mfirst(close, 2)), 24) * 6 * 24  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (tmp1 + tmp2 + tmp3) * 100 / (6 * 12 + 6 * 24 + 12 * 24) 
            context by jj_code;

            {table_name}.drop!(`tmp1`tmp2`tmp3);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha160(table_name: str, alpha_name: str = "gtja_a160", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = iif(close<=move(close,1),mstd(close,20),0) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\20) 
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha161(table_name: str, alpha_name: str = "gtja_a161", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mavg(max(max(high - low, abs(mfirst(close, 2) - high)), abs(mfirst(close, 2) - low)), 12)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha162(table_name: str, alpha_name: str = "gtja_a162", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = max((close-move(close,1)),0),
            B = abs(close-move(close,1)) 
            context by jj_code;
            
            update {table_name} 
            set C = ewmMean(A,alpha=1\\12),
            D = ewmMean(B,alpha=1\\12) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (C \\ D*100 - mmin((C \\ D*100), 12)) \\ (mmax((C \\ D*100), 12) - mmin((C \\ D*100), 12)) 
            context by jj_code;

            {table_name}.drop!(`A`B`C`D);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha163(table_name: str, alpha_name: str = "gtja_a163", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = -1 * (ratios(close) - 1) * mavg(volume, 20) * vwap * (high - close)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true)  
            context by trade_date;

            {table_name}.drop!(`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha164(table_name: str, alpha_name: str = "gtja_a164", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = iif(close>move(close,1), 1\\(close-move(close,1)), 1)  
            context by jj_code;

            update {table_name} 
            set B = (A - mmin(A,12)) \\ (high-low)*100 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(B, alpha=2\\13) 
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha165(table_name: str, alpha_name: str = "gtja_a165", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = msum((close - mavg(close,48)), 48),
            m2 = mstd(close,48)   
            context by jj_code;

            update {table_name} 
            set {alpha_name} = max(m1) - min(m1) \\ m2 
            context by trade_date;

            {table_name}.drop!(`m2`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha166(table_name: str, alpha_name: str = "gtja_a166", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = -20 * pow((20 - 1), 1.5) * msum(close \\ mfirst(close, 2) - 1 - mavg(close \\ mfirst(close, 2) - 1, 20), 20) \\ ((20 - 1) * (20 - 2) * pow((msum(pow(mavg(close \\ mfirst(close, 2), 20), 2), 20)), 1.5))  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha167(table_name: str, alpha_name: str = "gtja_a167", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(iif((close - mfirst(close, 2)) > 0, close - mfirst(close, 2), 0), 12)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha168(table_name: str, alpha_name: str = "gtja_a168", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = -1 * volume // mavg(volume, 20)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha169(table_name: str, alpha_name: str = "gtja_a169", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = close - move(close, 1) 
            context by jj_code;
            
            update {table_name} 
            set B = ewmMean(A, alpha=1\\9) 
            context by jj_code;
            
            update {table_name} 
            set C = mavg((move(B,1)),12) - mavg((move(B,1)),26) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(C,alpha=1\\10) 
            context by jj_code;

            {table_name}.drop!(`A`B`C);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha170(table_name: str, alpha_name: str = "gtja_a170", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = 1 / close,
            m2 = volume / mavg(volume, 20) * high,
            m3 = high - close,
            m4 = (msum(high, 5) / 5),
            m5 = vwap - mfirst(vwap, 6)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) * m2 * rank(m3, percent=true) / m4 - rank(m5, percent=true)  
            context by trade_date;

            {table_name}.drop!(`m2`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha171(table_name: str, alpha_name: str = "gtja_a171", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = -1 * (low - close) * pow(open, 5) / ((close - high) * pow(close, 5))  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha172(table_name: str, alpha_name: str = "gtja_a172", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set HD = high - mfirst(high, 2),
            LD = mfirst(low, 2) - low,
            TR = max(max(high - low, abs(high - mfirst(close, 2))), abs(low - mfirst(close, 2)))  
            context by jj_code;
            
            update {table_name} 
            set sum1 = msum(iif((LD > 0) && (LD > HD), LD, 0), 14) * 100 \\ msum(TR, 14),
            sum2 = msum(iif((HD > 0) && (HD > LD), HD, 0), 14) * 100 \\ msum(TR, 14)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = mavg(abs(sum1 - sum2) / (sum1 + sum2) * 100, 6)  
            context by jj_code;

            {table_name}.drop!(`HD`LD`TR`sum1`sum2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha173(table_name: str, alpha_name: str = "gtja_a173", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = ewmMean(close,alpha=2 \\ 13)  
            context by jj_code;
            
            update {table_name} 
            set B = ewmMean(A,alpha=2 \\ 13)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} =  3 * A - 2 * B + ewmMean(B, alpha=2\\13) 
            context by jj_code;

            {table_name}.drop!(`A`B);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha174(table_name: str, alpha_name: str = "gtja_a174", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = iif(close>move(close,1),mstd(close,20),0) 
            context by jj_code;

            update {table_name} 
            set {alpha_name} = ewmMean(A,alpha=1\\20) 
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha175(table_name: str, alpha_name: str = "gtja_a175", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mavg(max(max(high - low, abs(mfirst(close, 2) - high)), abs(mfirst(close, 2) - low)), 6)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha176(table_name: str, alpha_name: str = "gtja_a176", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = (close - mmin(low, 12)) \\ (mmax(high, 12) - mmin(low, 12))  
            context by jj_code;
            
            update {table_name} 
            set rank1 = rank(m1, percent=true), 
            rank2 = rank(volume, percent=true)  
            context by trade_date;

            update {table_name} 
            set {alpha_name} = mcorr(rank1, rank2, 6)  
            context by trade_date;

            {table_name}.drop!(`rank1`rank2`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha177(table_name: str, alpha_name: str = "gtja_a177", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (20 - (19 - mimax(high, 20))) \\ 20 * 100  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha178(table_name: str, alpha_name: str = "gtja_a178", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = (close - mfirst(close, 2)) \\ mfirst(close, 2) * volume  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha179(table_name: str, alpha_name: str = "gtja_a179", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m2 = mavg(volume, 50),
            m1 = mcorr(vwap, volume, 4)  
            context by jj_code;
            
            update {table_name} 
            set rank0 = rank(m1, percent=true),
            rank1 = rank(low, percent=true),
            rank2 = rank(m1, percent=true)  
            context by trade_date;
            
            update {table_name} 
            set m3 = mcorr(rank1, rank2, 12)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank0 * rank(m3, percent=true)  
            context by trade_date;

            {table_name}.drop!(`m2`m1`m3`rank0`rank1`rank2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha180(table_name: str, alpha_name: str = "gtja_a180", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = iif(mavg(volume, 20) < volume, -1 * mrank(abs(close - mfirst(close, 8)), true, 60) * sign(close - mfirst(close, 8)), -1 * volume)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


# TODO 需要index指数
@finish_res
def gtja_alpha181(table_name: str, alpha_name: str = "gtja_a181", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(((close\\move(close,1)-1)-mavg(close\\move(close,1)-1,20)) - pow(index_close-mavg(index_close,20),2), 20) \\ msum(pow(index_close-mavg(index_close,20),3), 20)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha182(table_name: str, alpha_name: str = "gtja_a182", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mcount(iif(iif(close>open,1,0) * iif(index_close>index_open,1,0) || iif(close<open,1,0) * iif(index_close<index_open,1,0),1,0), 20) \\ 20  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha183(table_name: str, alpha_name: str = "gtja_a183", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""    
            update {table_name} 
            set m1 = msum(close - mavg(close,24), 24),
            m2 = mstd(close,24)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = max(m1) - min(m1) \\ m2  
            context by trade_date;

            {table_name}.drop!(`m1`m2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha184(table_name: str, alpha_name: str = "gtja_a184", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set m1 = mcorr(mfirst(open - close, 2), close, 200)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = rank(m1, percent=true) + rank(open-close, percent=true)  
            context by trade_date;

            {table_name}.drop!(`m1);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha185(table_name: str, alpha_name: str = "gtja_a185", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = rank(-1 * pow(1 - open / close, 2), percent=true) 
            context by trade_date;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha186(table_name: str, alpha_name: str = "gtja_a186", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set HD = high - mfirst(high, 2),
            LD = mfirst(low, 2) - low,
            TR = max(max(high - low, abs(high - mfirst(close, 2))), abs(low - mfirst(close, 2)))  
            context by jj_code;

            update {table_name} 
            set sum1 = msum(iif((LD > 0) && (LD > HD), LD, 0), 14) * 100 / msum(TR, 14),
            sum2 = msum(iif((HD > 0) && (HD > LD), HD, 0), 14) * 100 / msum(TR, 14)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (mavg(abs(sum1 - sum2) / (sum1 + sum2) * 100, 6) + mfirst(mavg(abs(sum1 - sum2) / (sum1 + sum2) * 100, 6), 7)) / 2  
            context by jj_code;

            {table_name}.drop!(`HD`LD`TR`sum1`sum2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha187(table_name: str, alpha_name: str = "gtja_a187", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = msum(iif(open <= mfirst(open, 2), 0, max(high - open, open - mfirst(open, 2))), 20)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha188(table_name: str, alpha_name: str = "gtja_a188", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = ewmMean(high-low,alpha=2\\11)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} = (high - low - A) / A * 100  
            context by jj_code;

            {table_name}.drop!(`A);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha189(table_name: str, alpha_name: str = "gtja_a189", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mavg(abs(close - mavg(close, 6)), 6)  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha190(table_name: str, alpha_name: str = "gtja_a190", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set A = close / move(close, 1),
            B = pow((close/move(close,19)),1\\20)  
            context by jj_code;

            update {table_name} 
            set C = (A-1)>(B-1),
            D = (A-1)<(B-1)  
            context by jj_code;

            update {table_name} 
            set E = mcount(C,20),
            F = mcount(D,20),
            F0 = pow(A-1-B-1,2)  
            context by jj_code;
            
            update {table_name} 
            set F1 = msum(F0 * D, 20),
            F2 = msum(F0 * C, 20)  
            context by jj_code;

            update {table_name} 
            set {alpha_name} =  log(((E-1) * F1) / (D * F2)) 
            context by jj_code;

            {table_name}.drop!(`A`B`C`D`E`F`F0`F1`F2);
        """
    return res, table_name, alpha_name, top_n


@finish_res
def gtja_alpha191(table_name: str, alpha_name: str = "gtja_a191", top_n: int = None) -> Tuple[str, str, str, int]:
    res = f"""
            update {table_name} 
            set {alpha_name} = mcorr(mavg(volume, 20), low, 5) + (high + low) / 2 - close  
            context by jj_code;
        """
    return res, table_name, alpha_name, top_n


if __name__ == '__main__':
    gtja_alpha23(table_name="tt")
