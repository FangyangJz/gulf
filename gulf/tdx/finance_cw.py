# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/26 22:49
# @Author   : Fangyang
# @Software : PyCharm

import zipfile
from pathlib import Path
from typing import List, Union, Dict

import pandas as pd
from mootdx.affair import Affair
from gulf.tdx.path import cw_path
from loguru import logger


def get_local_cw_file_list(ext_name='.dat') -> List[Path]:
    """
    列出本地已有的专业财务文件。返回文件列表
    :param ext_name: str类型。文件扩展名。返回指定扩展名的文件列表
    :return: list类型。财务专业文件列表
    """
    return [f for f in cw_path.glob("*" + ext_name) if len(f.stem) == 12 and f.stem[:4] == "gpcw"]


def update_cw_data():
    local_files = [f.name for f in cw_path.glob('*.zip')]  # 本地财务文件列表
    remote_files = Affair.files()  # 远程财务文件列表

    if len(remote_files) == len(local_files):
        logger.info(f"Remote file length == local file length, no need to update.")

    for remote_file in remote_files:
        remote_filename = remote_file['filename']
        if remote_filename not in local_files:
            logger.info(f"{remote_filename} not in local, start to download.")
            # 下载单个
            Affair.fetch(downdir=str(cw_path), filename=remote_filename)

            with zipfile.ZipFile(cw_path / remote_filename, 'r') as zipobj:  # 打开zip对象，释放zip文件。会自动覆盖原文件。
                zipobj.extractall(cw_path)


def get_history_financial_df(filepath: Union[Path, str]) -> pd.DataFrame:
    """
    读取解析通达信目录的历史财务数据
    :param filepath: 字符串类型。传入文件路径
    :return: DataFrame格式。返回解析出的财务文件内容
    """
    import struct

    with open(filepath, 'rb') as cw_file:
        header_pack_format = '<1hI1H3L'
        header_size = struct.calcsize(header_pack_format)
        stock_item_size = struct.calcsize("<6s1c1L")
        data_header = cw_file.read(header_size)
        stock_header = struct.unpack(header_pack_format, data_header)
        max_count = stock_header[2]
        report_date = stock_header[1]
        report_size = stock_header[4]
        report_fields_count = int(report_size / 4)
        report_pack_format = '<{}f'.format(report_fields_count)
        results = []

        for stock_idx in range(0, max_count):
            cw_file.seek(header_size + stock_idx * struct.calcsize("<6s1c1L"))
            si = cw_file.read(stock_item_size)
            stock_item = struct.unpack("<6s1c1L", si)
            code = stock_item[0].decode("utf-8")
            foa = stock_item[2]
            cw_file.seek(foa)
            info_data = cw_file.read(struct.calcsize(report_pack_format))
            data_size = len(info_data)
            cw_info = list(struct.unpack(report_pack_format, info_data))
            cw_info.insert(0, code)
            cw_info.insert(1, filepath.stem[4:])
            results.append(cw_info)

    return pd.DataFrame(results)


def get_cw_dict() -> Dict[str, pd.DataFrame]:
    from tqdm import tqdm

    logger.info(f'Loading local financial data ...')
    cw_dict = {}
    for i in tqdm(get_local_cw_file_list()):
        cw_df = get_history_financial_df(i)

        if cw_df.empty:
            continue

        cw_dict[i.stem[4:]] = cw_df

    return cw_dict


if __name__ == '__main__':
    update_cw_data()
    file_list = get_local_cw_file_list()
    cw_test_dict = get_cw_dict()
    print(1)
