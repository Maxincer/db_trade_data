#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20201031T180000

"""
data type: 数据类型

假设:
1. patch 默认 fmtted


todo:
    1. 未处理patch data
    2. 首期版本以满足公司传统股票策略产品为主
"""

from datetime import datetime
import time
from threading import Thread
import orjson
import pandas as pd
import redis
from trader_v1 import Trader
import codecs
import logging
from logging.handlers import RotatingFileHandler
from stock_utils import ID2Source, get_sectype_from_code

from globals import Globals

logger_expo = logging.getLogger()
logger_expo.setLevel(logging.DEBUG)
fh = RotatingFileHandler('data/log/exposure.log', mode='w', maxBytes=2 * 1024, backupCount=0)
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter('%(asctime)s - line:%(lineno)d - %(levelname)s: %(message)s'))
logger_expo.addHandler(fh)


class GenerateExposureMonitorData:
    def __init__(self):
        self.redis_host = '47.103.187.110'
        self.redis_port = 6379
        self.redis_pass = 'Ms123456'
        self.server_redis_md = redis.Redis(host=self.redis_host, port=self.redis_port, password=self.redis_pass)

        self.gl = Globals()
        self.list_warn = []
        self.id2source = ID2Source(self.gl.db_basicinfo, 'data/security_id.xlsx')

    def read_rawdata_from_trdclient(
            self, fpath, sheet_type, data_source_type, accttype, idinfo, dict_acctidbymxz2acctidbyborker
    ):
        list_ret = []
        if sheet_type == 'fund':
            if data_source_type in ['zx_wealthcats']:
                fpath_replaced = fpath.replace('<YYYY-MM-DD>', self.gl.dt_today.strftime('%Y-%m-%d'))
                with codecs.open(fpath_replaced, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning(f'读取空白文件%s' % fpath_replaced)
                    else:
                        if len(list_datalines) == 0:
                            logger_expo.warning('读取空白文件%s' % fpath_replaced)
                        else:
                            list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().rstrip(',').split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund_wealthcats = dict(zip(list_keys, list_values))
                            if dict_fund_wealthcats['账户'] in idinfo:
                                dict_fund_wealthcats['AcctIDByMXZ'] = idinfo[dict_fund_wealthcats['账户']]
                                list_ret.append(dict_fund_wealthcats)

            elif data_source_type in ['ax_jzpb']:
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with open(fpath_replaced, encoding='ansi') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['账户编号'] in idinfo:
                                dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['账户编号']]
                                list_ret.append(dict_fund)

            elif data_source_type in self.gl.list_data_src_xtpb:
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['资金账号'] in idinfo:
                                dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['资金账号']]
                                list_ret.append(dict_fund)

            elif data_source_type in ['hait_ehfz_api']:
                for acctidbybroker in idinfo:
                    try:
                        fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                        with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s' % fpath_replaced)
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_fund = dict(zip(list_keys, list_values))
                                    dict_fund['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                    list_ret.append(dict_fund)

                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in ['huat_matic_tsi']:
                for acctidbybroker in idinfo:
                    try:
                        fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                        with codecs.open(fpath_replaced, 'rb', encoding='gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s' % fpath_replaced)
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_fund = dict(zip(list_keys, list_values))
                                    if dict_fund['fund_account'] == acctidbybroker:
                                        dict_fund['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                        list_ret.append(dict_fund)  # 有改动

                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in self.gl.list_data_src_htpb:  # 有改动
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['资金账户'] in idinfo:
                                dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['资金账户']]
                                list_ret.append(dict_fund)

            elif data_source_type in ['gtja_pluto']:  # 有改动
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['单元序号'] in idinfo:
                                dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['单元序号']]
                                list_ret.append(dict_fund)

            elif data_source_type in ['yh_apama'] and accttype == 'c':  # 有改动
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '币种', '可用余额', '可取金额', '冻结金额', '总资产', '证券市值', '资金资产']
                    for dataline in list_datalines:
                        dataline = dataline.strip('\n')
                        split_line = dataline.split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind + 1:])  # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            dict_fund['AcctIDByMXZ'] = list(idinfo.values())[0]  # fpath里自带交易账户， idinfo仅一个
                            list_ret.append(dict_fund)
                        else:
                            logger_expo.warning('strange fund keys of yh_apama %s' % fpath)

            elif data_source_type in ['yh_apama'] and accttype == 'm':
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '币种', '可用余额', '可取金额', '冻结金额', '总资产', '证券市值',
                                 '资金资产', '总负债', '融资负债', '融券负债', '融资息费', '融券息费', '融资可用额度',
                                 '融券可用额度', '担保证券市值', '维持担保比例', '实时担保比例']
                    for dataline in list_datalines:
                        dataline = dataline.strip('\n')
                        split_line = dataline.split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind + 1:])  # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            dict_fund['AcctIDByMXZ'] = list(idinfo.values())[0]  # fpath里自带交易账户， idinfo仅一个
                            list_ret.append(dict_fund)
                        else:
                            logger_expo.warning('strange fund key of yh_apama %s' % fpath)

            elif data_source_type in ['gf_tyt']:
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_fund = dict(zip(list_keys, list_values))
                            if dict_fund['projectid'] in idinfo:
                                dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['projectid']]
                                list_ret.append(dict_fund)

            elif data_source_type in ['dh_xtqmt_jjb', 'gd_xtqmt_jjb', 'hr_xtqmt_jjb', 'zhes_xtqmt_jjb', 'gj_xtqmt_jjb']:
                for dldfilter in idinfo:
                    fpath_replaced = (
                        fpath.replace('<YYYYMMDD>', self.gl.str_today)
                             .replace('<ID>', dict_acctidbymxz2acctidbyborker[idinfo[dldfilter]])
                    )
                    with open(fpath_replaced) as f:
                        list_datalines = f.readlines()
                        if len(list_datalines) == 0:
                            logger_expo.warning('读取空白文件%s' % fpath)
                        else:
                            list_keys = list_datalines[0].strip().split(',')
                        for dataline in list_datalines[1:]:
                            list_values = dataline.strip().split(',')
                            if len(list_values) == len(list_keys):
                                dict_fund = dict(zip(list_keys, list_values))
                                if dict_fund['账号'] in idinfo:
                                    dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['账号']]
                                    list_ret.append(dict_fund)

            elif data_source_type in ['trader_api']:
                pass

            else:
                e = f'Field data_source_type {data_source_type} not exist in basic info!'
                print(e)
                if e not in self.list_warn:
                    self.list_warn.append(e)
                    logger_expo.error(e)

        elif sheet_type == 'holding':
            if data_source_type in ['zx_wealthcats']:
                fpath_replaced = fpath.replace('<YYYY-MM-DD>', self.gl.dt_today.strftime('%Y-%m-%d'))
                with codecs.open(fpath_replaced, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['SymbolFull'].split('.')[1] == 'SZ':
                                dict_rec_holding['交易市场'] = '深A'
                            elif dict_rec_holding['SymbolFull'].split('.')[1] == 'SH':
                                dict_rec_holding['交易市场'] = '沪A'
                            else:
                                raise ValueError('Unknown exchange mark.')
                            if dict_rec_holding['账户'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['账户']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['ax_jzpb']:  # 有改动
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with open(fpath_replaced, encoding='ansi') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['账户编号'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['账户编号']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in self.gl.list_data_src_xtpb:
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账号'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['资金账号']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['hait_ehfz_api']:  # 有改动
                for acctidbybroker in idinfo:
                    fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                    try:
                        with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s' % fpath_replaced)
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_rec_holding = dict(zip(list_keys, list_values))
                                    dict_rec_holding['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                    list_ret.append(dict_rec_holding)

                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in ['huat_matic_tsi']:
                for acctidbybroker in idinfo:
                    fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                    try:
                        with codecs.open(fpath_replaced, 'rb', encoding='gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s' % fpath_replaced)
                                continue
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_holding = dict(zip(list_keys, list_values))
                                    dict_holding['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                    list_ret.append(dict_holding)
                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in self.gl.list_data_src_htpb:  # 有改动
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账户'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['资金账户']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['gtja_pluto']:  # 有改动
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['单元序号'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['单元序号']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['yh_apama'] and accttype == 'm':  # 有改动
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '证券代码', '交易市场', '股份可用', '当前持仓', '持仓成本', '最新价',
                                 '昨日持仓', '冻结数量', '买入冻结', '卖出冻结', '参考盈亏', '参考市值', '是否为担保品',
                                 '担保品折算率', '融资买入股份余额', '融资买入股份可用']
                    for dataline in list_datalines:
                        split_line = dataline.strip('\n').split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind + 1:])  # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_holding = dict(zip(list_keys, list_values))
                            dict_holding['AcctIDByMXZ'] = list(idinfo.values())[0]
                            list_ret.append(dict_holding)
                        else:
                            logger_expo.warning('strange holding keys of yh_apama %s' % fpath_replaced)

            elif data_source_type in ['yh_apama'] and accttype == 'c':  # 有改动
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '证券代码', '交易市场', '股份可用', '当前持仓', '持仓成本', '最新价',
                                 '昨日持仓', '股东代码', '买入冻结', '买入冻结金额', '卖出冻结', '卖出冻结金额']
                    for dataline in list_datalines:
                        split_line = dataline.strip('\n').split('|')
                        list_values = split_line[:-1]
                        for other_value in split_line[-1].split('&'):  # 扩展字段
                            ind = other_value.find('=')
                            list_values.append(other_value[ind + 1:])  # 'fl=; 'ml=
                        if len(list_values) == len(list_keys):
                            dict_holding = dict(zip(list_keys, list_values))
                            dict_holding['AcctIDByMXZ'] = list(idinfo.values())[0]
                            list_ret.append(dict_holding)
                        else:
                            logger_expo.warning('strange holidng keys of yh_apama %s' % fpath_replaced)

            elif data_source_type in ['gf_tyt']:
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_holding = dict(zip(list_keys, list_values))
                            if dict_holding['projectid'] in idinfo:
                                dict_holding['AcctIDByMXZ'] = idinfo[dict_holding['projectid']]
                                list_ret.append(dict_holding)

            elif data_source_type in ['dh_xtqmt_jjb', 'gd_xtqmt_jjb', 'hr_xtqmt_jjb', 'zhes_xtqmt_jjb', 'gj_xtqmt_jjb']:
                for dldfilter in idinfo:
                    fpath_replaced = (
                        fpath.replace('<YYYYMMDD>', self.gl.str_today)
                             .replace('<ID>', dict_acctidbymxz2acctidbyborker[idinfo[dldfilter]])
                    )
                    with open(fpath_replaced) as f:
                        list_datalines = f.readlines()
                        if len(list_datalines) == 0:
                            logger_expo.warning('读取空白文件%s' % fpath)
                        else:
                            list_keys = list_datalines[0].strip().split(',')
                        for dataline in list_datalines[1:]:
                            list_values = dataline.strip().split(',')
                            if len(list_values) == len(list_keys):
                                dict_fund = dict(zip(list_keys, list_values))
                                if dict_fund['账号'] in idinfo:
                                    dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['账号']]
                                    list_ret.append(dict_fund)

        elif sheet_type == 'order':
            # todo 先做这几个有secloan的（不然order没意义）:
            if data_source_type in self.gl.list_data_src_xtpb:
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['资金账号'] in idinfo:
                                dict_rec_order['AcctIDByMXZ'] = idinfo[dict_rec_order['资金账号']]
                                list_ret.append(dict_rec_order)

            elif data_source_type in ['hait_ehfz_api']:
                for acctidbybroker in idinfo:
                    try:
                        fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                        with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s' % fpath_replaced)
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_rec_order = dict(zip(list_keys, list_values))
                                    dict_rec_order['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                    list_ret.append(dict_rec_order)

                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in ['huat_matic_tsi']:  # 有改动
                for acctidbybroker in idinfo:
                    try:
                        fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today).replace('<ID>', acctidbybroker)
                        with codecs.open(fpath_replaced, 'rb', encoding='gbk') as f:
                            list_datalines = f.readlines()
                            if len(list_datalines) == 0:
                                logger_expo.warning('读取空白文件%s' % fpath_replaced)
                            else:
                                list_keys = list_datalines[0].strip().split(',')
                            for dataline in list_datalines[1:]:
                                list_values = dataline.strip().split(',')
                                if len(list_values) == len(list_keys):
                                    dict_order = dict(zip(list_keys, list_values))
                                    dict_order['AcctIDByMXZ'] = idinfo[acctidbybroker]
                                    list_ret.append(dict_order)

                    except FileNotFoundError as e:
                        e = str(e)
                        if e not in self.list_warn:
                            self.list_warn.append(e)
                            logger_expo.error(e)

            elif data_source_type in ['gtja_pluto']:  # 有改动
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['单元序号'] in idinfo:
                                dict_rec_order['AcctIDByMXZ'] = idinfo[dict_rec_order['单元序号']]
                                list_ret.append(dict_rec_order)

            elif data_source_type in ['yh_apama']:  # 成交明细不是委托明细
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced) as f:
                    list_datalines = f.readlines()
                    list_keys = ['请求编号', '资金账号', '证券代码', '交易市场', '委托序号', '买卖方向', '股东号', '成交时间',
                                 '成交编号', '成交价格', '成交数量', '成交金额', '成交类型', '委托数量', '委托价格']
                    for dataline in list_datalines:
                        split_line = dataline.strip('\n').split('|')
                        list_values = split_line[:-1]
                        if len(list_values) == len(list_keys):
                            dict_order = dict(zip(list_keys, list_values))
                            dict_order['AcctIDByMXZ'] = list(idinfo.values())[0]
                            list_ret.append(dict_order)
                        else:
                            logger_expo.warning('strange order keys of yh_apama %s' % fpath)

            elif data_source_type in ['ax_jzpb']:
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with open(fpath_replaced, encoding='ansi') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')

                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if '信息初始化' in list_values:  # todo 最后一行莫名多出这个（标题和其他行还没有）得改
                            list_values = list_values[:-1]
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['账户编号'] in idinfo:
                                dict_rec_order['AcctIDByMXZ'] = idinfo[dict_rec_order['账户编号']]
                                list_ret.append(dict_rec_order)

            elif data_source_type in ['zx_wealthcats']:
                fpath_replaced = fpath.replace('<YYYY-MM-DD>', self.gl.dt_today.strftime('%Y-%m-%d'))
                with codecs.open(fpath_replaced, 'rb', 'utf-8-sig') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_order = dict(zip(list_keys, list_values))
                            if dict_rec_order['账户'] in idinfo:
                                dict_rec_order['AcctIDByMXZ'] = idinfo[dict_rec_order['账户']]
                                list_ret.append(dict_rec_order)

            elif data_source_type in self.gl.list_data_src_htpb:  # 有改动
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_rec_holding = dict(zip(list_keys, list_values))
                            if dict_rec_holding['资金账户'] in idinfo:
                                dict_rec_holding['AcctIDByMXZ'] = idinfo[dict_rec_holding['资金账户']]
                                list_ret.append(dict_rec_holding)

            elif data_source_type in ['gf_tyt']:
                fpath_replaced = fpath.replace('<YYYYMMDD>', self.gl.str_today)
                with codecs.open(fpath_replaced, 'rb', 'gbk') as f:
                    list_datalines = f.readlines()
                    if len(list_datalines) == 0:
                        logger_expo.warning('读取空白文件%s' % fpath_replaced)
                    else:
                        list_keys = list_datalines[0].strip().split(',')
                    for dataline in list_datalines[1:]:
                        list_values = dataline.strip().split(',')
                        if len(list_values) == len(list_keys):
                            dict_order = dict(zip(list_keys, list_values))
                            if dict_order['projectid'] in idinfo:
                                dict_order['AcctIDByMXZ'] = idinfo[dict_order['projectid']]
                                list_ret.append(dict_order)

            elif data_source_type in ['dh_xtqmt_jjb', 'gd_xtqmt_jjb', 'hr_xtqmt_jjb', 'zhes_xtqmt_jjb', 'gj_xtqmt_jjb']:
                for dldfilter in idinfo:
                    fpath_replaced = (
                        fpath.replace('<YYYYMMDD>', self.gl.str_today)
                             .replace('<ID>', dict_acctidbymxz2acctidbyborker[idinfo[dldfilter]])
                    )
                    with open(fpath_replaced) as f:
                        list_datalines = f.readlines()
                        if len(list_datalines) == 0:
                            logger_expo.warning('读取空白文件%s' % fpath)
                        else:
                            list_keys = list_datalines[0].strip().split(',')
                        for dataline in list_datalines[1:]:
                            list_values = dataline.strip().split(',')
                            if len(list_values) == len(list_keys):
                                dict_fund = dict(zip(list_keys, list_values))
                                if dict_fund['账号'] in idinfo:
                                    dict_fund['AcctIDByMXZ'] = idinfo[dict_fund['账号']]
                                    list_ret.append(dict_fund)

        elif sheet_type == 'short_position':
            # todo 留出接口
            pass
        else:
            raise ValueError('Wrong sheet name!')
        return list_ret

    def update_trdraw_cmo(self):
        dict_col_rawdata = {
            'fund': self.gl.col_trade_rawdata_fund,
            'holding': self.gl.col_trade_rawdata_holding,
            'order': self.gl.col_trade_rawdata_order,
            'short_position': self.gl.col_trade_rawdata_short_position
        }

        dict_fpath_trddata2acct = {}
        dict_acctidbymxz2acctidbybroker = {}
        for dict_acctinfo in self.gl.col_acctinfo.find({'DataDate': self.gl.str_today, 'DataDownloadMark': 1}):
            fpath_trddata = dict_acctinfo['TradeDataFilePath']
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            acctidbybroker = dict_acctinfo['AcctIDByBroker']
            dict_acctidbymxz2acctidbybroker.update({acctidbymxz: acctidbybroker})
            if fpath_trddata:
                if dict_acctinfo['DownloadDataFilter']:
                    dldfilter = dict_acctinfo['DownloadDataFilter']
                else:
                    dldfilter = dict_acctinfo['AcctIDByBroker']
                if fpath_trddata in dict_fpath_trddata2acct:
                    dict_fpath_trddata2acct[fpath_trddata].update({dldfilter: dict_acctinfo['AcctIDByMXZ']})
                else:
                    # 假设： 同一地址 data_source_type, accttype一样, 普通户和信用户肯定分开保存
                    dict_fpath_trddata2acct[fpath_trddata] = {
                        dldfilter: dict_acctinfo['AcctIDByMXZ'],
                        'AcctType': dict_acctinfo['AcctType'],
                        'DataSourceType': dict_acctinfo['DataSourceType']
                    }

        dict_list_upload_recs = {'fund': [], 'holding': [], 'order': [], 'short_position': []}
        for fpath_trddata in dict_fpath_trddata2acct:
            info = dict_fpath_trddata2acct[fpath_trddata]
            list_fpath_data = fpath_trddata[1:-1].split(',')
            data_source_type = info["DataSourceType"]
            accttype = info['AcctType']
            id_info = info.copy()

            del id_info['AcctType']
            del id_info['DataSourceType']

            for i in range(4):  # 目前只需要使用前4个
                fpath_relative = list_fpath_data[i]
                if fpath_relative == '':
                    continue
                sheet_name = ['fund', 'holding', 'order', 'short_position'][i]

                try:
                    list_dicts_rec = self.read_rawdata_from_trdclient(
                        fpath_relative, sheet_name, data_source_type, accttype, id_info, dict_acctidbymxz2acctidbybroker
                    )
                    for dict_rec in list_dicts_rec:
                        dict_rec['DataDate'] = self.gl.str_today
                        dict_rec['UpdateTime'] = datetime.now().strftime('%H%M%S')
                        dict_rec['AcctType'] = accttype
                        dict_rec['DataSourceType'] = data_source_type

                    if list_dicts_rec:
                        dict_list_upload_recs[sheet_name] += list_dicts_rec

                except FileNotFoundError as e:
                    e = str(e)
                    if e not in self.list_warn:
                        logger_expo.warning(e)
                        self.list_warn.append(e)

        for ch in dict_col_rawdata:
            if dict_list_upload_recs[ch]:
                dict_col_rawdata[ch].delete_many(
                    {'DataDate': self.gl.str_today, 'AcctType': {'$in': ['c', 'm', 'o']}}
                )
                dict_col_rawdata[ch].insert_many(dict_list_upload_recs[ch])
        print('Update all raw data finished.')

    def update_trdraw_f(self):
        for _ in self.gl.col_acctinfo.find({'DataDate': self.gl.str_today, 'AcctType': 'f', 'DataDownloadMark': 1}):
            list_future_data_fund = []
            list_future_data_holding = []
            list_future_data_short_position = []
            list_future_data_trdrec = []
            prdcode = _['PrdCode']
            acctidbymxz = _['AcctIDByMXZ']
            acctidbyowj = _['AcctIDByOuWangJiang4FTrd']
            data_source_type = _['DataSourceType']
            try:
                trader = Trader(acctidbyowj)
            except Exception as e:
                str_e = f"{acctidbymxz}: {str(e)}"
                print(str_e)
                if str_e not in self.list_warn:
                    logger_expo.error(str_e)
                    self.list_warn.append(str_e)

                if '连接不通' in str_e:  # api 关闭
                    print(str_e)
                    break
                else:
                    continue

            dict_res_fund = trader.query_capital()
            if dict_res_fund:
                dict_fund_to_be_update = dict_res_fund
                dict_fund_to_be_update['DataDate'] = self.gl.str_today
                dict_fund_to_be_update['AcctIDByMXZ'] = acctidbymxz
                dict_fund_to_be_update['AcctIDByOWJ'] = acctidbyowj
                dict_fund_to_be_update['PrdCode'] = prdcode
                dict_fund_to_be_update['DataSourceType'] = data_source_type
                dict_fund_to_be_update['UpdateTime'] = datetime.now().strftime('%H%M%S')
                list_future_data_fund.append(dict_fund_to_be_update)
                if list_future_data_fund:
                    self.gl.col_trade_rawdata_fund.delete_many(
                        {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz}
                    )
                    self.gl.col_trade_rawdata_fund.insert_many(list_future_data_fund)

            list_list_res_holding = trader.query_holding()
            list_keys_holding = [
                'exchange', 'instrument_id', 'direction', 'hedge', 'position', 'position_td', 'open_volume',
                'close_volume', 'unknown1', 'unknown2', 'unknown3'
            ]

            if len(list_list_res_holding):
                list_dicts_holding_to_be_update = list_list_res_holding
                for list_holding_to_be_update in list_dicts_holding_to_be_update:
                    dict_holding_to_be_update = dict(zip(list_keys_holding, list_holding_to_be_update))
                    dict_holding_to_be_update['DataDate'] = self.gl.str_today
                    dict_holding_to_be_update['AcctIDByMXZ'] = acctidbymxz
                    dict_holding_to_be_update['AcctType'] = 'f'
                    dict_holding_to_be_update['AcctIDByOWJ'] = acctidbyowj
                    dict_holding_to_be_update['PrdCode'] = prdcode
                    dict_holding_to_be_update['DataSourceType'] = data_source_type
                    dict_holding_to_be_update['UpdateTime'] = datetime.now().strftime('%H%M%S')
                    if dict_holding_to_be_update['direction'] in ['buy']:
                        list_future_data_holding.append(dict_holding_to_be_update)
                    elif dict_holding_to_be_update['direction'] in ['sell']:
                        list_future_data_short_position.append(dict_holding_to_be_update)

                if list_future_data_holding:
                    self.gl.col_trade_rawdata_holding.delete_many(
                        {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz})
                    self.gl.col_trade_rawdata_holding.insert_many(list_future_data_holding)

                if list_future_data_short_position:
                    self.gl.col_trade_rawdata_short_position.delete_many(
                        {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz})
                    self.gl.col_trade_rawdata_short_position.insert_many(list_future_data_short_position)

            list_list_res_trdrecs = trader.query_trdrecs()
            if len(list_list_res_trdrecs):
                list_keys_trdrecs = ['instrument_id', 'direction', 'offset', 'volume', 'price', 'time', 'trader']
                for list_res_trdrecs in list_list_res_trdrecs:
                    dict_trdrec = dict(zip(list_keys_trdrecs, list_res_trdrecs))
                    dict_trdrec['DataDate'] = self.gl.str_today
                    dict_trdrec['AcctIDByMXZ'] = acctidbymxz
                    dict_trdrec['AcctIDByOWJ'] = acctidbyowj
                    dict_trdrec['PrdCode'] = prdcode
                    dict_trdrec['DataSourceType'] = data_source_type
                    dict_trdrec['UpdateTime'] = datetime.now().strftime('%H%M%S')
                    list_future_data_trdrec.append(dict_trdrec)

                if list_future_data_trdrec:
                    self.gl.col_trade_rawdata_order.delete_many(
                        {'DataDate': self.gl.str_today, 'AcctIDByMXZ': acctidbymxz})
                    self.gl.col_trade_rawdata_order.insert_many(list_future_data_trdrec)
        print('Update future account finished.')

    def formulate_raw_data(self, acctidbymxz, accttype, patchpath, sheet_type, raw_list):
        list_dicts_fmtted = []
        if accttype in ['c', 'm'] and patchpath is None:
            # ---------------  FUND 相关列表  ---------------------
            # 现金, 资产负债表概念, 现金 = 总资产-总市值, cash account 中的cash的值 = available_fund,
            # 可用资金, 交易概念, 包括: 可用于交易担保品资金, 可用于交易证券资金
            list_fields_af = [
                '可用', 'A股可用', '可用数', '现金资产', '可用金额', '资金可用金', '可用余额',
                'T+0交易可用金额', 'enable_balance', 'fund_asset', '可用资金', 'instravl',
            ]
            list_fields_ttasset = [
                '总资产', '资产', '总 资 产', '实时总资产', '单元总资产', '资产总额', '产品总资产',
                '账户总资产', '担保资产', 'asset_balance', 'assure_asset', '账户资产', '资产总值',
            ]
            list_fields_na = ['netasset', 'net_asset', '账户净值', '净资产']
            list_fields_kqzj = [
                '可取资金', '可取金额', 'fetch_balance', '沪深T+1交易可用', '可取余额', 'T+1交易可用金额', '可取数', '可取现金'
            ]
            list_fields_tl = ['总负债', 'total_debit']
            list_fields_mktvalue = ['总市值', 'market_value', '证券资产', '证券市值']

            # ---------------  Security 相关列表  ---------------------
            list_fields_secid = ['代码', '证券代码', 'stock_code', 'stkcode']
            list_fields_symbol = ['证券名称', 'stock_name', '股票名称', '名称']
            list_fields_shareholder_acctid = ['股东帐户', '股东账号', '股东代码']
            list_fields_exchange = [
                '市场代码', '交易市场', '交易板块', '板块', '交易所', '交易所名称', '交易市场', 'exchange_type', 'market', '市场'
            ]

            list_fields_longqty = [
                '当前拥股数量', '股票余额', '拥股数量', '证券余额', '证券数量', '库存数量', '持仓数量', '参考持股', '持股数量',
                '当前持仓', '当前余额', '当前拥股', '实际数量', '实时余额', 'current_amount', 'stkholdqty'
            ]

            dict_exchange2secidsrc = {
                '深A': 'SZSE', '沪A': 'SSE',
                '深Ａ': 'SZSE', '沪Ａ': 'SSE',
                '上海Ａ': 'SSE', '深圳Ａ': 'SZSE',
                '上海Ａ股': 'SSE', '深圳Ａ股': 'SZSE',
                '上海A股': 'SSE', '深圳A股': 'SZSE',
                'SH': 'SSE', 'SZ': 'SZSE',
                '上交所A': 'SSE', '深交所A': 'SZSE',
                '上证所': 'SSE', '深交所': 'SZSE',
            }

            dict_ambiguous_secidsrc = {
                'hait_ehfz_api': {'1': 'SZSE', '2': 'SSE'},
                'gtja_pluto': {'1': 'SSE', '2': "SZSE"},
                'huat_matic_tsi': {'1': 'SSE', '2': 'SZSE'},
                'yh_apama': {'0': 'SZSE', '2': 'SSE'},
                'ax_jzpb': {'0': 'SZSE', '1': 'SSE', '深Ａ': 'SZSE', '沪Ａ': 'SSE'},
                'gf_tyt': {'0': 'SZSE', '1': 'SSE'}
            }

            # -------------  ORDER 相关列表  ---------------------
            # order委托/entrust除了成交时间等信息最全，不是成交(trade,deal)（没有委托量等）
            #  zxjt_xtpb, zhaos_xtpb只有deal无order； deal/trade？
            # todo 撤单单独列出一个字段 + 买券还券等处理 （huat拆成两个如何合并？）
            #  带数字不明确的得再理一理
            #  OrdID 最好判断下是否有一样的，（数据源可能超级加倍...）
            # 撤单数+成交数=委托数 来判断终态, ordstatus ‘部撤’有时并非终态

            list_fields_cumqty = ['成交数量', 'business_amount', 'matchqty', '成交量']
            list_fields_leavesqty = ['撤单数量', '撤销数量', 'withdraw_amount', 'cancelqty', '撤单量', '已撤数量']
            list_fields_side = ['买卖标记', 'entrust_bs', '委托方向', '@交易类型', 'bsflag', '交易', '买卖标识']
            list_fields_orderqty = ['委托量', 'entrust_amount', '委托数量', 'orderqty']  # XXX_deal 会给不了委托量，委托日期，委托时间，只有成交
            list_fields_ordertime = ['委托时间', 'entrust_time', 'ordertime ', '时间', '成交时间']  # yh
            list_fields_avgpx = ['成交均价', 'business_price', '成交价格', 'orderprice']  # 以后算balance用， exposure不用
            dict_fmtted_side_name = {'买入': 'buy', '卖出': 'sell',
                                     '限价担保品买入': 'buy', '限价买入': 'buy', '担保品买入': 'buy', 'BUY': 'buy',
                                     # 担保品=券； 限价去掉,含"...“即可
                                     '限价卖出': 'sell', '限价担保品卖出': 'sell', '担保品卖出': 'sell', 'SELL': 'sell',
                                     '0B': 'buy', '0S': 'sell', '证券买入': 'buy', '证券卖出': 'sell',
                                     '限价融券卖出': 'sell short', '融券卖出': 'sell short',  # 快速交易的 hait=11
                                     '现券还券划拨': 'XQHQ', '现券还券划拨卖出': 'XQHQ',  # 快速交易的 hait=15, gtja=34??
                                     '买券还券划拨': 'MQHQ', '买券还券': 'MQHQ', '限价买券还券': 'MQHQ',  # 快速交易的 hait=13
                                     '撤单': 'cancel', 'ZR': 'Irrelevant', 'ZC': 'Irrelevant'}  # entrust_bs表方向时值为1，2
            dict_ambiguous_side_name = {
                'hait_ehfz_api': {
                    '1': 'buy', '2': 'sell', '12': 'sell short', '15': 'XQHQ', '13': 'MQHQ', '0': 'cancel'
                },
                'gtja_pluto': {
                    '1': 'buy', '2': 'sell', '34': 'MQHQ', '32': 'sell short', '31': 'buy', '33': 'sell', '36': 'XQHQ'
                },
            }
                # }  # 信用户在后面讨论（需要两个字段拼起来才行）
            # dict_datasource_ordstatus = {
            #     # 参考FIX：New已报； Partially Filled=部成待撤/部成，待撤=PendingCancel不算有效cumqty,中间态
            #     # 国内一般全成，部撤等都表示最终态，cumqty的数值都是有效的(Filled, Partially Canceled)，其他情况的cumqty不能算
            #     # 部撤 Partially Canceled(自己命名的）
            #     'hait_ehfz_api': {'5': 'Partially Canceled', '8': 'Filled', '6': 'Canceled'},
            #     'gtja_pluto': {'4': 'New', '6': 'Partially Filled', '7': 'Filled', '8': 'Partially Canceled',
            #                    '9': 'Canceled', '5': 'Rejected', '10': 'Pending Cancel', '2': 'Pending New'},
            #     'yh_apama': {'2': 'New', '5': 'Partially Filled', '8': 'Filled', '7': 'Partially Filled',  # todo 看表确认
            #                  '6': 'Canceled', '9': 'Rejected', '3': 'Pending Cancel', '1': 'Pending New'},
            #     'huat_matic_tsi': {'2': 'New', '7': 'Partially Filled', '8': 'Filled', '5': 'Partially Filled',
            #                        '6': 'Canceled', '9': 'Rejected', '4': 'Pending Cancel', '1': 'Pending New'},
            #     'zx_wealthcats': {'部撤': 'Partially Filled', '全成': 'Filled', '全撤': 'Canceled', '废单': 'Rejected'},
            #     'xtpb': {'部成': 'Partially Filled', '已成': 'Filled', '已撤': 'Canceled', '废单': 'Rejected', '部撤': 'Partially Filled'},
            #     'gt_tyt': {'8': 'Filled'},
            #     'ax_jzpb': {'已成': 'Filled', '已撤': 'Canceled', '废单': 'Rejected',
            #                 '部撤': 'Partially Filled', '已报': 'New'},
            #     'htpb': {'已成': 'Filled', '已撤': 'Canceled', '废单': 'Rejected', '部撤': 'Partially Filled'},
            #     }
            list_date_format = ['%Y%m%d']
            list_time_format = ['%H%M%S', '%H:%M:%S', '%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S']
            # -------------  SECURITY LOAN 相关列表  ---------------------
            # todo 加hait_xtpb; huat_matic参考其手册;
            #  pluto 合约类型，合约状态里的1和huat里的1指代一个吗？
            #  这块 有不少问题！！！目前只关注short暂不会出错
            list_fields_shortqty = ['未还合约数量', 'real_compact_amount', '未还负债数量', '发生数量']  # 未还合约数量一般是开仓数量
            # 合约和委托没有关系了，但是用contract还是compact(券商版）?
            list_fields_contractqty = ['合约开仓数量', 'business_amount', '成交数量']  # 国外sell short约为“融券卖出”
            # list_fields_contracttype = ['合约类型', 'compact_type']  # 一定能分开 锁券与否
            # list_fields_contractstatus = ['合约状态', 'compact_status', '@负债现状']  # filled='完成'那不是委托？融资融券能用
            list_fields_opdate = ['合约开仓日期', 'open_date', '发生日期']  # FIX 合约: contract
            list_fields_sernum = ['成交编号', '合同编号', 'entrust_no', '委托序号', '合约编号', '合同号', 'instr_no', '成交序号',
                                  '订单号', '委托编号']
            # SerialNumber 券商不统一，目前方便区分是否传了两遍..然而entrust_no还是重复 (RZRQ里的business_no)可以
            list_fields_compositesrc = []  # todo CompositeSource

            # todo: 其它名字’开仓未归还‘，私用融券（专项券池）等得之后补上, 像上面做一个 ambigu区分
            #  遇到bug，pluto vs matic 2指代不一样的
            # Note3. contractstatus, contracttype 有些标准乱，以后有用处理
            # dict_contractstatus_fmt = {'部分归还': '部分归还', '未形成负债': None, '已归还': '已归还',
            #                            '0': '开仓未归还', '1': '部分归还', '5': None,
            #                            '2': '已归还/合约过期', '3': None,
            #                            '未归还': '开仓未归还', '自行了结': None}  # 有bug了...pluto vs matic
            #
            # dict_contracttype_fmt = {'融券': 'rq', '融资': 'rz',
            #                          '1': 'rq', '0': 'rz',
            #                          '2': '其它负债/？？？'}  # 一般没有融资, 其它负债（2）

            if sheet_type == 'fund':
                list_dicts_fund = raw_list
                if list_dicts_fund is None:
                    list_dicts_fund = []
                for dict_fund in list_dicts_fund:
                    data_source = dict_fund['DataSourceType']
                    str_updatetime = dict_fund['UpdateTime']
                    cash = None
                    avlfund = None
                    ttasset = None
                    mktvalue = None
                    netasset = None
                    kqzj = None
                    total_liability = None

                    flag_check_new_name = True  # 用来弥补之前几个list的缺漏
                    for field_af in list_fields_af:
                        if field_af in dict_fund:
                            avlfund = float(dict_fund[field_af])
                            # todo patchdata fund 处理 有的券商负债的券不一样
                            flag_check_new_name = False
                    err = 'unknown available_fund name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_fund)
                            logger_expo.debug((err, dict_fund))

                    if accttype == 'm':
                        flag_check_new_name = True
                        for field_ttasset in list_fields_ttasset:
                            if field_ttasset in dict_fund:
                                ttasset = float(dict_fund[field_ttasset])
                                flag_check_new_name = False
                        err = 'unknown total asset name %s' % data_source
                        if flag_check_new_name:
                            if data_source not in ['gy_htpb', 'gs_htpb']:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))
                            else:
                                ttasset = float(dict_fund['产品总资产'])

                        flag_check_new_name = True
                        for field_mktv in list_fields_mktvalue:
                            if field_mktv in dict_fund:
                                mktvalue = float(dict_fund[field_mktv])
                                flag_check_new_name = False
                        err = 'unknown total market value name %s' % data_source
                        if flag_check_new_name:
                            if data_source not in ['gtja_pluto']:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))
                        else:
                            cash = ttasset - mktvalue

                        # 读取净资产，总负债，或者两者之中推出另一个
                        for field_na in list_fields_na:
                            if field_na in dict_fund:
                                netasset = float(dict_fund[field_na])

                        for field_tl in list_fields_tl:
                            if field_tl in dict_fund:
                                total_liability = float(dict_fund[field_tl])

                        if total_liability and netasset:
                            delta = total_liability + netasset - ttasset
                            if abs(delta) > 1:
                                if data_source in ['swhy_xtpb']:
                                    netasset = ttasset - total_liability
                                else:
                                    err = '券商%s数据错误：总资产 - 总负债 - 净资产 =%d' % (data_source, -delta)
                                    if err not in self.list_warn:
                                        self.list_warn.append(err)
                                        logger_expo.error((err, dict_fund))
                                        print(err, dict_fund)
                                    # 默认总资产正确（swhy_xtpb净资产数据错误）：
                                    netasset = ttasset - total_liability
                        else:
                            if data_source in ['gy_htpb', 'gs_htpb', 'gj_htpb']:
                                netasset = float(dict_fund['产品净值'])
                            elif data_source in []:  # 没有净资产等字段
                                pass
                            elif not (total_liability is None):
                                netasset = ttasset - total_liability
                            elif not (netasset is None):
                                total_liability = ttasset - netasset
                            else:
                                err = 'unknown net asset or liability name %s' % data_source
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))

                    else:
                        flag_check_new_name = True
                        for field_ttasset in list_fields_ttasset + list_fields_na:
                            if field_ttasset in dict_fund:
                                ttasset = float(dict_fund[field_ttasset])
                                flag_check_new_name = False
                        err = 'unknown total asset name %s' % data_source
                        if flag_check_new_name:
                            if data_source not in ['gy_htpb', 'gs_htpb', 'gj_htpb']:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_fund)
                                    logger_expo.debug((err, dict_fund))
                            else:
                                ttasset = float(dict_fund['产品总资产'])
                        netasset = ttasset
                        total_liability = 0
                        cash = avlfund

                    flag_check_new_name = True
                    for field_kqzj in list_fields_kqzj:
                        if field_kqzj in dict_fund:
                            kqzj = float(dict_fund[field_kqzj])
                            flag_check_new_name = False
                    err = 'unknown KQZJ name %s' % data_source

                    if flag_check_new_name and data_source not in (
                            ['gf_tyt', 'zhaos_xtpb'] + self.gl.list_data_source_types_xtqmt_jjb
                    ):
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_fund)
                            logger_expo.debug((err, dict_fund))

                    dict_fund_fmtted = {
                        'DataDate': self.gl.str_today,
                        'UpdateTime': str_updatetime,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': data_source,
                        'Cash': cash,
                        'NetAsset': netasset,
                        'AvailableFund': avlfund,
                        'TotalAsset': ttasset,
                        'TotalLiability': total_liability,
                        'KQZJ': kqzj
                    }
                    list_dicts_fmtted.append(dict_fund_fmtted)

            elif sheet_type == 'holding':
                # 2.1 rawdata(无融券合约账户)
                list_dicts_holding = raw_list

                for dict_holding in list_dicts_holding:
                    str_updatetime = dict_holding['UpdateTime']
                    secid = None
                    secidsrc = None
                    symbol = None
                    data_source = dict_holding['DataSourceType']
                    longqty = 0
                    flag_check_new_name = True
                    for field_secid in list_fields_secid:
                        if field_secid in dict_holding:
                            secid = str(dict_holding[field_secid])
                            flag_check_new_name = False
                    err = 'unknown secid name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_holding)
                            logger_expo.debug((err, dict_holding))

                    flag_check_new_name = True

                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_holding:
                            shareholder_acctid = str(dict_holding[field_shareholder_acctid])
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'
                            flag_check_new_name = False

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_holding:
                            try:
                                if data_source in dict_ambiguous_secidsrc:
                                    digit_exchange = str(dict_holding[field_exchange])
                                    secidsrc = dict_ambiguous_secidsrc[data_source][digit_exchange]
                                else:
                                    exchange = dict_holding[field_exchange]
                                    secidsrc = dict_exchange2secidsrc[exchange]
                            except KeyError as err:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_holding)
                                    logger_expo.debug((err, dict_holding))
                            flag_check_new_name = False
                            break

                    err = 'unknown security source name %s' % data_source
                    if flag_check_new_name:
                        secidsrc = self.id2source.find_exchange(secid)
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_holding)
                            logger_expo.warning(err)

                    flag_check_new_name = True
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_holding:
                            symbol = str(dict_holding[field_symbol])
                            flag_check_new_name = False

                    err = 'unknown symbol name %s' % data_source
                    if flag_check_new_name:
                        if data_source in (
                                ['hait_ehfz_api', 'yh_apama', 'gf_tyt'] + self.gl.list_data_source_types_xtqmt_jjb
                        ):
                            symbol = '???'
                        else:
                            if err not in self.list_warn:
                                self.list_warn.append(err)
                                print(err, dict_holding)
                                logger_expo.debug((err, dict_holding))

                    flag_check_new_name = True
                    for field_longqty in list_fields_longqty:
                        if field_longqty in dict_holding:
                            longqty = float(dict_holding[field_longqty])
                            flag_check_new_name = False

                    err = 'unknown longqty name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_holding)
                            logger_expo.debug((err, dict_holding))

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = get_sectype_from_code(windcode)

                    # todo 若持仓中的股票代码不在华泰数据字典redis中，则按0元处理
                    if sectype in ['CS']:
                        b_dict_md = self.server_redis_md.get(f'market_{windcode}')

                        if b_dict_md:
                            lastpx = orjson.loads(b_dict_md)['LastPx'] / 10000
                        else:
                            str_md_not_found = f'在行情redis中未找到{windcode}的行情数据'
                            if str_md_not_found not in self.list_warn:
                                print(str_md_not_found)
                                self.list_warn.append(str_md_not_found)
                            lastpx = 0
                        longamt = lastpx * longqty

                    elif sectype in ['CE']:
                        lastpx = 100
                        longamt = lastpx * longqty

                    else:
                        lastpx = None
                        longamt = None

                    dict_holding_fmtted = {
                        'DataDate': self.gl.str_today,
                        'UpdateTime': str_updatetime,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': data_source,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'LastPx': lastpx,
                        'LongQty': longqty,
                        'LongAmt': longamt,
                    }
                    list_dicts_fmtted.append(dict_holding_fmtted)

            elif sheet_type == 'order':  # 3.order
                list_dicts_order = raw_list
                for dict_order in list_dicts_order:
                    secid = None
                    secidsrc = None
                    symbol = None
                    leavesqty = None
                    cumqty = None
                    side = None
                    orderqty = None
                    transtime = None
                    avgpx = None
                    sernum = None
                    data_source = dict_order['DataSourceType']
                    str_updatetime = dict_order['UpdateTime']

                    # todo 数据筛选器
                    if data_source in ['huat_matic_tsi']:
                        if not dict_order['stock_code']:
                            continue

                    flag_check_new_name = True
                    for field_secid in list_fields_secid:
                        if field_secid in dict_order:
                            secid = str(dict_order[field_secid])
                            flag_check_new_name = False
                    err = 'unknown secid name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    flag_check_new_name = True
                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_order:
                            shareholder_acctid = str(dict_order[field_shareholder_acctid])
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'
                            flag_check_new_name = False

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_order:
                            try:
                                if data_source in dict_ambiguous_secidsrc:
                                    digit_exchange = dict_order[field_exchange]
                                    secidsrc = dict_ambiguous_secidsrc[data_source][digit_exchange]
                                else:
                                    exchange = dict_order[field_exchange]
                                    secidsrc = dict_exchange2secidsrc[exchange]
                            except KeyError as err:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_order)
                                    logger_expo.debug(err, dict_order)
                            flag_check_new_name = False

                    err = 'unknown security source name %s' % data_source
                    if flag_check_new_name:
                        secidsrc = self.id2source.find_exchange(secid)
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.warning(err)

                    flag_check_new_name = True
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_order:
                            symbol = str(dict_order[field_symbol])
                            flag_check_new_name = False

                    err = 'unknown symbol name %s' % data_source
                    if flag_check_new_name:
                        if data_source in ['hait_ehfz_api', 'yh_apama', 'gf_tyt'] + self.gl.list_data_source_types_xtqmt_jjb:
                            symbol = ''  # 这几个券商中没有提供symbol
                        else:
                            if err not in self.list_warn:
                                self.list_warn.append(err)
                                print(err, dict_order)
                                logger_expo.debug((err, dict_order))

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = get_sectype_from_code(windcode)

                    flag_check_new_name = True
                    for field_cumqty in list_fields_cumqty:
                        if field_cumqty in dict_order:
                            cumqty = dict_order[field_cumqty]
                            flag_check_new_name = False

                    err = 'unknown cumqty name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    flag_check_new_name = True
                    for field_leavesqty in list_fields_leavesqty:
                        if field_leavesqty in dict_order:
                            leavesqty = dict_order[field_leavesqty]
                            flag_check_new_name = False

                    err = 'unknown leavesqty name %s' % data_source
                    if flag_check_new_name:
                        if data_source in ['yh_apama']:
                            pass
                        else:
                            if err not in self.list_warn:
                                self.list_warn.append(err)
                                print(err, dict_order)
                                logger_expo.debug((err, dict_order))

                    if data_source == 'huat_matic_tsi':
                        entrust_bs = int(dict_order['entrust_bs'])
                        entrust_type = int(dict_order['entrust_type'])
                        try:  # entrust_type: 0普通委托, 2撤单, 6融资, 7融券, 9信用交易
                            side = {(9, 1): 'buy', (9, 2): 'sell', (7, 2): 'sell short',  # 6: 融资买入/卖券还款
                                    (7, 1): 'MQHQ', (6, 1): 'buy', (6, 2): 'sell',
                                    (0, 1): 'buy', (0, 2): 'sell'}[(entrust_type, entrust_bs)]
                        except KeyError:
                            side = 'cancel'
                    else:
                        flag_check_new_name = True
                        for field_side in list_fields_side:
                            if field_side in dict_order:
                                if data_source in dict_ambiguous_side_name:
                                    digit_side = dict_order[field_side]
                                    side = dict_ambiguous_side_name[data_source][digit_side]
                                else:
                                    str_side = dict_order[field_side]
                                    side = dict_fmtted_side_name[str_side]
                            flag_check_new_name = False

                    err = 'unknown side name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    flag_check_new_name = True
                    for field_orderqty in list_fields_orderqty:
                        if field_orderqty in dict_order:
                            orderqty = dict_order[field_orderqty]
                            flag_check_new_name = False
                    err = 'unknown orderqty name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    flag_check_new_name = True
                    for field_transtime in list_fields_ordertime:
                        if field_transtime in dict_order:
                            transtime = dict_order[field_transtime]
                            # 转化成统一时间格式
                            datetime_obj = None
                            for time_format in list_time_format:
                                try:
                                    datetime_obj = datetime.strptime(transtime, time_format)
                                except ValueError:
                                    pass
                            if datetime_obj:
                                transtime = datetime_obj.strftime('%H%M%S')
                            else:
                                err = 'unknown transtime format %s' % data_source
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_order)
                                    logger_expo.debug((err, dict_order))
                            flag_check_new_name = False

                    err = 'unknown transaction time name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    flag_check_new_name = True
                    for field_sernum in list_fields_sernum:
                        if field_sernum in dict_order:
                            sernum = str(dict_order[field_sernum])
                            flag_check_new_name = False
                    err = 'unknown serial number name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    flag_check_new_name = True
                    for field_avgpx in list_fields_avgpx:
                        if field_avgpx in dict_order:
                            avgpx = float(dict_order[field_avgpx])
                            flag_check_new_name = False
                    err = 'unknown average price name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_order)
                            logger_expo.debug((err, dict_order))

                    dict_order_fmtted = {
                        'DataDate': self.gl.str_today,
                        'UpdateTime': str_updatetime,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': data_source,
                        'SecurityID': secid,
                        'SerialNumber': sernum,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'CumQty': cumqty,
                        'Side': side,
                        'OrdQty': orderqty,
                        'LeavesQty': leavesqty,
                        'TransactTime': transtime,
                        'AvgPx': avgpx
                    }

                    list_dicts_fmtted.append(dict_order_fmtted)

            elif sheet_type == 'short_position':
                list_dicts_secloan = raw_list
                for dict_secloan in list_dicts_secloan:
                    secid = None
                    secidsrc = None
                    symbol = None
                    shortqty = 0
                    contractstatus = None
                    contracttype = None
                    contractqty = None
                    opdate = None
                    sernum = None
                    compositesrc = None
                    data_source = dict_secloan['DataSourceType']
                    str_updatetime = dict_secloan['UpdateTime']

                    flag_check_new_name = True
                    for field_secid in list_fields_secid:
                        if field_secid in dict_secloan:
                            secid = str(dict_secloan[field_secid])
                            flag_check_new_name = False
                    err = 'unknown field_secid name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_shareholder_acctid in list_fields_shareholder_acctid:
                        if field_shareholder_acctid in dict_secloan:
                            shareholder_acctid = str(dict_secloan[field_shareholder_acctid])
                            if len(shareholder_acctid) == 0:
                                continue
                            if shareholder_acctid[0].isalpha():
                                secidsrc = 'SSE'
                            if shareholder_acctid[0].isdigit():
                                secidsrc = 'SZSE'
                            flag_check_new_name = False

                    for field_exchange in list_fields_exchange:
                        if field_exchange in dict_secloan:
                            try:
                                if data_source in dict_ambiguous_secidsrc:
                                    digit_exchange = dict_secloan[field_exchange]
                                    secidsrc = dict_ambiguous_secidsrc[data_source][digit_exchange]
                                else:
                                    exchange = dict_secloan[field_exchange]
                                    secidsrc = dict_exchange2secidsrc[exchange]
                            except KeyError as err:
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_secloan)
                                    logger_expo.debug(err, dict_secloan)
                            flag_check_new_name = False

                    err = 'unknown security source name %s' % data_source
                    if flag_check_new_name:
                        secidsrc = self.id2source.find_exchange(secid)
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.warning(err)

                    flag_check_new_name = True
                    for field_symbol in list_fields_symbol:
                        if field_symbol in dict_secloan:
                            symbol = str(dict_secloan[field_symbol])
                            flag_check_new_name = False
                    err = 'unknown field symbol name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_shortqty in list_fields_shortqty:
                        if field_shortqty in dict_secloan:
                            shortqty = float(dict_secloan[field_shortqty])
                            flag_check_new_name = False
                    err = 'unknown field shortqty name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_contractqty in list_fields_contractqty:
                        if field_contractqty in dict_secloan:
                            contractqty = str(dict_secloan[field_contractqty])
                        flag_check_new_name = False
                    err = 'unknown field contractqty name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_sernum in list_fields_sernum:
                        if field_sernum in dict_secloan:
                            sernum = str(dict_secloan[field_sernum])
                            flag_check_new_name = False
                    err = 'unknown field serum name %s' % data_source
                    if flag_check_new_name:
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    # flag_check_new_name = True
                    # for field_contractstatus in list_fields_contractstatus:
                    #     if field_contractstatus in dict_secloan:
                    #         contractstatus = str(dict_secloan[field_contractstatus])
                    #         if contractstatus in dict_contractstatus_fmt:
                    #             contractstatus = dict_contractstatus_fmt[contractstatus]
                    #         else:
                    #             logger_expo.debug('Unknown contractstatus %s'%contractstatus)
                    #         # if contractstatus is None:
                    #         #     raise Exception('During Clearing, we can not have ambiguous status in the compact')
                    #         flag_check_new_name = False
                    #
                    # if flag_check_new_name:
                    #     logger_expo.debug(('unknown field_contractstatus name', dict_secloan))

                    # flag_check_new_name = True
                    # for field_contracttype in list_fields_contracttype:
                    #     if field_contracttype in dict_secloan:
                    #         contracttype = str(dict_secloan[field_contracttype])
                    #         if contracttype in dict_contracttype_fmt:
                    #             contracttype = dict_contracttype_fmt[contracttype]
                    #         else:
                    #             logger_expo.debug('Unknown contractstatus %s'%contracttype)
                    #         flag_check_new_name = False
                    # if flag_check_new_name:
                    #     if data_source != 'hait_ehfz_api':
                    #         logger_expo.debug(('unknown field_contracttype name', dict_secloan))

                    flag_check_new_name = True
                    for field_opdate in list_fields_opdate:
                        if field_opdate in dict_secloan:
                            opdate = str(dict_secloan[field_opdate])
                            flag_check_new_name = False
                            datetime_obj = None
                            # 和order共用 date格式
                            for date_format in list_date_format:
                                try:
                                    datetime_obj = datetime.strptime(opdate, date_format)
                                except ValueError:
                                    pass
                            if datetime_obj:
                                opdate = datetime_obj.strftime('%Y%m%d')
                            else:
                                err = 'Unrecognized trade date format %s' % data_source
                                if err not in self.list_warn:
                                    self.list_warn.append(err)
                                    print(err, dict_secloan)
                                    logger_expo.debug((err, dict_secloan))

                    if flag_check_new_name:
                        err = 'unknown field opdate name %s' % data_source
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    flag_check_new_name = True
                    for field_compositesrc in list_fields_compositesrc:
                        if field_compositesrc in dict_secloan:
                            compositesrc = str(dict_secloan[field_compositesrc])
                            flag_check_new_name = False
                    if flag_check_new_name and list_fields_compositesrc:
                        err = 'unknown field_compositesrc name %s' % data_source
                        if err not in self.list_warn:
                            self.list_warn.append(err)
                            print(err, dict_secloan)
                            logger_expo.debug((err, dict_secloan))

                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                    windcode = secid + windcode_suffix
                    sectype = get_sectype_from_code(windcode)

                    if sectype in ['CS']:
                        lastpx = orjson.loads(self.server_redis_md.get(f"market_{windcode}"))['LastPx'] / 10000
                        shortamt = lastpx * shortqty

                    elif sectype in ['MMF']:
                        lastpx = 100
                        shortamt = lastpx * shortqty
                    else:
                        shortamt = None

                    dict_secloan_fmtted = {
                        'DataDate': self.gl.str_today,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': data_source,
                        'UpdateTime': str_updatetime,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                        'Symbol': symbol,
                        'SecurityIDSource': secidsrc,
                        'SerialNumber': sernum,
                        'OpenPositionDate': opdate,  # Not Important
                        'ContractStatus': contractstatus,
                        'ContractType': contracttype,
                        'ContractQty': contractqty,
                        'CompositeSource': compositesrc,
                        'ShortQty': shortqty,
                        'ShortAmt': shortamt,
                    }
                    list_dicts_fmtted.append(dict_secloan_fmtted)
            else:
                raise ValueError('Unknown f_h_o_s_mark')

        elif accttype in ['f'] and patchpath is None:
            if sheet_type == 'fund':
                list_dicts_future_fund = raw_list
                for dict_fund_future in list_dicts_future_fund:
                    str_updatetime = dict_fund_future['UpdateTime']
                    cash = dict_fund_future['DYNAMICBALANCE']
                    acctidbymxz = dict_fund_future['AcctIDByMXZ']
                    kyzj = dict_fund_future['USABLECURRENT']
                    dict_future_fund_fmtted = {
                        'DataDate': self.gl.str_today,
                        'UpdateTime': str_updatetime,
                        'AcctIDByMXZ': acctidbymxz,
                        'DataSourceType': 'trader_api',
                        'Cash': cash,
                        'NetAsset': cash,
                        'TotalAsset': cash,
                        'AvailableFund': kyzj,
                    }
                    list_dicts_fmtted.append(dict_future_fund_fmtted)
                # todo 期货的合约直接放到 position, 双向(其实holding应该抽象为long_position)。

            elif sheet_type == 'holding':
                list_dicts_future_holding = raw_list
                for dict_holding_future in list_dicts_future_holding:
                    str_updatetime = dict_holding_future['UpdateTime']
                    secid = dict_holding_future['instrument_id']
                    secidsrc = dict_holding_future['exchange']

                    if secid[:-4] in ['IC', 'IF', 'IH']:
                        sectype = 'IndexFuture'
                        windcode = self.gl.dict_future2spot[secid[:-4]]
                        lastpx = orjson.loads(self.server_redis_md.get(f'index_{windcode}'))['LastIndex'] / 10000

                    else:
                        sectype = 'CommodityFuture'
                        windcode = f'{secid}.{secidsrc}'
                        # todo
                        lastpx = orjson.loads(self.server_redis_md.get(f'market_{windcode}'))['LastPrice'] / 10000

                    multiplier = self.gl.dict_future2multiplier[secid[:-4]]
                    acctidbymxz = dict_holding_future['AcctIDByMXZ']
                    direction = dict_holding_future['direction']
                    if direction in ['buy']:
                        longqty = dict_holding_future['position']
                        longamt = longqty * lastpx * multiplier
                        dict_holding_fmtted = {
                            'DataDate': self.gl.str_today,
                            'UpdateTime': str_updatetime,
                            'AcctIDByMXZ': acctidbymxz,
                            'DataSourceType': 'trader_api',
                            'SecurityID': secid,
                            'SecurityType': sectype,
                            'Symbol': '',
                            'SecurityIDSource': secidsrc,
                            'LastPx': lastpx,
                            'LongQty': longqty,
                            'LongAmt': longamt,
                        }
                        list_dicts_fmtted.append(dict_holding_fmtted)

            elif sheet_type in ['order']:
                pass

            elif sheet_type == 'short_position':
                list_dicts_future_short_position = raw_list
                for dict_holding_future_short_position in list_dicts_future_short_position:
                    str_updatetime = dict_holding_future_short_position['UpdateTime']
                    acctidbymxz = dict_holding_future_short_position['AcctIDByMXZ']
                    direction = dict_holding_future_short_position['direction']
                    secid = dict_holding_future_short_position['instrument_id']
                    secidsrc = dict_holding_future_short_position['exchange']

                    if secid[:-4] in ['IC', 'IF', 'IH']:
                        sectype = 'IndexFuture'
                        windcode = self.gl.dict_future2spot[secid[:-4]]
                        lastpx = orjson.loads(self.server_redis_md.get(f'index_{windcode}'))['LastIndex'] / 10000
                        multiplier = self.gl.dict_future2multiplier[secid[:-4]]

                    else:
                        sectype = 'CommodityFuture'
                        windcode = f'{secid}.{secidsrc}'
                        lastpx = orjson.loads(self.server_redis_md.get(f'market_{windcode}')['LastPrice']) / 10000
                        multiplier = self.gl.dict_future2multiplier[secid[:-4]]

                    if direction in ['sell']:
                        shortqty = dict_holding_future_short_position['position']
                        shortamt = shortqty * lastpx * multiplier
                        dict_trddata_fmtted_short_position = {
                            'DataDate': self.gl.str_today,
                            'UpdateTime': str_updatetime,
                            'AcctIDByMXZ': acctidbymxz,
                            'DataSourceType': 'trader_api',
                            'SecurityID': secid,
                            'SecurityType': sectype,
                            'Symbol': '',
                            'SecurityIDSource': secidsrc,
                            'LastPx': lastpx,
                            'ShortQty': shortqty,
                            'ShortAmt': shortamt,
                        }
                        list_dicts_fmtted.append(dict_trddata_fmtted_short_position)
            else:
                raise ValueError('Unknown sheet type.')

        elif patchpath:
            if accttype == 'o':
                # todo patch 里场外暂时放放
                pass
            else:
                df = pd.read_excel(patchpath, dtype=str, sheet_name=sheet_type)
                df = df.where(df.notnull(), None)
                for i, row in df.iterrows():
                    doc = dict(row)
                    doc['UpdateTime'] = datetime.now().strftime('%H%M%S')
                    doc['DataDate'] = self.gl.str_today
                    list_dicts_fmtted.append(doc)
        else:
            logger_expo.debug('Unknown account type in basic account info.')
        return list_dicts_fmtted

    def update_trdfmt_cmfo(self):
        dict_acct2patch = {}
        for _ in self.gl.col_datapatch.find({'DataDate': self.gl.str_today}):
            acctid = _['AcctIDByMXZ']
            if acctid in dict_acct2patch:
                dict_acct2patch[acctid].append(_)
            else:
                dict_acct2patch[acctid] = [_]

        dict_sheet_type2col = {
            'fund': self.gl.col_trade_rawdata_fund,
            'holding': self.gl.col_trade_rawdata_holding,
            'order': self.gl.col_trade_rawdata_order,
            'short_position': self.gl.col_trade_rawdata_short_position
        }

        dict_fmt_col = {
            'fund': self.gl.col_trade_fmtdata_fund,
            'holding': self.gl.col_trade_fmtdata_holding,
            'order': self.gl.col_trade_fmtdata_order,
            'short_position': self.gl.col_trade_fmtdata_short_position
        }

        # todo 将future账户视为股票账户，重构
        dict_sheet_type2dict_acctidbymxz2list_dicts_trdraw = {}
        for sheet_type in dict_sheet_type2col:
            col = dict_sheet_type2col[sheet_type]
            dict_acctidbymxz2list_dicts_trdraw = {}
            for dict_trdraw in col.find({'DataDate': self.gl.str_today}, {'_id': 0}):
                acctidbymxz = dict_trdraw["AcctIDByMXZ"]
                if acctidbymxz in dict_acctidbymxz2list_dicts_trdraw:
                    dict_acctidbymxz2list_dicts_trdraw[acctidbymxz].append(dict_trdraw)
                else:
                    dict_acctidbymxz2list_dicts_trdraw[acctidbymxz] = [dict_trdraw]
            dict_sheet_type2dict_acctidbymxz2list_dicts_trdraw.update({sheet_type: dict_acctidbymxz2list_dicts_trdraw})

        dict_shtype2listfmtted = {'fund': [], 'holding': [], 'order': [], 'short_position': []}
        for dict_acctinfo in self.gl.col_acctinfo.find(
                {'DataDate': self.gl.str_today, 'DataDownloadMark': 1}, {'_id': 0}
        ):
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            accttype = dict_acctinfo['AcctType']
            patchpaths = {}
            if dict_acctinfo['PatchMark']:
                for _ in dict_acct2patch[acctidbymxz]:
                    patchpaths[_['SheetName']] = _['DataFilePath']

            for sheet_type in dict_sheet_type2dict_acctidbymxz2list_dicts_trdraw.keys():
                if sheet_type in patchpaths:
                    patchpath = patchpaths[sheet_type]
                else:
                    patchpath = None

                # 有patch就不从数据库里取
                if acctidbymxz in dict_sheet_type2dict_acctidbymxz2list_dicts_trdraw[sheet_type] and patchpath is None:
                    raw_list = dict_sheet_type2dict_acctidbymxz2list_dicts_trdraw[sheet_type][acctidbymxz]
                elif patchpath:
                    raw_list = None
                else:
                    continue

                # Note3. raw_list  {stock:{fund:{acctid: [准备format的list]}}}里的 ‘准备format的list’
                list_dicts_fmtted = self.formulate_raw_data(acctidbymxz, accttype, patchpath, sheet_type, raw_list)
                dict_shtype2listfmtted[sheet_type] += list_dicts_fmtted

        for sheet_type in dict_shtype2listfmtted:
            list_dicts_fmtted = dict_shtype2listfmtted[sheet_type]
            col_2b_inserted = dict_fmt_col[sheet_type]
            if list_dicts_fmtted:
                col_2b_inserted.delete_many({'DataDate': self.gl.str_today})
                col_2b_inserted.insert_many(list_dicts_fmtted)
        print('Update formatted data finished.')

    def update_position(self):
        list_dicts_position = []
        dict_id2info = {}
        dict_pair2allcol = {}
        # 存成字典dict_pair2allcol： {pair: {holding:[...], order:[...], secloan: []},之后遍历每一个key
        dict_learn_secid2src = {}  # 有的post里面没有source，得用fmt里的“学”

        for dict_acctinfo in self.gl.col_acctinfo.find(
                {'DataDate': self.gl.str_today, 'DataDownloadMark': 1, }
        ):
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            accttype = dict_acctinfo['AcctType']
            patchmark = dict_acctinfo['PatchMark']
            data_source = dict_acctinfo['DataSourceType']
            dict_id2info.update({acctidbymxz: [accttype, patchmark, data_source]})

        # Note4. pair作用： 相同的acctid, 证券对应唯一的position，但是holding，合约，order可能不止一个
        # Note4. 上面的dict_id2info作用在于把acctinfo拍扁成dict，pair -> acctid -> 账户信息

        for col_name in ['trade_formatted_data_order', 'trade_formatted_data_holding']:
            for dict_trade_data in self.gl.db_trade_data[col_name].find(
                    {'DataDate': self.gl.str_today}, {'_id': 0}
            ):
                secid = dict_trade_data['SecurityID']
                secidsrc = dict_trade_data['SecurityIDSource']
                acctidbymxz = dict_trade_data['AcctIDByMXZ']
                sectype = dict_trade_data['SecurityType']
                tuple_pair = (acctidbymxz, secid, secidsrc, sectype)

                if secid in dict_learn_secid2src:
                    if dict_learn_secid2src[secid] != secidsrc:
                        dict_learn_secid2src[secid] = None
                else:
                    dict_learn_secid2src.update({secid: secidsrc})

                dict_trade_data_copy = dict_trade_data.copy()
                if tuple_pair in dict_pair2allcol:
                    if col_name in dict_pair2allcol[tuple_pair]:
                        dict_pair2allcol[tuple_pair][col_name].append(dict_trade_data_copy)
                    else:
                        dict_pair2allcol[tuple_pair].update({col_name: [dict_trade_data_copy]})
                else:
                    dict_pair2allcol.update({tuple_pair: {col_name: [dict_trade_data_copy]}})


        for col_name in ['post_trade_formatted_data_short_position']:
            for dict_post_trade_data_last_trddate in self.gl.db_post_trade_data[col_name].find(
                    {'DataDate': self.gl.str_last_trddate}, {'_id': 0}
            ):
                acctidbymxz = dict_post_trade_data_last_trddate['AcctIDByMXZ']
                secid = dict_post_trade_data_last_trddate['SecurityID']
                secidsrc = dict_post_trade_data_last_trddate['SecurityIDSource']
                sectype = dict_post_trade_data_last_trddate['SecurityType']

                if 'SecurityType' not in dict_post_trade_data_last_trddate:
                    if 'SecurityIDSource' not in dict_post_trade_data_last_trddate:
                        secid = dict_post_trade_data_last_trddate['SecurityID']
                        if secid in dict_learn_secid2src:
                            dict_post_trade_data_last_trddate['SecurityIDSource'] = dict_learn_secid2src[secid]
                        else:
                            dict_post_trade_data_last_trddate['SecurityIDSource'] = self.id2source.find_exchange(secid)
                    windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[dict_post_trade_data_last_trddate['SecurityIDSource']]
                    dict_post_trade_data_last_trddate['SecurityType'] = get_sectype_from_code(
                        dict_post_trade_data_last_trddate['SecurityID'] + windcode_suffix
                    )
                tuple_pair = (acctidbymxz, secid, secidsrc, sectype)

                dict_post_trade_data_last_trddate_copy = dict_post_trade_data_last_trddate.copy()
                if tuple_pair in dict_pair2allcol:
                    if col_name in dict_pair2allcol[tuple_pair]:
                        dict_pair2allcol[tuple_pair][col_name].append(dict_post_trade_data_last_trddate_copy)
                    else:
                        dict_pair2allcol[tuple_pair].update({col_name: [dict_post_trade_data_last_trddate_copy]})
                else:
                    dict_pair2allcol.update({tuple_pair: {col_name: [dict_post_trade_data_last_trddate_copy]}})

        # 注: 此处只使用了 short position中的期货数据，margin account 不用short_position.
        for col_name in ['trade_formatted_data_short_position']:
            for dict_trade_data in self.gl.db_trade_data[col_name].find(
                    {'DataDate': self.gl.str_today}, {'_id': 0}
            ):
                secid = dict_trade_data['SecurityID']
                secidsrc = dict_trade_data['SecurityIDSource']
                acctidbymxz = dict_trade_data['AcctIDByMXZ']
                accttype = acctidbymxz.split('_')[1]
                sectype = dict_trade_data['SecurityType']

                if accttype in ['f']:
                    tuple_pair = (acctidbymxz, secid, secidsrc, sectype)
                    if secid in dict_learn_secid2src:
                        if dict_learn_secid2src[secid] != secidsrc:
                            dict_learn_secid2src[secid] = None
                    else:
                        dict_learn_secid2src.update({secid: secidsrc})

                    dict_trade_data_copy = dict_trade_data.copy()
                    if tuple_pair in dict_pair2allcol:
                        if col_name in dict_pair2allcol[tuple_pair]:
                            dict_pair2allcol[tuple_pair][col_name].append(dict_trade_data_copy)
                        else:
                            dict_pair2allcol[tuple_pair].update({col_name: [dict_trade_data_copy]})
                    else:
                        dict_pair2allcol.update({tuple_pair: {col_name: [dict_trade_data_copy]}})


        # for col_name in ['trade_future_api_holding']:
        #     for dict_trade_data in self.gl.db_trade_data[col_name].find({'DataDate': self.gl.str_today}):
        #         secid = dict_trade_data['instrument_id']
        #         secidsrc = dict_trade_data['exchange']
        #         acctidbymxz = dict_trade_data['AcctIDByMXZ']
        #
        #         # todo 假设 除了IC, IF, IH三类指数合约外, 为商品期货合约
        #         if secid[:-4] in ['IC', 'IF', 'IH']:
        #             sectype = 'IndexFuture'
        #         else:
        #             sectype = 'CommodityFuture'
        #
        #         tuple_pair = (acctidbymxz, secid, secidsrc, sectype)
        #
        #         dict_trade_data_copy = dict_trade_data.copy()
        #
        #         if tuple_pair in dict_pair2allcol:
        #             if col_name in dict_pair2allcol[tuple_pair]:
        #                 dict_pair2allcol[tuple_pair][col_name].append(dict_trade_data_copy)
        #             else:
        #                 dict_pair2allcol[tuple_pair].update({col_name: [dict_trade_data_copy]})
        #         else:
        #             dict_pair2allcol.update({tuple_pair: {col_name: [dict_trade_data_copy]}})

        for tuple_pair in dict_pair2allcol:
            acctidbymxz = tuple_pair[0]
            secid = tuple_pair[1]
            secidsrc = tuple_pair[2]
            sectype = tuple_pair[3]
            longqty = 0
            longamt = 0
            str_updatetime = ''

            try:
                accttype, patchmark, data_source = dict_id2info[acctidbymxz]
            except KeyError:
                continue

            if 'trade_formatted_data_holding' in dict_pair2allcol[tuple_pair]:
                list_dicts_trade_fmtdata_holding = dict_pair2allcol[tuple_pair]['trade_formatted_data_holding']
            else:
                list_dicts_trade_fmtdata_holding = []

            try:
                list_dicts_posttrd_fmtdata_short_position_last_trddate = (
                    dict_pair2allcol[tuple_pair]['post_trade_formatted_data_short_position']
                )
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_posttrd_fmtdata_short_position_last_trddate = []

            try:
                list_dicts_trade_fmtdata_short_position = (
                    dict_pair2allcol[tuple_pair]['trade_formatted_data_short_position']
                )
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_trade_fmtdata_short_position = []

            try:
                list_dicts_trade_fmtdata_order = dict_pair2allcol[tuple_pair]['trade_formatted_data_order']
            except KeyError:  # pair may not has 'fmtdata_holding' etc key
                list_dicts_trade_fmtdata_order = []

            if accttype in ['c', 'm', 'o']:
                if len(tuple_pair) == 4:
                    sectype = tuple_pair[3]

                windcode_suffix = {'SZSE': '.SZ', 'SSE': '.SH'}[secidsrc]
                windcode = secid + windcode_suffix

                shortqty_from_posttrd_last_trddate = 0
                if list_dicts_trade_fmtdata_holding:
                    for dict_trade_fmtdata_holding in list_dicts_trade_fmtdata_holding:
                        if dict_trade_fmtdata_holding['SecurityType'] in ['IrrelevantItem']:
                            continue
                        else:
                            longqty = float(dict_trade_fmtdata_holding['LongQty'])
                            longamt = float(dict_trade_fmtdata_holding['LongAmt'])
                            str_updatetime = dict_trade_fmtdata_holding['UpdateTime']

                if list_dicts_posttrd_fmtdata_short_position_last_trddate:
                    for dict_posttrd_fmtdata_short_position_last_trddate in list_dicts_posttrd_fmtdata_short_position_last_trddate:
                        shortqty_from_posttrd_last_trddate += dict_posttrd_fmtdata_short_position_last_trddate['ShortQty']

                # todo 此处只计算融券卖出情况： shortqty = shortqty_from_posttrd_last_trddate + order
                shortqty = shortqty_from_posttrd_last_trddate
                for dict_order in list_dicts_trade_fmtdata_order:
                    side = dict_order['Side']
                    cumqty = float(dict_order['CumQty'])
                    avgpx = dict_order['AvgPx']

                    if 'yh' in dict_order['AcctIDByMXZ']:
                        valid_cum = True
                    elif data_source == 'huat_matic_tsi' and avgpx == 0:
                        # 华泰有0元成交0元委托（不算撤单，成交量不为0的奇怪情况）
                        valid_cum = False
                    else:
                        leavesqty = float(dict_order['LeavesQty'])
                        orderqty = float(dict_order['OrdQty'])
                        valid_cum = (cumqty + leavesqty == orderqty)  # 判定order数据是否有效的数据

                    if valid_cum:  # 交易最终态的cumqty才可以用
                        if side == 'sell short':
                            shortqty = shortqty + cumqty
                        elif side == 'XQHQ':
                            shortqty = shortqty - cumqty
                        elif side == 'MQHQ':  # 导致资金变动而不是券的变动
                            shortqty = shortqty - cumqty
                        else:  # 判断撤单
                            continue
                    if not str_updatetime:
                        str_updatetime = dict_order['UpdateTime']

                # 出于节省查询次数考虑，只在shortqty有值时查询
                if shortqty:
                    if windcode in ['510500.SH']:
                        lastpx = 7.12  # todo 实时行情
                    else:
                        b_dict_md = self.server_redis_md.get(f'market_{windcode}')
                        lastpx = orjson.loads(b_dict_md)['LastPx'] / 10000
                    shortamt = shortqty * lastpx
                else:
                    shortqty = 0
                    shortamt = 0

                if not str_updatetime:
                    str_updatetime = datetime.now().strftime('%H%M%S')

                if longqty or shortqty:
                    dict_position = {
                        'DataDate': self.gl.str_today,
                        'UpdateTime': str_updatetime,
                        'AcctIDByMXZ': acctidbymxz,
                        'SecurityID': secid,
                        'SecurityType': sectype,
                        'SecurityIDSource': secidsrc,
                        'LongQty': longqty,
                        'ShortQty': shortqty,
                        'NetQty': longqty - shortqty,
                        'LongAmt': longamt,
                        'ShortAmt': shortamt,
                        'NetAmt': longamt - shortamt,
                        'WindCode': windcode
                    }
                    list_dicts_position.append(dict_position)

            elif accttype in ['f']:
                longqty = 0
                longamt = 0
                shortqty = 0
                shortamt = 0
                str_updatetime = ''

                for dict_trade_fmtdata in list_dicts_trade_fmtdata_holding:
                    str_updatetime = dict_trade_fmtdata['UpdateTime']
                    if dict_trade_fmtdata['SecurityID'] == secid:
                        longqty += dict_trade_fmtdata['LongQty']
                        longamt += dict_trade_fmtdata['LongAmt']

                for dict_trade_fmtdata in list_dicts_trade_fmtdata_short_position:
                    if not str_updatetime:
                        str_updatetime = dict_trade_fmtdata['UpdateTime']
                    if dict_trade_fmtdata['SecurityID'] == secid:
                        shortqty += dict_trade_fmtdata['ShortQty']
                        shortamt += dict_trade_fmtdata['ShortAmt']

                dict_position = {
                    'DataDate': self.gl.str_today,
                    'UpdateTime': str_updatetime,
                    'AcctIDByMXZ': acctidbymxz,
                    'SecurityID': secid,
                    'SecurityType': sectype,
                    'Symbol': None,
                    'SecurityIDSource': secidsrc,
                    'LongQty': longqty,
                    'ShortQty': shortqty,
                    'NetQty': longqty - shortqty,
                    'LongAmt': longamt,
                    'ShortAmt': shortamt,
                    'NetAmt': longamt - shortamt,
                }
                list_dicts_position.append(dict_position)
            else:
                raise ValueError('Unknown Accttype.')

        # for dict_position in list_dicts_position:
        #     windcode = dict_position['WindCode']
        #     if dict_position['SecurityType'] == 'Index Future':
        #         secid_first_part = dict_position['SecurityID'][:-4]
        #         point = self.gl.dict_future2multiplier[secid_first_part]
        #         windcode_spot = self.gl.dict_future2spot[secid_first_part]
        #         b_dict_md = server_redis_md.get(f'market_{windcode_spot}')
        #         lastpx = orjson.loads(b_dict_md)['LastPx'] / 10000
        #         dict_position['LongAmt'] = dict_position['LongQty'] * lastpx * point
        #         dict_position['ShortAmt'] = dict_position['ShortQty'] * lastpx * point
        #
        #     else:
        #         b_dict_md = server_redis_md.get(f'market_{windcode}')
        #         lastpx = orjson.loads(b_dict_md)['LastPx'] / 10000
        #         dict_position['LongAmt'] = dict_position['LongQty'] * lastpx
        #         dict_position['ShortAmt'] = dict_position['ShortQty'] * lastpx
        #     dict_position['NetAmt'] = dict_position['LongAmt'] - dict_position['ShortAmt']
        #     dict_position['UpdateTime'] = self.record_position_time
        # print(list_dicts_position)

        if list_dicts_position:
            self.gl.db_trade_data['trade_position'].delete_many({'DataDate': self.gl.str_today})
            self.gl.db_trade_data['trade_position'].insert_many(list_dicts_position)
        print('Update position finished.')

    def exposure_analysis(self):
        dict_acctid2list_position = {}
        str_updatetime = ''
        for dict_trade_position in self.gl.db_trade_data['trade_position'].find({'DataDate': self.gl.str_today}):
            acctidbymxz = dict_trade_position['AcctIDByMXZ']
            str_updatetime = dict_trade_position['UpdateTime']
            if acctidbymxz in dict_acctid2list_position:
                dict_acctid2list_position[acctidbymxz].append(dict_trade_position)
            else:
                dict_acctid2list_position[acctidbymxz] = [dict_trade_position]

        list_dict_acct_exposure = []
        dict_prdcode2exposure = {}
        for dict_acctinfo in self.gl.col_acctinfo.find({'DataDate': self.gl.str_today, 'DataDownloadMark': 1}):
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            accttype = dict_acctinfo['AcctType']
            prdcode = dict_acctinfo['PrdCode']
            mdm = dict_acctinfo['MonitorDisplayMark']
            acct_exposure_dict = {
                'AcctIDByMXZ': acctidbymxz, 'PrdCode': prdcode, 'MonitorDisplayMark': mdm, 'UpdateTime': str_updatetime,
                'DataDate': self.gl.str_today, 'LongQty': 0, 'ShortQty': 0, 'NetQty': 0, 'LongAmt': 0, 'ShortAmt': 0,
                'NetAmt': 0
            }

            if acctidbymxz in dict_acctid2list_position:
                for dict_position in dict_acctid2list_position[acctidbymxz]:
                    if (dict_position['SecurityType'] == 'IrrelevantItem'
                            or dict_position['SecurityType'] in ['CE', 'MMF']):
                        continue
                    else:
                        for key in ['LongQty', 'ShortQty', 'NetQty', 'LongAmt', 'ShortAmt', 'NetAmt']:
                            acct_exposure_dict[key] += dict_position[key]

            if not (prdcode in dict_prdcode2exposure):
                prdcode_exposure_dict = acct_exposure_dict.copy()
                del prdcode_exposure_dict['AcctIDByMXZ']
                dict_prdcode2exposure[prdcode] = prdcode_exposure_dict
                if accttype != 'f':
                    dict_prdcode2exposure[prdcode]['StkLongAmt'] = acct_exposure_dict['LongAmt']
                    dict_prdcode2exposure[prdcode]['StkShortAmt'] = acct_exposure_dict['ShortAmt']
                    dict_prdcode2exposure[prdcode]['StkNetAmt'] = acct_exposure_dict['NetAmt']
                else:
                    dict_prdcode2exposure[prdcode]['StkLongAmt'] = 0
                    dict_prdcode2exposure[prdcode]['StkShortAmt'] = 0
                    dict_prdcode2exposure[prdcode]['StkNetAmt'] = 0

            elif dict_prdcode2exposure[prdcode]['LongQty'] is None:
                pass
            else:
                # 4舍5入保留两位小数， todo 在flask展示里而不是在数据库里保留2位
                for key in ['LongQty', 'ShortQty', 'NetQty', 'LongAmt', 'ShortAmt', 'NetAmt']:
                    dict_prdcode2exposure[prdcode][key] += acct_exposure_dict[key]
                    acct_exposure_dict[key] = round(acct_exposure_dict[key], 2)
                if accttype != 'f':
                    dict_prdcode2exposure[prdcode]['StkLongAmt'] += acct_exposure_dict['LongAmt']
                    dict_prdcode2exposure[prdcode]['StkShortAmt'] += acct_exposure_dict['ShortAmt']
                    dict_prdcode2exposure[prdcode]['StkNetAmt'] += acct_exposure_dict['NetAmt']
            list_dict_acct_exposure.append(acct_exposure_dict)

        for prdcode in dict_prdcode2exposure:
            for key in ['LongQty', 'ShortQty', 'NetQty', 'LongAmt', 'ShortAmt', 'NetAmt']:
                if dict_prdcode2exposure[prdcode][key]:
                    dict_prdcode2exposure[prdcode][key] = round(dict_prdcode2exposure[prdcode][key], 2)

        list_dict_prdcode_exposure = list(dict_prdcode2exposure.values())  # todo ? 这是啥

        if list_dict_acct_exposure:
            self.gl.db_trade_data['trade_exposure_by_acctid'].delete_many({'DataDate': self.gl.str_today})
            self.gl.db_trade_data['trade_exposure_by_acctid'].insert_many(list_dict_acct_exposure)
        if list_dict_prdcode_exposure:
            self.gl.db_trade_data['trade_exposure_by_prdcode'].delete_many({'DataDate': self.gl.str_today})
            self.gl.db_trade_data['trade_exposure_by_prdcode'].insert_many(list_dict_prdcode_exposure)

    def get_col_bs(self):
        dict_acctidbymxz2list_dicts_trade_position = {}
        for dict_trade_position in self.gl.db_trade_data['trade_position'].find(
                {'DataDate': self.gl.str_today}, {'_id': 0}
        ):
            acctidbymxz = dict_trade_position['AcctIDByMXZ']
            if acctidbymxz in dict_trade_position:
                dict_acctidbymxz2list_dicts_trade_position[acctidbymxz].append(dict_trade_position)
            else:
                dict_acctidbymxz2list_dicts_trade_position[acctidbymxz] = [dict_trade_position]

        # todo 需要向此处添加期货户的bs.
        dict_acctidbymxz2dict_fund = {}
        for dict_trade_formatted_fund in (
                self.gl.db_trade_data['trade_formatted_data_fund'].find({'DataDate': self.gl.str_today}, {'_id': 0})
        ):
            dict_acctidbymxz2dict_fund.update({dict_trade_formatted_fund['AcctIDByMXZ']: dict_trade_formatted_fund})

        list_dicts_bs_by_acct = []
        dict_prdcode2dict_bs_by_acct = {}

        for dict_acctinfo in self.gl.col_acctinfo.find(
                {'DataDate': self.gl.str_today, 'DataDownloadMark': 1}, {'_id': 0}
        ):
            acctidbymxz = dict_acctinfo['AcctIDByMXZ']
            prdcode = dict_acctinfo['PrdCode']
            mdm = dict_acctinfo['MonitorDisplayMark']
            if acctidbymxz in dict_acctidbymxz2dict_fund:
                ttasset = dict_acctidbymxz2dict_fund[acctidbymxz]['TotalAsset']
                netasset = dict_acctidbymxz2dict_fund[acctidbymxz]['NetAsset']
                avlfund = dict_acctidbymxz2dict_fund[acctidbymxz]['AvailableFund']
                cash = dict_acctidbymxz2dict_fund[acctidbymxz]['Cash']
                if 'KQZJ' in dict_acctidbymxz2dict_fund[acctidbymxz]:
                    kqzj = dict_acctidbymxz2dict_fund[acctidbymxz]['KQZJ']
                else:
                    kqzj = ''

            else:
                continue

            flag_na = netasset is None
            flag_cash = cash is None   # m户: avlfund可能为 None; c户 avlfund也是None
            flag_ta = ttasset is None
            assert(not(flag_na and flag_ta))  # 净资产总资产必有一个
            if flag_ta:
                ttasset = netasset
            if flag_na:
                netasset = ttasset
            if flag_cash:
                if flag_ta:
                    cash = netasset
                else:
                    cash = ttasset

            if acctidbymxz in dict_acctidbymxz2list_dicts_trade_position:
                for dict_position in dict_acctidbymxz2list_dicts_trade_position[acctidbymxz]:
                    if dict_position['SecurityType'] != 'IrrelevantItem':
                        if flag_ta:
                            ttasset += dict_position['ShortAmt']  # 总资产 = 净资产+ 总负债（约为short）
                        if flag_na:
                            netasset -= dict_position['ShortAmt']
                        if flag_cash:
                            if flag_na:
                                cash -= dict_position['NetAmt']   # 总资产 - 总市值 （约为NetAmt)
                            if flag_ta:
                                cash -= dict_position['LongAmt']  # cash = 净资产 - （总市值 - 总负债） （约为LongAmt）

            if flag_cash and '_c_' in acctidbymxz:
                avlfund = cash

            dict_bs_by_acct = {
                'AcctIDByMXZ': acctidbymxz, 'PrdCode': prdcode, 'MonitorDisplayMark': mdm,
                'DataDate': self.gl.str_today, 'AvailableFund': avlfund, 'Cash': cash, 'KQZJ': kqzj,
                'NetAsset': netasset
            }
            list_dicts_bs_by_acct.append(dict_bs_by_acct)

            if prdcode in dict_prdcode2dict_bs_by_acct:
                for key in ['Cash', 'NetAsset', 'AvailableFund']:
                    dict_prdcode2dict_bs_by_acct[prdcode][key] += dict_bs_by_acct[key]
                    dict_bs_by_acct[key] = round(dict_bs_by_acct[key], 2)
            else:
                dict_bs_by_acct_copy = dict_bs_by_acct.copy()
                del dict_bs_by_acct_copy['AcctIDByMXZ']
                dict_prdcode2dict_bs_by_acct[prdcode] = dict_bs_by_acct_copy

        for prdcode in dict_prdcode2dict_bs_by_acct:
            for key in ['Cash', 'NetAsset', 'KQZJ', 'AvailableFund']:
                if dict_prdcode2dict_bs_by_acct[prdcode][key]:
                    dict_prdcode2dict_bs_by_acct[prdcode][key] = round(dict_prdcode2dict_bs_by_acct[prdcode][key], 2)

        list_dict_prdcode_bs = list(dict_prdcode2dict_bs_by_acct.values())

        if list_dicts_bs_by_acct:
            self.gl.col_trade_bs_by_acct.delete_many({'DataDate': self.gl.str_today})
            self.gl.col_trade_bs_by_acct.insert_many(list_dicts_bs_by_acct)
            self.gl.col_trade_bs_by_prdcode.delete_many({'DataDate': self.gl.str_today})
            self.gl.col_trade_bs_by_prdcode.insert_many(list_dict_prdcode_bs)
        print('Exposure analysis finished.')

    def run(self):
        while True:
            self.update_trdraw_cmo()
            if '083000' < datetime.now().strftime('%H%M%S') < '151500':
                self.update_trdraw_f()
            self.update_trdfmt_cmfo()
            self.update_position()
            self.get_col_bs()
            self.exposure_analysis()
            print('Next round.')

        # t1_trdraw_cmo = Thread(target=self.update_trdraw_cmo)
        # t1_trdraw_cmo.start()
        # if '083000' < datetime.now().strftime('%H%M%S') < '151500':
        #     t2_trdraw_f = Thread(target=self.update_trdraw_f)
        #     t2_trdraw_f.start()
        # t3_trdfmt_cmfo = Thread(target=self.update_trdfmt_cmfo)
        # t3_trdfmt_cmfo.start()
        # t4_update_position = Thread(target=self.update_position)
        # t4_update_position.start()
        # t5_exposure_analysis = Thread(target=self.exposure_analysis)
        # t5_exposure_analysis.start()


if __name__ == '__main__':
    task = GenerateExposureMonitorData()
    task.run()
