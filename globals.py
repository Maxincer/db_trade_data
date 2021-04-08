#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20201031T180000

"""
This script provides global variables, constants, functions in this project
"""

from datetime import datetime

from pymongo import MongoClient

from WindPy import w

STR_TODAY = datetime.today().strftime('%Y%m%d')


class Globals:
    def __init__(self, str_today=STR_TODAY):
        # 日期部分
        self.str_today = str_today
        self.dt_today = datetime.strptime(self.str_today, '%Y%m%d')
        self.server_mongodb = MongoClient(
            'mongodb://192.168.2.162:27017/', username='Maxincer', password='winnerismazhe'
        )
        self.db_global = self.server_mongodb['global']
        self.col_trdcalendar = self.db_global['trade_calendar']

        self.list_str_trdcalendar = []
        for _ in self.col_trdcalendar.find():
            self.list_str_trdcalendar += _['Data']
        idx_str_today = self.list_str_trdcalendar.index(self.str_today)
        self.str_last_trddate = self.list_str_trdcalendar[idx_str_today - 1]
        self.str_next_trddate = self.list_str_trdcalendar[idx_str_today + 1]
        self.str_last_last_trddate = self.list_str_trdcalendar[idx_str_today - 2]
        self.str_next_next_trddate = self.list_str_trdcalendar[idx_str_today + 2]
        self.dt_last_trddate = datetime.strptime(self.str_last_trddate, '%Y%m%d')

        # 配置文件部分: basicinfo
        self.db_basicinfo = self.server_mongodb['basicinfo']
        self.col_acctinfo = self.db_basicinfo['acctinfo']
        self.col_prdinfo = self.db_basicinfo['prdinfo']
        self.col_strategy_info = self.db_basicinfo['strategy_info']
        self.col_datapatch = self.db_basicinfo['data_patch']

        # trade
        self.db_trade_data = self.server_mongodb['trade_data']
        self.col_trade_glv = self.db_trade_data['exposure_monitor_glv']
        self.col_trade_rawdata_fund = self.db_trade_data['trade_raw_data_fund']
        self.col_trade_rawdata_holding = self.db_trade_data['trade_raw_data_holding']
        self.col_trade_rawdata_order = self.db_trade_data['trade_raw_data_order']
        self.col_trade_rawdata_short_position = self.db_trade_data['trade_raw_data_short_position']

        self.col_trade_fmtdata_fund = self.db_trade_data['trade_formatted_data_fund']
        self.col_trade_fmtdata_holding = self.db_trade_data['trade_formatted_data_holding']
        self.col_trade_fmtdata_order = self.db_trade_data['trade_formatted_data_order']
        self.col_trade_fmtdata_short_position = self.db_trade_data['trade_formatted_data_short_position']

        self.db_post_trade_data = self.server_mongodb['post_trade_data']

        # 其他
        self.dict_exchange2secidsrc = {'深A': 'SZSE', '沪A': 'SSE'}

        self.list_data_source_types_xtqmt_jjb = ['dh_xtqmt_jjb',  'hr_xtqmt_jjb', 'gd_xtqmt_jjb', 'zhes_xtqmt_jjb']
        self.list_data_src_xtpb = [
            'zxjt_xtpb', 'zhaos_xtpb', 'zhes_xtpb', 'hf_xtpb', 'gl_xtpb',
            'swhy_xtpb', 'cj_xtpb', 'hengt_xtpb', 'zygj_xtpb', 'gd_xtpb', 'tpy_xtpb'
        ]
        self.list_data_src_htpb = ['gy_htpb', 'gs_htpb', 'gj_htpb', 'sh_htpb']

    @classmethod
    def get_secid2windcode(cls, str_secid):
        # 将沪深交易所标的代码转为windcode
        if len(str_secid) == 6:
            if str_secid[0] in ['0', '3']:
                __windcode = str_secid + '.SZ'
                return __windcode
            elif str_secid[0] in ['6']:
                __windcode = str_secid + '.SH'
                return __windcode
            elif str_secid[0] in ['5']:
                __windcode = str_secid + '.SH'
                return __windcode
            else:
                raise ValueError('Wrong secid first letter: Cannot get windcode suffix according to SecurityID.')
        else:
            raise ValueError('The length of the security ID is not 6.')

    @staticmethod
    def get_list_str_trddate(str_startdate, str_enddate):
        """
        获得期间自然日期间的交易日，参数为闭区间两端
        :param str_startdate: 起始日期，包括
        :param str_enddate: 终止日期，包括
        :return: list, 指定期间内交易日的列表
        """
        wtdays = w.tdays(str_startdate, str_enddate)
        list_str_trddates = [x.strftime('%Y%m%d') for x in wtdays.Data[0]]
        return list_str_trddates

    @classmethod
    def get_mingshi_sectype_from_code(cls, str_code):
        """
        实际使用的函数
        :param str_code: SecurityID.SecurityIDSource
            1. SecuritySource:
                1. SSE: 上交所
                2. SZSE: 深交所
                3. ITN: internal id
        :return:
            1. CE, Cash Equivalent, 货基，质押式国债逆回购
            2. CS, Common Stock, 普通股
            3. ETF, ETF, 注意：货币类ETF归类为CE，而不是ETF
            4. SWAP, swap
        """

        list_split_wcode = str_code.split('.')
        secid = list_split_wcode[0]
        exchange = list_split_wcode[1]
        if exchange in ['SH', 'SSE'] and len(secid) == 6:
            if secid in ['511990', '511830', '511880', '511850', '511660', '511810', '511690']:
                return 'CE'
            elif secid in ['204001']:
                return 'CE'
            elif secid[:3] in ['600', '601', '603', '688']:
                return 'CS'
            elif secid in ['510500', '000905', '512500']:
                return 'ETF'
            else:
                return 'IrrelevantItem'

        elif exchange in ['SZ', 'SZSE'] and len(secid) == 6:
            if secid[:3] in ['000', '001', '002', '003', '004', '300', '301', '302', '303', '304', '305', '306', '307',
                             '308', '309']:
                return 'CS'
            elif secid[:3] in ['115', '120', '121', '122', '123', '124', '125', '126', '127', '128', '129']:
                return '可转债'
            elif secid[:3] in ['131']:
                return 'CE'
            elif secid in ['159001', '159005', '159003']:
                return 'CE'
            else:
                return 'IrrelevantItem'
        elif exchange in ['CFE', 'CFFEX']:
            return 'Index Future'

        elif exchange == 'ITN':
            sectype = secid.split('_')[0]
            return sectype

        else:
            raise ValueError(f'{str_code} has unknown exchange or digit number is not 6.')

