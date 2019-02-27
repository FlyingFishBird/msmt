# -*- coding: utf-8 -*-
from functools import partial
import json
import sys
from datetime import datetime

_ISPY2 = sys.version_info.major == 2


def _encode(data):
    if _ISPY2 and isinstance(data, unicode):
        return data.encode('utf-8')
    return data


def _convert(src_parser, dst_parser, data):
    return dst_parser(src_parser(data))


def _raw(x):
    return x


def _date(defval, t):
    """
    支持的格式:
        unix 时间戳
        yyyy-mm-dd 格式的日期字符串
        yyyy/mm/dd 格式的日期字符串
        yyyymmdd 格式的日期字符串

        如果年月日其中有一项是0，将被转换成 1
    """
    if t is None:
        return defval

    if isinstance(t, (int, float)):
        return datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')

    lt = len(t)
    if lt < 8:
        return defval

    if lt == 8:
        format_str = '%Y%m%d'
    else:
        t = t.replace('/', '-')
        format_str = '%Y-%m-%d %H:%M:%S'
        if lt > 19:
            format_str += '.%f'
    try:
        return str(datetime.strptime(t, format_str))
    except:
        return defval


def _custom_date(defval, t):
    """
    支持的格式:
        unix 时间戳
        yyyy-mm-dd 格式的日期字符串
        yyyy/mm/dd 格式的日期字符串
        yyyymmdd 格式的日期字符串

        其中mm dd 可以不确定。指定成 00 00 那么将使用
    """
    if t is None:
        return defval

    if isinstance(t, (int, float)):
        return datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')

    lt = len(t)
    if lt < 8:
        return defval

    if lt == 8:
        return '%s-%s-%s 00:00:00' % (t[:4], t[4:6], t[6:])
    return t.replace('/', '-')


def _daten(t):
    return _date(datetime.now(), t)


def _custom_daten(t):
    return _custom_date(datetime.now(), t)


_VAL_PARSER = {
    'json': json.loads,
    'date': partial(_date, None),
    'cdate': partial(_custom_date, None),
    'daten': _daten,
    'cdaten': _custom_daten,
    'int': int,
    'str': str,
    'float': float,
}


def _get_db_key_parser(dbconn, table_name, tid):
    def op(fid):
        sql = 'select * from %s where `%s` = "%s" limit 1' % (table_name, tid,
                                                              fid)
        with dbconn.cursor() as csr:
            csr.execute(sql)
            return csr.fetchone() or {}

    return op


def _get_key_parser(key, dbconn=None):
    name = key.split('+')
    lname = len(name)

    if lname == 2:
        return name[0], _VAL_PARSER.get(name[1], _raw)
    # 数据库解释器
    if dbconn is not None and lname > 3 and name[1] == 'db':
        return name[0], _get_db_key_parser(dbconn, *name[2:])

    return key, _raw


def _get_dst_list_parser(val, dbconn):
    c = []

    def dict_impl(src, dst, vmap, data):
        srcdata = data.get(src, None)
        if srcdata is None:
            return None

        sk, op = _get_key_parser(dst, dbconn)
        if vmap:
            vdef = vmap['def']
            return (sk, op(_encode(vmap.get(srcdata, vdef))))
        else:
            return (sk, op(_encode(srcdata)))

    def str_impl(v, data):
        sk, op = _get_key_parser(v, dbconn)
        return (sk, op(_encode(data)))

    # 生成解析器
    for v in val:
        if isinstance(v, dict):
            src, dst = v['src'], v['dst']
            vmap = v.get('map', None)
            c.append(partial(dict_impl, src, dst, vmap))
        else:
            # v 是字符串的形式，表示多映射
            c.append(partial(str_impl, v))

    def op(data):
        res = []
        for cc in c:
            v = cc(data)
            if v is not None:
                res.append(v)
        return res

    return op


def _get_dst_str_parser(val, dbconn):
    sk, op = _get_key_parser(val, dbconn)
    return lambda data: ((sk, op(_encode(data))), )


def _get_dst_parser(val, dbconn=None):
    if isinstance(val, str):
        return _get_dst_str_parser(val, dbconn)
    elif isinstance(val, dict):
        dst = val['dst']
        vmap = val.get('map')
        if vmap:
            defv = vmap['def']
            return lambda data: ((dst, _encode(vmap.get(data, defv))), )
        else:
            op = val['py']
            return lambda data: ((dst, _encode(op(data))), )
    elif isinstance(val, list):
        return _get_dst_list_parser(val, dbconn)

    return None


class Convertor(object):
    def __init__(self, map_data, const_data, dbconn):
        self._c = {}
        self._consts = None

        for k in map_data.keys():
            v = map_data[k]

            # 获取数据源解析器
            sk, src_parser = _get_key_parser(k, dbconn)
            # 获取目标数据解析器
            dst_parser = _get_dst_parser(v, dbconn)
            self._c[sk] = partial(_convert, src_parser, dst_parser)

        # 处理固定字段
        if isinstance(const_data, dict):
            self._consts = const_data

    @property
    def keys(self):
        return tuple(self._c.keys())

    def process(self, key_data):
        res = {}
        for k in key_data:
            # 获取数据转换器
            convert = self._c.get(k, None)
            if convert is None:
                continue
            # 获取数据
            kres = convert(key_data[k])
            res.update(kres)
        if self._consts:
            res.update(self._consts)
        return res
