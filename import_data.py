# -*- coding: utf-8 -*-

import argparse
import sys

IS_PY2 = sys.version_info.major == 2


def connect_db(host, port, user, passwd):
    import pymysql
    try:
        wconn = pymysql.connect(
            host=host,
            user=user,
            password=passwd,
            port=port,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        rconn = pymysql.connect(
            host=host,
            user=user,
            password=passwd,
            port=port,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.SSDictCursor)
        return wconn, rconn
    except Exception as e:
        sys.stderr.write(u'连接数据库失败：%s\n' % (e.args, ))
        sys.exit(1)


def enable_db_foreign_key_check(dbconn, st):
    with dbconn.cursor() as csr:
        csr.execute('SET FOREIGN_KEY_CHECKS=%d' % st)
    dbconn.commit()


def prepare_db_conn(dbconn):
    # 禁止外键检测
    enable_db_foreign_key_check(dbconn, 0)


def clear_db_conn(dbconn):
    # 恢复外键检测
    enable_db_foreign_key_check(dbconn, 1)

    dbconn.close()


def load_conf(path):
    import os, yaml

    abspath = os.path.abspath(path)
    try:
        module_path = os.path.dirname(abspath)
        sys.path.insert(0, module_path)
        return yaml.load(open(abspath, 'r'))
    except Exception as e:
        sys.stderr.write(u'加载配置文件 %s 失败：%s\n' % (abspath, e.args))
        sys.exit(2)


def make_src_sql(keys, dbname, where):
    return 'SELECT `%s` FROM %s WHERE %s' % ('`,`'.join(keys), dbname, where)


def make_dst_sql(dstdata, tbname, update_by):
    if update_by is None:
        return 'INSERT INTO {tablename} (`{columns}`) VALUES ({values})'.format(
            tablename=tbname,
            columns='`,`'.join(dstdata.keys()),
            values=','.join(('%s', ) * len(dstdata)))

    # update 操作
    uid = dstdata.pop(update_by)
    if not dstdata:
        return None

    return 'UPDATE {tablename} SET {columns} WHERE `{uid_key}` = "{uid}"'.format(
        tablename=tbname,
        columns=', '.join('`{}`=%s'.format(k) for k in dstdata.keys()),
        uid_key=update_by,
        uid=uid)


def make_progress(dbconn, dbname, where, limit):
    from tqdm import tqdm
    if limit > 0:
        return tqdm(total=limit)

    with dbconn.cursor() as csr:
        csr.execute('SELECT COUNT(*) FROM %s WHERE %s' % (dbname, where))
        res = tuple(csr.fetchone().values())[0]
    return tqdm(total=res)


def convert(wdbconn, rdbconn, dbsrc, dbdst, conf):
    limit = max(conf.get('limit', 0), 0)
    tbfrom, tbto = dbsrc + '.' + conf['from'], dbdst + '.' + conf['to']
    where = conf.get('where', '1')
    update_by = conf.get('update_by')

    from convertor import Convertor
    c = Convertor(conf['map'], conf.get('const'), wdbconn)
    progress = make_progress(wdbconn, tbfrom, where, limit)

    rcsr = rdbconn.cursor()
    srcsql = make_src_sql(c.keys, tbfrom, where)
    rcsr.execute(srcsql)

    # 获取数据表
    offset, step = 0, 200
    while limit == 0 or offset < limit:
        with wdbconn.cursor() as wcsr:
            srcdata = rcsr.fetchmany(step)

            # 因为每条 sql 可能都不一样，所以不能使用 executemany
            for sd in srcdata:
                dstdata = c.process(sd)
                dstsql = make_dst_sql(dstdata, tbto, update_by)
                # print(dstsql)
                if dstsql:
                    wcsr.execute(dstsql, tuple(dstdata.values()))

        wdbconn.commit()
        progress.update(step)
        if len(srcdata) < step:
            break
        offset += step

    rcsr.close()
    progress.close()


def make_check_rand_sql(dbname, where):
    return 'SELECT * FROM %s AS t1 JOIN (SELECT ROUND(RAND() * ((SELECT MAX(id) FROM %s)-(SELECT MIN(id) FROM %s))+(SELECT MIN(id) FROM %s)) AS id) AS t2 WHERE t1.id >= t2.id AND %s ORDER BY t1.id LIMIT 1' % (
        dbname, dbname, dbname, dbname, where)


def make_check_src_sql(keys, dbname, where, dst_data, check_cond):
    checks = []
    for k in check_cond:
        checks.append("`%s` = '%s'" % (check_cond[k], dst_data[k]))
    checks.append(where)

    return 'SELECT `%s` FROM %s WHERE %s' % ('`,`'.join(keys), dbname,
                                             ' AND '.join(checks))


def strit(o):
    if IS_PY2 and isinstance(o, unicode):
        return o.encode('utf8')
    return str(o)


def compare(src_dict, dst_dict):
    for k in src_dict:
        if (k not in dst_dict) or (strit(src_dict[k]) != strit(dst_dict[k])):
            return False
    return True


def check(wdbconn, dbsrc, dbdst, conf, count):
    '''
    检测转换后的数据的正确性
    '''
    check_cond = conf.get('check')
    if check_cond is None:
        sys.stderr.write(u'无法校验数据，需要在配置文件中指定 check 字段')
        return

    tbfrom, tbto = dbsrc + '.' + conf['from'], dbdst + '.' + conf['to']
    where = conf.get('where', '1')
    update_by = conf.get('update_by')

    from convertor import Convertor
    c = Convertor(conf['map'], conf.get('const'), wdbconn)
    progress = make_progress(wdbconn, tbfrom, where, count)

    check_where = conf.get('check_where', '1')
    check_src_use_where = conf.get('check_src_use_where', False)
    if not check_src_use_where:
        where = '1'
    rand_sql = make_check_rand_sql(tbto, check_where)
    limit = 0
    ncomm = 0
    while limit < count:
        # 从目标数据随机取出一条数据
        with wdbconn.cursor() as wcsr:
            wcsr.execute(rand_sql)
            dst_data = wcsr.fetchone()
            if dst_data is None:
                sys.stderr.write(u'数据不足，无法检测')
                break
            # 获取对应的源数据
            src_sql = make_check_src_sql(c.keys, tbfrom, where, dst_data,
                                         check_cond)
            wcsr.execute(src_sql)
            src_data = wcsr.fetchall()

            for sd in src_data:
                src_dst_data = c.process(sd)
                # 比较双方的数据
                if compare(src_dst_data, dst_data):
                    ncomm += 1
                    break
        progress.update(1)
        limit += 1

    progress.close()

    ndiff = count - ncomm
    print(u'''随机检测条数 %d
相同 %d
不同 %d
正确率 %.2f
错误率 %.2f''' % (count, ncomm, ndiff, ncomm * 100.0 / count,
               ndiff * 100.0 / count))


def parse_args():
    parser = argparse.ArgumentParser(description=u'数据库迁移工具')
    parser.add_argument('-u', '--user', help=u'连接数据库的用户', default='eye')
    parser.add_argument('-p', '--passwd', help=u'连接数据库的密码', default='sauron')
    parser.add_argument(
        '-P', '--port', help=u'设置连接数据库的端口', default=3306, type=int)
    parser.add_argument(
        '-H', '--host', help=u'设置连接数据库的地址', default='127.0.0.1')
    parser.add_argument('-c', '--config', help=u'指定转换数据的配置路径', required=True)
    parser.add_argument(
        '-C', '--check', help=u'指定转换数据的配置路径', type=int, default=0)
    parser.add_argument('-s', '--src', help=u'指定转换源数据库', required=True)
    parser.add_argument('-d', '--dst', help=u'指定转换的目标数据库', required=True)

    return parser.parse_args()


def main():
    args = parse_args()

    # 载入配置
    conf = load_conf(args.config)

    # 连接数据库
    wconn, rconn = connect_db(args.host, args.port, args.user, args.passwd)
    prepare_db_conn(wconn)

    if args.check > 0:
        check(wconn, args.src, args.dst, conf, args.check)
    else:
        convert(wconn, rconn, args.src, args.dst, conf)

    clear_db_conn(wconn)
    rconn.close()


if __name__ == "__main__":
    main()
