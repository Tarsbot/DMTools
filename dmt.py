import os
import sys
import configparser

import git
import psycopg2

con = None
temp = "/tmp/output.md"
config_file_path = "config.ini.sample"


def write_head(path):
    f = open(path, "w")
    f.write("postgresql数据结构\n"
            "========\n")
    f.close()


def write_extra():
    # TODO 列举枚举类型
    f.write("> 1. `request_type`合法请求类型:`'move', 'adjust', 'fin', 'ptz', 'mapping', 'charge', 'turn'`\n\n"
            "> 2. `request_status`合法请求状态:`'pending', 'processing', 'finished'`\n\n"
            "状态记录在`redis`数据库中缓存20分钟\n\n"
            "key: `request:[id]:status`\n\n")


try:
    write_head(temp)
    cf = configparser.ConfigParser()
    cf.read(config_file_path)
    path = cf['git']['git_dir'] + cf['git']['git_file']
    con = psycopg2.connect(host=cf['db']['host'],
                           port=cf['db']['port'],
                           database=cf['db']['database'],
                           user=cf['db']['user'],
                           password=cf['db']['password'])

    cur = con.cursor()
    # cur.execute("select * from information_schema.columns
    # where table_schema='rabbit'
    # order by table_name,ordinal_position")
    # 获得所有拥有主键的表
    cur.execute("SELECT table_name "
                "FROM information_schema.table_constraints "
                "WHERE table_schema='%s' "
                "and constraint_type = 'PRIMARY KEY'"
                "order by table_name;" % cf['db']['schema'])
    rows = cur.fetchall()
    # 显示所有拥有主键的表
    # print(rows)
    # 追加写入方式，首行先插入一个空行
    f = open(temp, "a")
    f.write("\n")
    for row in rows:
        # 取得table的comment信息
        cur.execute("SELECT obj_description('%s'::regclass) "
                    "FROM pg_class "
                    "WHERE relkind = 'r';" % row[0])
        rows = cur.fetchall()
        # 显示table的comment信息+table_name
        # print('* **' + rows[0][0] + '** `%s`' % row[0])
        f.write('* **' + rows[0][0] + '** `%s`\n\n' % row[0])
        if row[0] == "request_detail":
            write_extra()
        elif row[0][-3:] == "_cd":
            # 处理_cd类table详情
            cur.execute("select * from %s" % row[0])
            rows = cur.fetchall()
            f.write('```\n')
            for list in rows:
                for item in list:
                    f.write('%s\t' % item)
                f.write('\n')
            f.write('```\n\n')
        f.write('|序号|字段|中文名|类型|是否主键|非空|\n|:-:|:-:|:-:|:-:|:-:|:-:|\n')
        # 取得table基本信息
        cur.execute("SELECT DISTINCT "
                    "a.attnum as num, "
                    "a.attname as name, "
                    "com.description as comment, "
                    "format_type(a.atttypid, a.atttypmod) as typ, "
                    "coalesce(p.indisprimary,false) as primary_key, "
                    "a.attnotnull as notnull "
                    "FROM pg_attribute a "
                    "JOIN pg_class pgc ON pgc.oid = a.attrelid "
                    "LEFT JOIN pg_index p ON "
                    "p.indrelid = a.attrelid AND a.attnum = ANY(p.indkey) "
                    "LEFT JOIN pg_description com on "
                    "(pgc.oid = com.objoid AND a.attnum = com.objsubid) "
                    "LEFT JOIN pg_attrdef def ON "
                    "(a.attrelid = def.adrelid AND a.attnum = def.adnum) "
                    "WHERE a.attnum > 0 AND pgc.oid = a.attrelid "
                    "AND pg_table_is_visible(pgc.oid) "
                    "AND NOT a.attisdropped "
                    "AND pgc.relname = '%s' "
                    "ORDER BY a.attnum;" % row[0])
        rows = cur.fetchall()
        for item in rows:
            # 逐行显示table中的columns信息
            # print(item)
            f.write('|%s|%s|%s|%s|%s|%s|\n' % item)
        f.write('\n')
    f.close()
    with open(temp, 'r') as infile, \
            open(path, 'w') as outfile:
        data = infile.read()
        # 删除所有False，带上|是防止误杀
        data = data.replace("|False", "|")
        # 把True替换成对勾，似乎看上去更和谐
        data = data.replace("True|", "√|")
        outfile.write(data)
    # 删除临时文件
    os.remove(temp)
    # 更新wiki页面信息
    g = git.cmd.Git(cf['git']['git_dir'])
    g.add('.')
    g.commit('-m', 'update datamodel info by DMTools')
    g.push()

except psycopg2.DatabaseError as e:
    print('Error %s' % e)
    sys.exit(1)

except git.GitCommandError as e:
    # 如果没有更新任何内容，也会抛出错误
    print('Error %s' % e)
    sys.exit(1)

except IOError as e:
    print('Error %s' % e)
    sys.exit(1)

finally:
    if con:
        con.close()
