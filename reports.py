import openpyxl
import requests
from bs4 import BeautifulSoup as BS
import os 
import pandas as pd
from progressbar import ProgressBar as Bar

import json
import time
import random
import re

proxy = {
    "http": "http://127.0.0.1:12639",
    "https": "https://127.0.0.1:12639",
}

proxy = None

headers={
    "Host": "www.chinanpo.gov.cn",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
    "Origin": "http://www.chinanpo.gov.cn",
    "Content-Type": "application/x-www-form-urlencoded"
}

SESSIONID = "5F479882D0CB076A3E3AECC98EBD038E"
SESSIONID = "ED9A06D8C865114D077D964931DCA7D8"

def sleep(s=1,e=2):
    return 
    s = random.randint(s,e)
    print("sleep for %d seconds" % s)
    time.sleep(s)

def shenzhenReportLinks():
    ofile = "declare_ids.csv"

    # 42页到940页
    url = "http://218.17.83.146:9008/SOCSP_PS_SP/api/info/njnb"
    dfs = []
    for page in (1,2):
        params = dict(pageNum=page, pageSize=10000, size=10000)
        r = requests.post(url, params, proxies=proxy, headers= headers)

        data = json.loads(r.text)
        if data["msg"] != "成功":
            raise ValueError(data["msg"])

        dfs.append(pd.DataFrame(data["data"]["list"]))

    df = pd.concat(dfs)
    df.to_csv(ofile,index=False)

def shenzhenReports():
    ofile = "declare_ids_2018.csv"
    agents = pd.read_csv(ofile)
    agents = agents[agents.SOCIETY_NAME.map(lambda r: isinstance(r,str) and "基金会" in r)]

    path = "funds"

    names = set()
    for file in os.listdir(path):
        if os.path.isdir(os.path.join(path,file)):
            continue
        groups = re.search("(.*)-(.*).html", file)
        if groups is None:
            continue
        name = groups.group(1)
        names.add(name)

    #url = "http://218.17.83.146:9009/SOCSP_O/publicnjnb/printAll?declareId="
    url = "http://218.17.83.146:9009"

    failed = 0

    sites = [
        #("基本信息","/SOCSP_O/njnbJjh/toJbxx\?.*print=1&dy=1"),
        #("机构建设一（理事会）","/SOCSP_O/njnbJjh/toLshcy\?.*print=1&dy=1"),
        #("机构建设一（监事会）","/SOCSP_O/njnbJjh/tojshcy\?.*print=1&dy=1"),
        #("机构建设二","/SOCSP_O/njnbJjh/tojg2\?.*print=1&dy=1"),
        #("机构建设三","/SOCSP_O/njnbJjh/toJg3\?.*print=1&dy=1"),
        #("机构建设四","/SOCSP_O/publicnjnb/toDjqk\?.*print=1&dy=1"),
        #("机构建设六","/SOCSP_O/njnbJjh/toJjhnbJgjs6\?.*print=1&dy=1"),
        ("业务活动一","/SOCSP_O/njnbJjh/toYwhdqk1\?.*print=1&dy=1"),
        #("业务活动五","/SOCSP_O/njnbJjh/toYwhd5Form\?.*print=1&dy=1"),
        #("资产负债","/SOCSP_O/publicnjnb/topublicZcfz\?.*print=1&dy=1"),
        #("业务活动","/SOCSP_O/publicnjnb/topublicYwhd\?.*print=1&dy=1"),
    ]

    for i,(name,did) in enumerate(zip(agents.SOCIETY_NAME,agents.DECLAREID)):
        
        if isinstance(name,str)==False or isinstance(did,str)==False or name is None or did is None or not name.strip() or not did.strip() or name.strip() in names:
            continue
        if "基金会" not in name:
            continue

        name, did = name.strip(), did.strip()

        print("Loading <{}/{}>: {} {}".format(i+1,agents.shape[0], name, did))
        
        contents = []
        try:
            #url_ = url + postfix +  did
            url_ = url + "/SOCSP_O/publicnjnb/printAll?declareId=" + did

            r = requests.get(url_, proxies = proxy, headers = headers, cookies={"JSESSIONID": SESSIONID})
            if r.status_code != 200:
                continue

            content = r.text
            for sname, spattern in sites:
                g = re.search(spattern, content)
                if g is None:
                    break
                link = url + g.group(0)

                print("=" * 120)
                print("Crawling for <{}:{}>".format(sname, link))

                r = requests.get(link, proxies = proxy, headers = headers, cookies={"JSESSIONID": SESSIONID})
                if r.status_code != 200:
                    break
                
                contents.append((sname, r.text))
                sleep()
        except Exception as ex:
            failed += 1
        finally:
            if len(contents) == len(sites):
                for tname, content in contents:
                    with open("{}/{}-{}.html".format(path,name,tname),"w", encoding="utf8") as writer:
                        writer.write(content)
        # if i>=2:
        #     break

    #driver.quit()
    print("Failed files in total <{}>".format(failed))

def parse_reports():
    path = "reports"
    path = "funds"

    ofile = "declare_ids_2018.csv"
    agents = pd.read_csv(ofile)
    agents = agents[agents.SOCIETY_NAME.map(lambda r: isinstance(r,str) and "基金会" in r)]

    from collections import defaultdict
    
    failed = 0
    tabs = defaultdict(list)

    # def safe_parse(func):
    #     def func_(path, name):
    #         try:
    #             return func(path, name)
    #         except Exception as ex:
    #             failed.add("{}：<{}>".format(name, func.__name__))

    #         return None
    #     return func_

    def load_html(path,name,tname):
        file = "{}/{}-{}.html".format(path,name,tname)
        print("Parsing <{}>".format(file))

        with open(file,"r",encoding="utf8") as reader:
            data = reader.read()
        
        soup = BS(data, "html.parser")
        return soup

    def parse_zcfz(path,name):
        soup = load_html(path,name,"资产负债")
        rows = soup.find("table", {"id": "tablepr"}).findAll("tr")

        fields = [tuple(td.text.strip() for td in r.findAll("td")) for r in rows]
        fields = [line[s:s+4] for line in fields for s in (0,4)]

        maps = {
            "105": "限制性净资产",
            "110": "净资产合计",
            "101": "非限制性净资产",
            "2": "短期投资",
            "21": "长期股权投资",
            "24": "长期债权投资",
            "15": "一年内到期的长期债权投资",
            "60": "资产总计",
        }
        values = {"机构名称": name}
        for _,rid,s,e in fields:
            rid = rid.strip()
            if rid in maps:
                values["{}-年初数".format(maps[rid])] = s
                values["{}-期末数".format(maps[rid])] = e
        return values


    def parse_ywhd(path,name):
        soup = load_html(path,name,"业务活动")
        rows = soup.find("table", {"id": "tablepr"}).findAll("tr")

        fields = [tuple(td.text.strip() for td in r.findAll("td")) for r in rows]
        fields = [v for v in fields if len(v)==8]
        maps = {
            "1": "捐赠收入",
            "2": "会费收入",
            "3": "提供服务收入",
            "4": "商品销售收入",
            "5": "政府补助收入",
            "6": "投资收益",
            "9": "其他收入",
            "11": "收入合计",
            "12": "业务活动成本",
            "14": "提供服务成本",
            "21": "管理费用",
            "24": "筹资费用",
            "28": "其他费用",
            "35": "费用合计",
            "45": "净资产变动额",
        }
        values = {"机构名称": name}
        for row in fields:
            rid = row[1]
            if rid in maps:
                for i,it in [(2,"上年末期"),(5,"本年累计")]:
                    for j,jt in [(0,"非限定"),(1,"限定"),(2,"合计")]:
                        values["{}-{}-{}".format(maps[rid],it,jt)] = row[i+j]
        return values

    def parse_ywhd1(path,name):
        soup = load_html(path,name,"业务活动一")
        table = soup.findAll("table")[-1]
        rows = table.findAll("tr")
        fields = [tuple(td.text.strip() for td in r.findAll("td")) for r in rows]
        #fields = [r for r in fields if len(r)==2]

        values = {"机构名称": name}
        for r in fields:
            n, v = r[0], r[1]
            if n in ('本年度总支出',"本年度用于慈善活动的支出","管理费用"):
                values[n] = v
        return values

    def parse_jgjs4(path,name):
        soup = load_html(path,name,"机构建设四")
        rows = soup.find("table", {"id": "table1"}).findAll("tr")

        fields = [
            ("工作人员中党员数",0,2),
            ("专职人员中党员人数",1,2),
            ("本年度发展新吸收入党积极分子数",2,2),
            ("本年度发展新党员人数",2,4),
            ("党组织类型",4,2),
            ("专职党务工作人员数",6,2),
            ("兼职党务工作人员数",6,4),
            ("是否建立共青团组织",10,2),
            ("是否建立工会组织",11,2),
            ("是否将党建工作写入章程",12,2),
        ]
        values = {"机构名称": name}
        for n,r,c in fields:
            row = rows[r]
            v = row.findAll("td")[c].text
            if v:
                values[n] = v.strip()
            else:
                values[n] = ""

        return values

    def parse_jgjs3(path,name):
        soup = load_html(path,name,"机构建设三")
        rows = soup.findAll("table")[-1].findAll("tr")

        values = {"机构名称": name}
        v = rows[16].findAll("td")[1].text.strip()
        values["人民币开户银行"] = v

        v = rows[22].findAll("td")[2].text.strip()
        values["信息公开"] = v

        return values

    def parse_jbxx1(path,name):
        soup = load_html(path,name,"基本信息")
        rows = soup.find("table", {"id": "tablepr"}).findAll("tr")

        fields = [
            ("理事数",21,3),
            ("监事数",22,1),
            ("负责人数",22,3),
        ]
        values = {"机构名称": name}
        for n,r,c in fields:
            row = rows[r]
            v = row.findAll("td")[c].text
            if v:
                values[n] = v.strip()
            else:
                values[n] = ""
        return values

    def parse_zxjj(path,name):
        soup = load_html(path,name,"机构建设六")
        rows = soup.findAll("table")[0].findAll("tr")
        fields = [tuple(td.text.strip() for td in r.findAll("td")) for r in rows]

        values = {"机构名称": name}
        settings = [
            ("本年度专项基金总数",0,1,4,1),
            ("代表机构总数",2,1,4,1),
            ("内设机构总数",4,1,4,1),
        ]
        for n,r,c1,c2,c3 in settings:
            values["{}".format(n)] = fields[r][c1]
            values["{}-本年度新设".format(n)] = fields[r][c2]
            values["{}-本年度注销".format(n)] = fields[r+1][c3]

        return values

    def parse_jz(path,name):
        soup = load_html(path,name,"业务活动一")
        rows = soup.find("table",{"id": "jsjzqk"}).findAll("tr")
        fields = [tuple(td.text.strip() for td in r.findAll("td")) for r in rows[:8]]

        values = {"机构名称": name}
        settings = [
            ("境内自然人",3),
            ("境内法人",4),
            ("境外自然人",6),
            ("境外法人",7),
        ]
        for prefix, row in settings:
            values["{}-现金".format(prefix)] = fields[row][1]
            values["{}-非现金".format(prefix)] = fields[row][2]
            values["{}-合计".format(prefix)] = fields[row][3]

        return values

    def records_gmzcgl(path,name):
        soup = load_html(path,name,"机构建设六")
        rows = soup.find("table", {"id": "table5"}).findAll("tr")

        fields = [tuple(td.text.strip() for td in r.findAll("td")) for r in rows]

        records = []
        for rid,n,amt,a,b in fields:
            records.append({
                "机构名称": name,
                "序号": rid,
                "购买的资产管理产品名称": n,
                "购买金额": amt,
                "当年实际收益金额": a,
                "当年实际收回金额": b
                })

        return records

    def records_cygqst(path,name):
        soup = load_html(path,name,"机构建设六")
        rows = soup.find("table", {"id": "table3"}).findAll("tr")
        fields = [tuple(td.text.strip() for td in r.findAll("td")) for r in rows]

        if len(fields) == 5:
            return []
        records = []
        record = {"机构名称": name, "序号": 1}
        for i, r in enumerate(fields):
            r = r if len(r)==6 else r[1:]

            if i % 5 == 0 and i > 0:
                records.append(record)
                record = {"机构名称": name, "序号": i + 1}

            record[r[0].text.strip()] = r[1].text.strip()
            record[r[2].text.strip()] = r[3].text.strip()
            record[r[4].text.strip()] = r[5].text.strip()
        records.append(record)
        return records

    def records_jsqk(path,name):
        soup = load_html(path,name,"机构建设一（监事会）")
        rows = soup.findAll("table")[0].findAll("tr")
        fields = [tuple(td.text.strip() for td in r.findAll("td")) for r in rows]

        settings = [
            ("姓名",0,2),
            ("性别",0,4),
            ("学历",0,6),
            ("出生日期",1,3),
            ("出席理事会次数",4,3),
        ]

        records = []
        record = {"机构名称": name, "序号": 1}
        for i, row in enumerate(fields):
            if i % 6 == 0 and i > 0:
                records.append(record)
                record = {"机构名称": name, "序号": i // 6 + 1}
            for n, r, c in settings:
                if i % 6 == r:
                    record[n] = row[c]
        records.append(record)
        return records

    def records_lsqk(path,name):
        soup = load_html(path,name,"机构建设一（理事会）")
        bodies = soup.find("table",{"id":"tablepr"}).findAll("tbody")

        settings = [
            ("姓名",0,2),
            ("性别",0,4),
            ("学历",0,8),
            ("出生日期",1,1),
            ("理事会职务",1,3),
            ("政治面貌",2,1),
            ("任职起始",2,3),
        ]

        records = []
        for i, body in enumerate(bodies):
            rows = body.findAll("tr")
            fields = [tuple(td.text.strip() for td in r.findAll("td")) for r in rows]
            record = {"机构名称": name, "序号": 1}

            if fields[1][3] not in ('理事长','秘书长'):
                continue

            for n,r,c in settings:
                record[n] = re.sub("[\r\n\t ]+",' ',fields[r][c])
            records.append(record)
        return records

    def records_ywhd5(path,name):
        soup = load_html(path,name,"业务活动五")
        rows = soup.findAll("table")[1].findAll("tr")
        fields = [tuple(td.text.strip() for td in r.findAll("td")) for r in rows]
        # print(fields)
        records = []
        for i, row in enumerate(fields[1:]):
            if row[1] == "无":
                continue
            record = {"机构名称": name, "序号": 1+i}
            record["受托机构"] = row[1]
            record["受托人是否有资质在中国境内从事投资管理业务"] = row[2]
            record["委托金额"] = row[3]
            record["委托期限"] = row[4]
            record["收益确定方式"] = row[5]
            record["当年实际收益金额"] = row[6]
            record["当年实际收回金额"] = row[7]
            records.append(record)
        return records
    
    for i,(name,did) in enumerate(zip(agents.SOCIETY_NAME,agents.DECLAREID)):    
        if isinstance(name,str)==False or isinstance(did,str)==False or name is None or did is None or not name.strip() or not did.strip():
            continue
        if "基金会" not in name:
            continue
        print("="*120)
        try:
            # values = {}
            # values.update(parse_jgjs3(path,name))
            # values.update(parse_jbxx1(path,name))            
            # values.update(parse_zcfz(path,name))
            # values.update(parse_ywhd1(path,name))
            # tabs["汇总信息"].append(values)

            # values = {}
            # values.update(parse_ywhd(path,name))
            # tabs["业务活动"].append(values)

            # values = {}
            # values.update(parse_zxjj(path,name))
            # tabs["专项基金"].append(values)

            # values = {}
            # values.update(parse_jgjs4(path,name))
            # tabs["党建"].append(values)

            # values = {}
            # values.update(parse_jgjs3(path,name))
            # tabs["开户银行"].append(values)

            values = {}
            values.update(parse_jz(path,name))
            tabs["捐赠情况"].append(values)

            # ##############################################
            # # 
            # ##############################################
            # tabs["购买资产管理"].extend(records_gmzcgl(path,name))
            # tabs["持有股权的实体情况"].extend(records_cygqst(path, name))
            # tabs["监事会情况"].extend(records_jsqk(path,name))
            # tabs["理事长秘书长"].extend(records_lsqk(path,name))
            # tabs["委托投资情况"].extend(records_ywhd5(path,name))
            
        except Exception as ex:
            print("Exception parsing <{}>".format(name))
            raise ex
            failed += 1
        
        # if i ==0:
        #     break
        # if i >= 10:
        #     break
    print("Failed files in total <{}>".format(failed))

    writer = pd.ExcelWriter('results/深圳基金会-接受捐赠.xlsx')

    for tab, records in tabs.items():
        df = pd.DataFrame(records) 
        df.to_excel(writer,tab,index=False)
    writer.save()

def guangzhouReportLinks():
    url = "https://www.gdnpo.gov.cn/home/index/indexywnjdtnew?MainMc=%u57FA%u91D1%u4F1A&MainCreID=&NJND=2018"
    coockies = {
        "ASP.NET_SessionId": "vj4jkfxl1vefcsc5dufvyz2x",
        "Hm_lpvt_297a9be5a55d666e645ce55f89be171a": "1590648759",
        "Hm_lvt_297a9be5a55d666e645ce55f89be171a": "1590648558,1590648559,1590648759,1590648759"
    }
    r = requests.get(url, proxies = proxy, headers = headers, cookies=coockies)
    if r.status_code != 200:
        return

    content = r.text
    print(content)

if __name__ == '__main__':
    #shenzhenReportLinks()
    # shenzhenReports()
    parse_reports()

    # guangzhouReportLinks()
