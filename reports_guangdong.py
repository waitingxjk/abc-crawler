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

with open("guangdong/names.txt","r",encoding="utf8") as reader:
    NAMES = reader.read().strip().split(",")

proxy = {
    "http": "http://127.0.0.1:12639",
    "https": "https://127.0.0.1:12639",
}

headers={
    # "Host": "www.chinanpo.gov.cn",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
    "Origin": "http://www.chinanpo.gov.cn",
    "Content-Type": "application/x-www-form-urlencoded"
}

coockies = {
    "ASP.NET_SessionId": "zh1seoirbvnpyd4lzmhiuy5n",
    "Hm_lpvt_297a9be5a55d666e645ce55f89be171a": "1591928793",
    "Hm_lvt_297a9be5a55d666e645ce55f89be171a": "1590648759,1590664966,1590664966,1591928786"
}

def sleep(s=1,e=2):
    s = random.randint(s,e)
    print("sleep for %d seconds" % s)
    time.sleep(s)

def guangzhouReportLinks():
    url = "https://www.gdnpo.gov.cn/home/index/indexywnjdtnew?MainMc=%E5%9F%BA%E9%87%91%E4%BC%9A&MainCreID=&NJND=2018&cp={}"
    
    total = 25
    for i in range(1, 25):
        print("Loading page {}/{}".format(i, total))

        r = requests.get(url.format(i), proxies = proxy, headers = headers, cookies=coockies)
        if r.status_code != 200:
            continue
        content = r.text

        with open("guangdong/pages/page-{}.html".format(i),"w",encoding="utf8") as writer:
            writer.write(content)

def load_html(path,name,tname):
    file = "{}/{}-{}.html".format(path,name,tname)
    print("Parsing <{}>".format(file))

    with open(file,"r",encoding="utf8") as reader:
        data = reader.read()
    
    soup = BS(data, "html.parser")
    return soup

def guangzhouReports():
    reports = []

    total = 25
    for i in range(1, 25):
        soup = load_html("guangdong/pages","page",i)
        table = soup.find("table",{"class": "admintable"})

        for row in table.tbody.findAll("tr"):
            fields = [v.text.strip() if i<6 else v.find('a').get('href') for i, v in enumerate(row.findAll("td"))]
            if fields[5] != '已报送并公开':
                continue
            reports.append(fields)
    
    import pandas as pd
    data = pd.DataFrame(reports,columns=["序号","名称","代码","法人","年份","年报","链接"])
    data.to_csv("guangdong/links.csv",index=False)

headers={
    "Host": "www.gdnpo.gov.cn",
    # "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    # "Content-Type": "application/x-www-form-urlencoded",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "max-age=0"
}

def parse_frames():
    import pandas as pd 
    data = pd.read_csv("guangdong/links.csv")
    data.columns = ["序号","名称","代码","法人","年份","年报","链接"]

    url = "https://www.gdnpo.gov.cn/pub/xxgsShzzPageYwDetail"

    records = []
    for i,(name,link) in enumerate(zip(data.名称,data.链接)):
        link = url + link[link.rfind("/"):]
        print("<{}/{}> downloading frame information for <{}>: <{}>".format(i+1,data.shape[0],name, link))

        r = requests.get(link, proxies = proxy, headers = headers, cookies=coockies)
        if r.status_code != 200:
            continue
        soup = BS(r.text, "html.parser")
        menu = soup.find("p",{"id": "ifmenus"})

        m = {"名称": name, "链接": link}
        m.update({a.text.strip(): a.get("href") for a in menu.findAll("a")})

        records.append(m)

    data = pd.DataFrame(records)

    data.to_csv("guangdong/frames.csv",index=False)
        

def downloadHTML(link):
    print("Downloading from <{}>".format(link))
    r = requests.get(link, proxies = proxy, headers = headers, cookies=coockies)
    if r.status_code != 200:
        return None
    return BS(r.text, "html.parser")

def parse_reports():
    import pandas as pd 
    data = pd.read_csv("guangdong/frames.csv")

    url = "https://www.gdnpo.gov.cn"

    def parse_ywhdb(r):
        link = r["业务活动表"]
        if not link:
            return {}

        soup = downloadHTML(url + link)
        if not soup:
            return {}

        rows = soup.findAll("table")[0].tbody.findAll("tr")

        # for row in rows:
        #     print("=" * 80)
        #     print(row)

        fields = [tuple(td for td in r.findAll("td")) for r in rows]
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
            "21": "管理费用",
            "24": "筹资费用",
            "28": "其他费用",
            "35": "费用合计",
            "45": "净资产变动额",
        }
        values = {"机构": r["名称"]}
        for row in fields:
            rid = row[1].text.strip()
            if rid in maps:
                for i,it in [(2,"上年末期"),(5,"本年累计")]:
                    for j,jt in [(0,"非限定"),(1,"限定"),(2,"合计")]:
                        #print("rid=<{}> row[i+j]=<{}>".format(rid, row[i+j]))
                        values["{}-{}-{}".format(maps[rid],it,jt)] = row[i+j].find("font",{"name": "showArea"}).text.strip()
        return values

    def parse_zxjj(r):
        link = r["专项基金、分支（代表）机构及内设机构基本情况"]
        if not link:
            return {}

        soup = downloadHTML(url + link)
        if not soup:
            return {}

        rows = soup.find("div",{"class": "htcontainer"}).find("table").findAll("tr")[:6]
        fields = [tuple(td for td in r.findAll("td")) for r in rows]

        values = {"机构名称": r["名称"]}
        settings = [
            ("专项基金总数",0,1,3,1),
            ("代表机构总数",2,1,3,1),
            ("内设机构总数",4,1,3,1),
        ]
        for n,r,c1,c2,c3 in settings:
            values["{}".format(n)] = fields[r][c1].find("font",{"name": "showArea"}).text.strip()
            values["{}-本年度新设".format(n)] = fields[r][c2].find("font",{"name": "showArea"}).text.strip()
            values["{}-本年度注销".format(n)] = fields[r+1][c3].find("font",{"name": "showArea"}).text.strip()

        return values

    def parse_gysy(r):
        """各机构字段定义不一致"""
        link = r["公益事业（慈善活动）支出和管理费用情况"]
        if not link:
            return {}

        soup = downloadHTML(url + link)
        if not soup:
            return {}
        return

        rows = soup.find("div",{"id": "Div1"}).findAll("table")[-1].findAll("tr")[1:9]
        fields = [tuple(td for td in r.findAll("td")) for r in rows]

    def parse_zcfz(r):

        link = r["资产负债表"]
        if not link:
            return {}

        soup = downloadHTML(url + link)
        if not soup:
            return {}

        rows = soup.findAll("div",{"class": "stcontainer f12 c"})[0].findAll("table")[0].findAll("tr")
        fields = [tuple(td for td in r.findAll("td")) for r in rows]

        values = {"机构名称": r["名称"]}
        for r in fields:
            if r[1].text.strip()=="60":
                values["{}-年初数".format("资产总计")] = r[2].find("font",{"name": "showArea"}).text.strip()
                values["{}-期末数".format("资产总计")] = r[3].find("font",{"name": "showArea"}).text.strip()

        fields = [v for v in fields if len(v)==8]
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
        for _,rid,s,e in fields:
            rid = rid.text.strip()
            if rid in maps:
                values["{}-年初数".format(maps[rid])] = s.find("font",{"name": "showArea"}).text.strip()
                values["{}-期末数".format(maps[rid])] = e.find("font",{"name": "showArea"}).text.strip()
        
        return values

    def parse_jbxx(r):
        link = r["基本信息"]
        if not link:
            return {}

        soup = downloadHTML(url + link)
        if not soup:
            return {}

        values = {"机构名称": r["名称"]}

        tables = soup.findAll("table")

        rows = tables[-2].findAll("tr")
        fields = [tuple(td for td in r.findAll("td")) for r in rows]

        settings = [
            ("类型",4,1),
            ("成立时间",5,3),
            ("住所",9,1),
        ]
        for n,r,c in settings:
            v = fields[r][c].find("font",{"name": "showArea"}).text.strip()
            if v:
                values[n] = v
            else:
                values[n] = ""

        rows = tables[-1].findAll("tr")
        fields = [tuple(td for td in r.findAll("td")) for r in rows]

        settings = [
            ("理事数",8,1),
            ("监事数",9,1),
            ("负责人数",9,3),
            ("专职工作人员数",10,1),
        ]
        for n,r,c in settings:
            v = fields[r][c].find("font",{"name": "showArea"}).text.strip()
            if v:
                values[n] = v
            else:
                values[n] = ""
        return values

    def parse_dzzjs(r):
        link = r["党组织建设情况"]
        if not link:
            return {}

        soup = downloadHTML(url + link)
        if not soup:
            return {}

        values = {"机构名称": r["名称"]}
        ps = soup.find("div",{"class": "PrintArea"}).find("div").findAll("p")[1].find("font",{"name": "showArea"})
        if ps:
            values["是否建立党组织"] = ps.text.strip()
        else:
            values["是否建立党组织"] = "未知"

        # print(soup.text)
        # m = re.match("是否建立党组织:\n(.)", soup.text)
        # if m:
        #     values["是否建立党组织"] = m.group()
        # else:
        #     values["是否建立党组织"] = "未知"
        

        rows = soup.findAll("div",{"class": "stcontainer f14 c"})[0].find("table").findAll("tr")
        fields = [tuple(td for td in r.findAll("td")) for r in rows]

        settings = [
            ("党员总人数",3,2),
            ("专职人员中党员人数",8,1)
        ]
        
        for n,r,c in settings:
            v = fields[r][c].find("font",{"name": "showArea"})
            if v:
                values[n] = v.text.strip()
            else:
                values[n] = ""
        return values

    def parse_xxgk(r):
        link = r["信息公开情况"]
        if not link:
            return {}

        soup = downloadHTML(url + link)
        if not soup:
            return {}

        rows = soup.find("div",{"class": "stcontainer f14 c"}).find("table").findAll("tr")
        tds = rows[0].findAll("td")

        values = {
            "机构名称": r["名称"],
            "是否履行信息公开义务": tds[1].find("font",{"name": "showArea"}).text.strip()
        }

        return values

    def parse_xxgkzd(r):
        link = r["内部制度建设情况"]
        if not link:
            return {}

        soup = downloadHTML(url + link)
        if not soup:
            return {}

        rows = soup.find("div",{"class": "stcontainer f14 c"}).find("table").findAll("tr")
        tds = rows[24].findAll("td")

        values = {
            "机构名称": r["名称"],
            "信息公开制度": tds[2].find("font",{"name": "showArea"}).text.strip()
        }

        return values


    def records_lshzk(r):
        link = r["理事会召开情况"]
        if not link:
            return []

        soup = downloadHTML(url + link)
        if not soup:
            return []

        p = "本基金会于.*?(\d+).*?年.*?(\d+).*?月.*?(\d+).*?日召开.*?(\d+).*?届"
        matches = re.findall(p, soup.text)
        records = []
        for i, m in enumerate(matches):
            values = {
                "机构名称": r["名称"],
                "次数": i+1,
                "日期": "{}年{}月{}日".format(m[0],m[1],m[2]),
                "届": m[3]
            }
            records.append(values)
        return records

    def records_zcgl(r):
        link = r["购买资产管理产品情况"]
        if not link:
            return []

        soup = downloadHTML(url + link)
        if not soup:
            return []

        rows = soup.find("div",{"class": "htcontainer f14 c"}).findAll("table")[0].findAll("tr")[1:]
        fields = [tuple(td for td in r.findAll("td")) for r in rows]
        fields = [v for v in fields if len(v)==5]

        records = []
        for vs in fields:
            name = vs[1].find("font",{"name": "showArea"}).text.strip()
            if not name or name=="无":
                continue

            values = {
                "机构名称": r["名称"],
                "序号": vs[0].text.strip(),
                "产品": name,
                "购买金额": vs[2].find("font",{"name": "showArea"}).text.strip(),
                "当年实际收益额": vs[3].find("font",{"name": "showArea"}).text.strip(),
                "当年实际收回额": vs[4].find("font",{"name": "showArea"}).text.strip()
            }
            records.append(values)
        return records

    def records_wttz(r):
        link = r["委托投资情况"]
        if not link:
            return []

        soup = downloadHTML(url + link)
        if not soup:
            return []

        rows = soup.find("div",{"class": "htcontainer f14 c"}).findAll("table")[0].findAll("tr")[1:]
        fields = [tuple(td for td in r.findAll("td")) for r in rows]
        fields = [v for v in fields if len(v)==7]

        records = []
        for i,vs in enumerate(fields):
            name = vs[1].find("font",{"name": "showArea"}).text.strip()
            if not name or name=="无":
                continue

            values = {
                "机构名称": r["名称"],
                "序号": i + 1,
                "受托机构": vs[0].find("font",{"name": "showArea"}).text.strip(),
                "委托金额": vs[2].find("font",{"name": "showArea"}).text.strip(),
                "委托期限": vs[3].find("font",{"name": "showArea"}).text.strip(),
                "当年实际收益额": vs[5].find("font",{"name": "showArea"}).text.strip(),
                "当年实际收回额": vs[6].find("font",{"name": "showArea"}).text.strip()
            }
            records.append(values)
        return records



    from collections import defaultdict

    tabs = defaultdict(list)
    for i,r in enumerate(data.to_dict("records")):
        if r["名称"] not in NAMES:
            continue

        print("="*80)
        print("<{}/{}> Parsing for <{}>".format(i+1,data.shape[0],r["名称"]))

        # tabs["基本信息"].append(parse_jbxx(r))
        # tabs["业务活动"].append(parse_ywhdb(r))
        # tabs["专项基金"].append(parse_zxjj(r))
        tabs["资产负债"].append(parse_zcfz(r))
        # tabs["党组织建设"].append(parse_dzzjs(r))
        
        # values = {}
        # values.update(parse_xxgk(r))
        # values.update(parse_xxgkzd(r))
        # tabs["信息公开情况"].append(values)

        # tabs["资产管理"].extend(records_zcgl(r))
        # tabs["委托投资"].extend(records_wttz(r))
        # tabs["理事会召开"].extend(records_lshzk(r))
        
        # 有问题
        # parse_gysy(r)

        # if i == 9:
        #     break

    writer = pd.ExcelWriter('guangdong/广东基金会.xlsx')

    for tab, records in tabs.items():
        df = pd.DataFrame(records) 
        df.to_excel(writer,tab,index=False)
    writer.save()

if __name__ == '__main__':
    # guangzhouReportLinks()
    # guangzhouReports()
    # parse_frames()
    parse_reports()
