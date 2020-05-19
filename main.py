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

headers={
    "Host": "www.chinanpo.gov.cn",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
    "Origin": "http://www.chinanpo.gov.cn",
    "Content-Type": "application/x-www-form-urlencoded"
}

SESSIONID = "0E6EF88C5BE69C37B536BFD783317E5A"

def sleep(s=1,e=2):
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
    
    # <iframe  name="首页" src="/SOCSP_O/njnbMf/toIndex?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=483cb448150749c4876f79a471960aee&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="基本信息" src="/SOCSP_O/njnbMf/toJbxx?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=e75f654cbe124a7992e55add4066ae35&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="监事会信息" src="/SOCSP_O/njnbMf/toZzjsJs?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=29ab3ebe15734ae1a2f198a3f33ceda6&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="组织建设一" src="/SOCSP_O/njnbMf/toZzjs?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=d7c9509e77d1451db95751fb6a8b1f18&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="理事会信息" src="/SOCSP_O/njnbMf/toLShxx?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=d46af5dd7b9c479faadf0ca8d8025569&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="组织建设二" src="/SOCSP_O/njnbMf/toZzjs2?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=aaa5fb5e9652457cb7c24ee2ab17180f&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="内部制度建设" src="/SOCSP_O/njnbMf/toNbzdjs?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=20239a9b95844778a74ec40b939c55d7&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="党建情况" src="/SOCSP_O/publicnjnb/toDjqk?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=ca7a850398ce4abca104a86a419e75aa&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="工作采集表" src="/SOCSP_O/njnbMf/toGzcjb?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=dfd720d52d3c469d9b30c888c536f71c&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="监督管理" src="/SOCSP_O/njnbMf/toJdgl?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=1d439d63e33e45908438999ecfef5b1e&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="资产负债表" src="/SOCSP_O/publicnjnb/topublicZcfz?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=31968c50628b456a9869de3a84f4d05d&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="业务活动表" src="/SOCSP_O/publicnjnb/topublicYwhd?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=7972c934d8f34a0cafb60ed4f231d907&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="现金流量表" src="/SOCSP_O/publicnjnb/topublicXjll?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=c17f07c63da0422686b5a5c9583f6cce&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="附注" src="/SOCSP_O/njnbMf/toFz?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=2f36a1f054214ac085078576b15f4aa5&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="净资产" src="/SOCSP_O/njnbMf/toJzc?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=0bdaae1500534a55b760f6b0c85c364c&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="政府补助收入" src="/SOCSP_O/njnbMf/toZfbzsr?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=fb516bd0bbbb4db39955ee9893e641c5&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="年度公益活动" src="/SOCSP_O/njnbMf/toNdgyhd?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=0fef381cd7104541898e10e2494dd17f&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="年度工作总结" src="/SOCSP_O/njnbMf/toNdgzzj?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=83202c84780c4b98962927bb6a05bcf8&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="公益支出比例" src="/SOCSP_O/njnbShtt/toWelfareForm?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=72086076a7b94cc3be72f88c4ef59b84&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="业务开展情况" src="/SOCSP_O/njnbJjh/toYwkzqk?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=089d349df95645d783779ab079face11&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="公益项目收支" src="/SOCSP_O/njnbMf/toGyxmsz?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=da10eca2d46e4ef6a708d5430a09094c&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>
    # <iframe  name="公益活动支出" src="/SOCSP_O/publicnjnb/toGyhdzc?declareId=04fde8135bd04309a6b5c864fd5e1dc9&formId=2206dc51892b455d87fde7dbffd68154&print=1&dy=1"  marginheight="0" marginwidth="0" frameborder="0" scrolling="none" style="visibility:hidden;width:100%; overflow:auto; border-bottom:0px solid blue" ></iframe>

    #url = "http://218.17.83.146:9009/SOCSP_O/publicnjnb/printAll?declareId="
    url = "http://218.17.83.146:9009"

    settings = [
        ("资产负债表","/SOCSP_O/publicnjnb/topublicZcfz?formId=31968c50628b456a9869de3a84f4d05d&print=1&dy=1&declareId="),
        ("业务活动表","/SOCSP_O/publicnjnb/topublicYwhd?formId=089d349df95645d783779ab079face11&print=1&dy=1&declareId=")
    ]
    failed = 0
    for i,(name,did) in enumerate(zip(agents.SOCIETY_NAME,agents.DECLAREID)):

        if isinstance(name,str)==False or isinstance(did,str)==False or name is None or did is None or not name.strip() or not did.strip() or name.strip() in names:
            continue
        if "基金会" not in name:
            continue

        name, did = name.strip(), did.strip()

        print("Loading <{}/{}>: {} {}".format(i+1,agents.shape[0], name, did))

        
        contents = []
        try:
            for tname, postfix in settings:
                url_ = url + postfix +  did
                r = requests.get(url_, proxies = proxy, headers = headers, cookies={"JSESSIONID": SESSIONID})
                if r.status_code != 200:
                    continue
                contents.append((tname, r.text))
        except Exception as ex:
            failed += 1
        finally:
            if len(contents) == 2:
                for tname, content in contents:
                    with open("{}/{}-{}.html".format(path,name,tname),"w", encoding="utf8") as writer:
                        writer.write(content)
        sleep()
    print("Failed files in total <{}>".format(failed))

def parse_reports():
    path = "reports"

    failed = 0
    records = {}
    for i, file in enumerate(os.listdir(path)):
        if os.path.isdir(os.path.join(path,file)):
            continue
        groups = re.search("(.*)-(.*).html", file)
        if groups is None:
            continue
        name = groups.group(1)
        tname = groups.group(2)
        
        try:
            if tname == "资产负债表":
                with open(os.path.join(path, file),"r",encoding="utf8") as reader:
                    data = reader.read()
                    
                    soup = BS(data, "html.parser")
                    rows = soup.find("table", {"id": "tablepr"}).findAll("tr") 
                    
                    values = {
                    "组织名称": name,
                    "资产总计": "",
                    "净资产合计": "",
                    "非限制性净资产": "",
                    "货币资金": "",
                    "短期投资": "",
                    "长期股权投资": "",
                    "长期债权投资": ""
                    }
                    for r in rows:
                        fields = tuple(td.text.strip() for td in r.findAll("td"))
                        #print(fields)
                        for n,rid,_,v in [fields[:4], fields[4:]]:
                            if n in values:
                                values[n] = v
                    
                    if name in records:
                        records[name].update(values)
                    else:
                        records[name] = values

            if tname == "业务活动表":
                with open(os.path.join(path, file),"r",encoding="utf8") as reader:
                    data = reader.read()
                    
                    soup = BS(data, "html.parser")
                    rows = soup.find("table", {"id": "tablepr"}).findAll("tr") 

                    values = {
                    "组织名称": name,
                    "捐赠收入": "",
                    "政府补助收入": "",
                    "提供服务收入": "",
                    "投资收益": "",
                    }

                    for r in rows:
                        fields = tuple(td.text.strip() for td in r.findAll("td"))
                        if len(fields) != 8:
                            continue
                        if fields[0] == "其中：捐赠收入":
                            values["捐赠收入"] = fields[-1]
                        elif fields[0] in values:
                            values[fields[0]] = fields[-1]

                    if name in records:
                        records[name].update(values)
                    else:
                        records[name] = values
        except Exception as ex:
            print("Exception parsing <{}>".format(file))
            failed += 1
    print("Failed files in total <{}>".format(failed))

    df = pd.DataFrame(records.values()) #[["组织名称","资产总计","净资产合计","非限制性净资产","货币资金","短期投资","长期股权投资","长期债权投资","捐赠收入","政府补助收入","提供服务收入","投资收益"]]
    df.to_csv("result.csv",index=False)


if __name__ == '__main__':
    # shenzhenReportLinks()
    shenzhenReports()
    # parse_reports()