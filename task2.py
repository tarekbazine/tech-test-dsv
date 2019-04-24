import json

import xlrd
import urllib.parse
import xmltodict
from urllib.request import Request, urlopen
from pymongo import MongoClient

def read_xlsx(path):
    wb = xlrd.open_workbook(path)
    return wb.sheet_by_index(0)


def int_str(x):
    return str(int(x))


def build_obj(data):
    _o = {
        "declVal": data[13],
        "declValCur": data[14],
        "wgtUom": data[12],
        "noPce": int_str(data[10]),
        "wgt0": str(data[11]),
        "shpDate": data[1],
        "orgCtry": data[4],
        "orgCity": data[3],
        "orgZip": str(data[2]),
        "dstCtry": data[8],
        "dstCity": data[7],
        "dstZip": str(data[6]),
    }

    if int(_o["noPce"]) > 1:
        for j in range(1, int(_o["noPce"])):
            _o["wgt" + str(j)] = _o["wgt0"]

    return _o


def get_results(obj):
    _params = urllib.parse.urlencode(obj)
    _url = "http://dct.dhl.com/data/quotation/?dtbl=N&w0=0&l0=0&h0=0&dimUom=cm&" + _params

    print(_url)
    req = Request(_url, headers={'User-Agent': 'Mozilla/5.0'})
    _res = urlopen(req).read()

    return xmltodict.parse(_res)


# dsv-task2
# etas
# dsa@2019
# terrybaz@tmails.net

if __name__ == '__main__':

    loc = ("./data_in/Test 2 - DHL Shipments Report.xlsx")

    sheet = read_xlsx(loc)

    client = MongoClient('mongodb+srv://terry:dsa2019@cluster0-w4atg.mongodb.net/test?retryWrites=true')
    db = client['dsv-task2']

    etas = db.etas

    _list_etas = []

    for i in range(1, sheet.nrows):
        print(sheet.row_values(i))
        _obj = build_obj(sheet.row_values(i))
        res = get_results(_obj)

        _count = int(res['quotationResponse']['count'])
        if _count == 0:
            print('***********************')
            _obj["ETA OK?"] = "NO"
            _obj["ETA"] = res['quotationResponse']['errorMessage']
        elif _count == 1:
            _obj["ETA OK?"] = "YES"
            _obj["ETA"] = res['quotationResponse']['quotationList']['quotation']['estDeliv']
        else:
            for k in range(0, _count):
                _obj["ETA " + str(k) + " OK?"] = "YES"
                _obj["ETA " + str(k)] = res['quotationResponse']['quotationList']['quotation'][k]['estDeliv']

        _obj["DHL Response"] = res

        print(res['quotationResponse']['count'])

        _list_etas.append(_obj.copy())

        result = etas.insert_one(_obj)
        print('inserted id : {0}'.format(result.inserted_id))

    file_output = open("./data_out/task2_ETAs.json", "w", encoding='utf-8-sig')
    json.dump(_list_etas, file_output, ensure_ascii=False)
