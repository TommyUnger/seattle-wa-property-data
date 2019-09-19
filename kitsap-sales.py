import import_utils
import pandas
from tabula import read_pdf
import requests
import time
import os
import re

kitsap_hoods = [7100519, 7100521, 7100522, 7100531, 7100541, 7100542, 7100543, 7100580, 7100591, 7100592, 7303601, 
                7303602, 7303603, 7303604, 7303605, 7303606, 7303607, 7303608, 7303609, 7303610, 7303611, 7303612, 
                7303621, 7303622, 7303623, 7303624, 7303625, 7303626, 7303627, 7303680, 7400201, 7400202, 7400203, 
                7400204, 7400205, 7400207, 7400220, 7400221, 7400222, 7400231, 7400241, 7400251, 7400280, 7400303, 
                7400304, 7400305, 7400306, 7400307, 7400308, 7400309, 7400311, 7400312, 7400313, 7400320, 7400321, 
                7400322, 7400323, 7400324, 7400325, 7400326, 7400331, 7400351, 7400380, 7400390, 7401113, 7401114, 
                7401116, 7401117, 7401118, 7401120, 7401121, 7401122, 7401123, 7401124, 7401125, 7401127, 7401131, 
                7401132, 7401141, 7401151, 7401180, 7401190, 7401521, 7401522, 7401580, 7401591, 7401592, 7402381, 
                7402390, 7402391, 7402393, 7402394, 7402395, 7402396, 7402401, 7402402, 7402403, 7402404, 7402405, 
                7402406, 7402421, 7402422, 7402423, 7402424, 7402425, 7402426, 7402480]
                
                
years = [2013, 2014, 2015, 2016, 2017, 2018]

def url_to_file(url, file_name):
    with open(file_name, "wb") as file:
        response = requests.get(url)
        file.write(response.content)

good_count = 0
fw = open("data/kitsap-sales.txt", "w")
for year in years:
    for hood in kitsap_hoods:
        file_name = "data/%s-%s.pdf" % (hood, year)
        if not os.path.exists(file_name):
            url = "https://www.kitsapgov.com/assessor/Documents/%s-%s.pdf" % (hood, year)
            print(url)
            url_to_file(url, file_name)
            time.sleep(1)
        else:
            try:
                df = read_pdf(file_name, 
                              pages="all",
                              guess=False, 
                              stream=True,
                              columns=(106, 253, 291, 342, 390, 495, 630, 683)
                             )
            except:
                print("*Empty file*: %s" % file_name)
                continue
            if df is None:
                print("*None*: %s" % file_name)
                continue
            print("File %30s - Row count: %s" % (file_name, len(df)))
#             print("Column count: %s" % len(df.columns))
#             print("First column name: '%s'" % df.columns[0])
#             print("Last column name: '%s'" % df.columns[-1])
#             print("First cell value: '%s'" % df.iloc[0, 0])
#             print("-----" * 20)
            for i, row in df.iterrows():
                pin_m = re.match('([0-9-]{10,20})', str(row[0]))
                if pin_m:
                    pin = re.sub("[^0-9]+", "", pin_m.groups()[0])
                    address = str(row[1])
                    sqft_lot = int(float(str(row[2])) * 43560)
                    land_infl = re.sub('[^a-zA-Z]+', '', str(row[4]))
                    if land_infl == 'nan':
                        land_infl = ''
                    m = re.match('([a-z]) ([a-z]+) ([a-z]+)', str(row[5]).lower())
                    if m:
                        quality = ''.join(m.groups()[0:2])
                        home_type = m.groups()[2]
                    m = re.match('([0-9]+) ([0-9]+) sf ([a-z]+) cond ([0-9]+).*', str(row[6]).lower())
                    if m:
                        year_built = m.groups()[0]
                        sqft_finished = m.groups()[1]
                        condition = m.groups()[2]
                        perc_finished = m.groups()[3]
                    sale_date = str(row[7])
                    sale_price = re.sub('[^0-9]+', '', str(row[8]))
                    fw.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
                                                        pin, address, sqft_lot, land_infl, quality, 
                                                         home_type, year_built, sqft_finished, condition
                                                        , perc_finished, sale_date, sale_price))
            good_count += 1
