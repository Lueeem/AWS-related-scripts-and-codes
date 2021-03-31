import random
import csv

i=0

cols = ["Trading_Date","Transaction_Type","Stock_Code",
        "Volume","Price","Value","Currency","Buy_Broker_Code",
        "Sell_Broker_Code","Buy_Account_Code","Sell_Account_Code",
        "Buy_Trade_Origin","Sell_Trade_Origin","Short_Sell_Type",
        "Market_Type"]

rows = []

while(i <= 1000):
    rand_date=str(random.randint(5,30))+"/5/20"
    rand_stockcode=str(random.randint(1000,9999))+random.choice("VWXYZ")+random.choice("ABCDE")
    rand_vol=random.choice([50,100,300])
    rand_price=random.uniform(0.1,1.0)
    rand_val=rand_vol*rand_price
    rand_origin=random.choice(["DMA", "Algo", "Internet"])
    rand_selltype=random.choice(["", "PDT", "RSS"])
    rows.append(
        [
            rand_date,"Buy",rand_stockcode,
            str(rand_vol),str(rand_price)[:4],
            str(rand_val)[:4],"MYR",str(i),
            str(1001-i),str(100000000+i),
            str(100000000+1001-i),rand_origin,
            rand_origin,rand_selltype,"OMT"
        ]
    )
    i += 1 

with open("trading.csv","w") as file:
    write = csv.writer(file)
    write.writerow(cols)
    write.writerows(rows)

