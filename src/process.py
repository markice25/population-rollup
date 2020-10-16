import csv

if __name__ == "__main__":
    cbsa_dict = {}
    with open("input/censustract-00-10.csv","r") as file:
        next(file)
        reader = csv.reader(file)
        for line in reader:
            #print(line)
            #cols = line.split(",")
            cbsa09, cbsa_t, pop_00, pop_10 = line[7],line[8],line[12],line[14]
            #print(line)
            #print(cbsa09, cbsa_t, pop_00, pop_10)
            if not cbsa09:
                continue
            if cbsa09 not in cbsa_dict.keys():
                cbsa_dict[cbsa09] = [cbsa_t,0,0,0,0.00]
            cbsa_dict[cbsa09][1] += 1
            cbsa_dict[cbsa09][2] += int(pop_00)
            cbsa_dict[cbsa09][3] += int(pop_10)


    for key, value in cbsa_dict.items():
        pop_change = float(value[3] - value[2])/ float(value[2])
        cbsa_dict[key][4] = round(pop_change,2)
        
    with open("output/report.csv","w") as file:
        writer = csv.writer(file)
        for key, value in cbsa_dict.items():
            writer.writerow([key]+value)
