import csv
import json

def csv_dict(path='/Users/thiyageshdhandapani/Documents/Thiyagesh_code/Python-Cache-main/CSVReader/missing_rc_events.csv'):
    with open(path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        i = 0
        d = {}
        for row in csv_reader:
            if not i:
                i += 1
                continue
            if d.get(row[2]):
                l = d[row[2]]
                l.append(row[0])
                d[row[2]] = l
            else:
                d[row[2]] = [row[0]]
            line_count += 1

        print(f"Line count: {line_count}. RC List: {d}")
    rc_list_path = "/".join(path.split('/')[:-1]) + "/result_files/rc_dict.json"
    with open(rc_list_path, "w+") as fin:
        json.dump(d, fin)

def csv_reader(path='/Users/thiyageshdhandapani/Documents/Thiyagesh_code/Python-Cache-main/CSVReader/CAN.csv'):
    with open(path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        rc_list = []
        for row in csv_reader:
            rc_list.extend(row)
            line_count += 1

        print(f"Line count: {line_count}. RC List: {rc_list}")
    rc_list_path = "/".join(path.split('/')[:-1]) + "/result_files/rc_list_can.txt"
    with open(rc_list_path, "w+") as fin:
        json.dump(rc_list, fin)

if __name__ == '__main__':
    csv_dict()