import os
from multiprocessing import Pool
from libs.hash import str_hash
WORKER_NUM = 5
DATA_DIR = "./data"
KEY_DIR = "./key"

def int_to_bytes(x: int, len=6) -> bytes:
    return x.to_bytes(len, 'big')

def int_from_bytes(xbytes: bytes) -> int:
    return int.from_bytes(xbytes, 'big')

def train_data(file):
    data_file = os.path.join(DATA_DIR,file)
    key_file = os.path.join(KEY_DIR,file+".db")
    des_file = os.path.join(KEY_DIR,file+".des")
    index_map = {}
    with open(data_file, "r", encoding="utf-8") as df:
        while True:
            rpid = df.readline()
            if rpid=="":
                break
            rpid = int(rpid)
            content = df.readline()
            hashlist = str_hash(content)
            for _hash in hashlist:
                if _hash not in index_map:
                    index_map[_hash] = []
                index_map[_hash].append(rpid)
    key_map = []
    with open(key_file, "wb") as kf:
        for key in sorted(index_map):
            key_map.append((key, kf.tell()))
            for value in index_map[key]:
                kf.write(int_to_bytes(value))
    with open(des_file, "wb") as df:
        for key in key_map:
            df.write(int_to_bytes(key[0]))
            df.write(int_to_bytes(key[1]))
    print(len(key_map))

def main():
    data_files = os.listdir(DATA_DIR)
    processes = Pool(WORKER_NUM)
    print("start work")
    [processes.apply_async(train_data, args=(file, )) for file in data_files]
    processes.close()
    processes.join()
    print("end work")

if __name__=="__main__":
    main()