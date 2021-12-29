import os
WORKER_NUM = 5
KEY_DIR = "./key"
OUTPUT_DIR = "./merged"

def int_to_bytes(x: int) -> bytes:
    return x.to_bytes(6, 'big')

def int_from_bytes(xbytes: bytes) -> int:
    return int.from_bytes(xbytes, 'big')

def merge():
    files = os.listdir(KEY_DIR)
    db_files = []
    for file in files:
        splited_name = os.path.splitext(file)
        if splited_name[-1]==".db":
            db_files.append((file, splited_name[0]+".des"))
    db_num = len(db_files)
    index_maps = [[] for _ in range(0, db_num)]
    for index, db_file in enumerate(db_files):
        with open(os.path.join(KEY_DIR, db_file[1]), "rb") as f:
            while True:
                key = f.read(6)
                if key==b"":
                    break
                key = int_from_bytes(key)
                offset = int_from_bytes(f.read(6))
                index_maps[index].append((key, offset))
    lists = [index_maps[i].pop(0) for i in range(0, db_num)]
    files = [open(os.path.join(KEY_DIR, db_file[0]), "rb") for db_file in db_files]
    all_map = {}
    with open(os.path.join(OUTPUT_DIR, "merged.db"), "wb") as outfile:
        while lists:
            min_item = min(lists, key=lambda x:x[0])
            min_index = lists.index(min_item)
            if min_item[0] not in all_map:
                all_map[min_item[0]] = outfile.tell()
            try:
                next_item = index_maps[min_index].pop(0)
                length = next_item[1] - min_item[1]
                outfile.write(files[min_index].read(length))
                lists[min_index] = next_item
            except IndexError:
                outfile.write(files[min_index].read())
                lists.pop(min_index)
                index_maps.pop(min_index)
                files[min_index].close()
                files.pop(min_index)
            

    print(len(all_map))
    des_file = os.path.join(OUTPUT_DIR, "merged.des")
    with open(des_file, "wb") as df:
        for key in sorted(all_map):
            df.write(int_to_bytes(key))
            df.write(int_to_bytes(all_map[key]))

    

def main():
    merge()

if __name__=="__main__":
    main()