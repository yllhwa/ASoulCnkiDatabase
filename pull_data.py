from multiprocessing import Pool
import pymysql
from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
PAGE_SIZE = 10000
WORKER_NUM = 10

def get_page_data(page_index):
    db = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    cursor = db.cursor()
    SQL=f"select * from reply limit {(page_index)*PAGE_SIZE}, {PAGE_SIZE};"
    cursor.execute(SQL)
    results=cursor.fetchall()
    cursor.close()
    db.close()
    return results

def save_page(page_index):
    replies = get_page_data(page_index)
    with open(f"data/page{page_index}.txt", "w", encoding="utf-8") as f:
        for reply in replies:
            rpid = reply[0]
            content = reply[7].replace(' ', '').replace('\n', '').replace('\r', '')
            f.write(f"{rpid}\n{content}\n")
        f.flush()
        f.close()

def main(page_start, page_end):
    processes = Pool(WORKER_NUM)
    print("start work")
    [processes.apply_async(save_page, args=(i, )) for i in range(page_start, page_end)]
    processes.close()
    processes.join()
    print("end work")

if __name__=="__main__":
    main(1, 300)