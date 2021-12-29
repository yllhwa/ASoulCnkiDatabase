from libs.hash import str_hash
from collections import defaultdict
test_text = """我好想做嘉然小姐的狗啊。
可是嘉然小姐说她喜欢的是猫，我哭了。
我知道既不是狗也不是猫的我为什么要哭的。因为我其实是一只老鼠。
我从没奢望嘉然小姐能喜欢自己。我明白的，所有人都喜欢理解余裕上手天才打钱的萌萌的狗狗或者猫猫，没有人会喜欢阴湿带病的老鼠。
但我还是问了嘉然小姐:“我能不能做你的狗？”
我知道我是注定做不了狗的。但如果她喜欢狗，我就可以一直在身边看着她了，哪怕她怀里抱着的永远都是狗。
可是她说喜欢的是猫。
她现在还在看着我，还在逗我开心，是因为猫还没有出现，只有我这老鼠每天蹑手蹑脚地从洞里爬出来，远远地和她对视。
等她喜欢的猫来了的时候，我就该重新滚回我的洞了吧。
但我还是好喜欢她，她能在我还在她身边的时候多看我几眼吗？
嘉然小姐说接下来的每个圣诞夜都要和大家一起过。我不知道大家指哪些人。好希望这个集合能够对我做一次胞吞。

猫猫还在害怕嘉然小姐。
我会去把她爱的猫猫引来的。
我知道稍有不慎，我就会葬身猫口。
那时候嘉然小姐大概会把我的身体好好地装起来扔到门外吧。
那我就成了一包鼠条，嘻嘻。
我希望她能把我扔得近一点，因为我还是好喜欢她。会一直喜欢下去的。

我的灵魂透过窗户向里面看去，挂着的铃铛在轻轻鸣响，嘉然小姐慵懒地靠在沙发上，表演得非常温顺的橘猫坐在她的肩膀。壁炉的火光照在她的脸庞，我冻僵的心脏在风里微微发烫。"""

index_map = {}

def int_to_bytes(x: int) -> bytes:
    return x.to_bytes(6, 'big')

def int_from_bytes(xbytes: bytes) -> int:
    return int.from_bytes(xbytes, 'big')

with open("merged/merged.des", "rb") as f:
    last_key = -1
    while True:
        key = f.read(6)
        if key==b"":
            index_map[last_key][1] = f.tell()-index_map[last_key][0]
            break
        key = int_from_bytes(key)
        offset = int_from_bytes(f.read(6))
        if last_key!=-1:
            index_map[last_key][1] = offset-index_map[last_key][0]
        index_map[key] = [offset, -1]
        last_key = key

rf = open("merged/merged.db", "rb")

def test():
    hashs = str_hash(test_text)
    passage = defaultdict(lambda:0)
    import time
    time_cout = 0
    start = time.time()
    all_count = 0
    for hash in hashs:
        try:
            offset = index_map[hash][0]
            length = index_map[hash][1]
            rf.seek(offset)
            cache = rf.read(6*length)
            for i in range(0 ,length, 6): # cost most of the time
                passage[cache[i:i+6]] += 1
                all_count+=1

        except KeyError:
            continue
    time_cout += time.time()-start
    print(time_cout, all_count)
    print(sorted(passage.items(), key = lambda x:x[1], reverse = True)[:5])
test()
test()