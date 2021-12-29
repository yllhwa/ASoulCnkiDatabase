# -*- encoding: utf-8 -*-
'''
Filename         :_hash.py
Description      :摘要算法相关函数
Time             :2021/06/22 15:20:24
Author           :hwa
Version          :1.0
'''
import logging
logobj = logging.getLogger('Hash')
def hashValue(s:int,k:int,q:int,b:int):
    num = 0
    for index, c in enumerate(s):
        num+=ord(c)*(b**(k-1-index))
    return num%q

def str_hash(s:int, k:int=4, q:int=1145141919780, b:int=2, w:int=4)->list:
    """
    @description  :
    对字符串做摘要
    @param  :
    s: 字符串
    k: 敏感度 即几个字做一次摘要
    q: 模数 摘要结果对其取模
    b: 基数 摘要算法的基数
    w: 窗口宽度 选择摘要时的窗口大小
    @Returns  :
    字符串的摘要
    """
    # 逐位做摘要
    _hash=[]
    _hash.append(hashValue(s[:k],k,q,b))
    for i in range(1, len(s)-k+1):
        _hash.append(hashValue(s[i:k+i],k,q,b))
    # 选择摘要
    hash_pick=[]
    hash_pick.append(min(_hash[:w]))
    j = 1
    while(j+w<=len(_hash)):
        min_hash = min(_hash[j:j+w])
        if min_hash!=hash_pick[-1]:
            hash_pick.append(min_hash)
        j+=1
    return hash_pick
test_text = """嘉然想要一件漂亮的衣服
和yhm一样靓丽的裙子
鼠鼠们犯了难
要是有天国的锦缎该多好啊
以金银色的光线编织
还有湛蓝的夜色与洁白的昼光
以及黎明和黄昏错综的光芒
要是然然得了这锦缎
她该有多开心啊
但是啊，鼠鼠们，如此贫穷，除了梦一无所有
于是鼠鼠们每个人拿出自己的梦
凑了一件可爱的衣裳
然然穿着很合身
有了这件衣服，然然终于得了猫猫的喜爱
越来越多的猫猫依偎在然然的怀里
鼠鼠们再不敢靠近嘉然小姐了
只敢偷偷地从洞里探出脑袋
望着她，望着她
偶有发了病的鼠鼠冲出来
转瞬便葬身猫口
终于有一天
嘉然得了天国的锦缎
身边的猫猫也嫌弃鼠鼠的衣服太过寒碜
然然穿上新衣服
华丽动人，连yhm都要心生嫉妒
是时候丢掉旧衣服了
然然回想起
她与鼠鼠一起度过的时光
她陪伴鼠鼠唱过的歌曲
她为鼠鼠哭泣过的夜
又于心不忍了
于是她整齐地把衣服垫在枕头下
与此同时
阴暗的洞里，一只鼠鼠悄声说道
“轻一点啊，因为你枕着我的梦”
"""
if __name__=="__main__":
    print(str_hash(test_text))