#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"
using namespace std;

namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
    : globalDepth(0), bucketSize(size), bucketNum(1) {
    buckets.push_back(make_shared<Bucket>(0));
}
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash() {
    ExtendibleHash(64);
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K& key) const {
    return hash<K>{}(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    lock_guard<mutex> lock(latch);
    return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    // lock_guard<mutex> lck2(latch);
    if (!buckets[bucket_id])
        return -1;
    lock_guard<mutex> lck(buckets[bucket_id]->latch);
    if (buckets[bucket_id]->kmap.size() == 0)// 若该桶为空
        return -1;
    return buckets[bucket_id]->localDepth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    lock_guard<mutex> lock(latch);
    return bucketNum;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K& key, V& value) {
    int index = getIdx(key);
    lock_guard<mutex> lck(buckets[index]->latch);
    if (buckets[index]->kmap.find(key) == buckets[index]->kmap.end())//没找到
      return false;
    else {
        value = buckets[index]->kmap[key];
        return true;
    }
}

template <typename K, typename V>
int ExtendibleHash<K, V>::getIdx(const K& key) const {
    lock_guard<mutex> lck(latch);
    return HashKey(key) & ((1 << globalDepth) - 1);  //取key的hash值的后globalDepth位
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K& key) {
    int index = getIdx(key);
    lock_guard<mutex> lck(buckets[index]->latch);
    shared_ptr<Bucket> cur = buckets[index];
    if (cur->kmap.find(key) == cur->kmap.end())
        return false;
    else cur->kmap.erase(key);
    return true;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K& key, const V& value) {
    int index = getIdx(key);
    shared_ptr<Bucket> cur = buckets[index];  // cur指向待插入信息应该插入的桶
    // 为什么要循环:分裂后仅靠localDepth前一位可能不能将数据分在两个桶中，所以继续算法
    for(;;) {
        lock_guard<mutex> lck(cur->latch);
        //若能插入则直接插入，算法结束
        if (cur->kmap.size() < bucketSize || cur->kmap.find(key) != cur->kmap.end()) {
            cur->kmap[key] = value;
            return;
        }

        {
            lock_guard<mutex> lck2(latch);
            //局部深度大于全局深度，将buckets复制扩大一倍
            if (++cur->localDepth > globalDepth) {
                size_t len = buckets.size();
                for (size_t i = 0; i < len; i++) {
                    buckets.push_back(buckets[i]);
                }
                globalDepth++;  // buckets翻倍，globalDepth++
            }
            //建立一个新桶,新桶的localDepth等于久桶的localDepth+1（前一步已经加1）
            bucketNum++;
            auto newBuc = make_shared<Bucket>(cur->localDepth);
            // mask用来确定靠哪一位来将原来桶中数据分配到分裂桶中
            int mask = (1 << (cur->localDepth - 1));
            typename map<K, V>::iterator it;
            for (it = cur->kmap.begin(); it != cur->kmap.end();) {
                // mask为1的那一位来决定旧桶分裂后哪些数据放在分裂出的桶中
                if (HashKey(it->first) & mask) {
                    newBuc->kmap[it->first] = it->second;
                    cur->kmap.erase(it++);
                } else
                    it++;
            }
            //给分裂出的桶找一个buckets的位置
            for (size_t i = 0; i < buckets.size(); i++) {
                if (buckets[i] == cur && (i & mask))
                    buckets[i] = newBuc;
                //为什么不在if中break:buckets中可能有多个满足条件
            }
        }
        //为下一次循环做初始化
        index = getIdx(key);
        cur = buckets[index];
    }
}

template class ExtendibleHash<page_id_t, Page*>;
template class ExtendibleHash<Page*, std::list<Page*>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
}  // namespace scudb
