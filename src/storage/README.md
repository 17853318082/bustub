# project2 B+Tree

##  B+树在数据库中的作用
1. B+树用<br/><br/>
2. 评价一个数据结构作为索引的优劣最重要的指标就是在查找过程中磁盘I/O操作次数的渐进复杂度。换句话说，索引的结构组织要尽量减少查找过程中磁盘I/O的存取次数。

## InooDB存储引擎使用B+树的原因
1. 页是InnoDB存储引擎管理数据库的最小磁盘单位，如果使用AVL树或红黑树会浪费磁盘空间，并且相同数量的节点b+树的高度更低，那么就会有更小的IO开销 <br/>

## 一些参数和变量

Internal Page（内部页面）： B+树中的非叶子节点，内部的页不存任何真实的数据，但是存m个有序的key entries和m+1个孩子指针（即page_id）。因为指针的数量不等于key的数量，第一个key被设置为invalid，查找方法总是从第二个key开始。任何时候，每个内部页至少半满。在删除时，两个半满页可以被合并成一个合法页或者可以被重新分配避免合并，当插入时，一个满页可以分裂成两个。</br></br>
Leaf Page(叶子页面)：叶子节点。叶子节点存储了有序的m个key entries以及m value entries。在你的实现中，value应该是64位record_id，它被用于定位真实的tuple被存储的位置。RID类定义在src/include/common/rid.h
叶子页面在键值数量上和内部页有相同的限制，应该遵守相同的合并，重分配，分裂操作。</br></br>
index(索引)： b+树索引，依赖于叶子和内部页面做实现</br></br>

KeyType:索引中每个键的类型。这通常是通用键（GenericKey），实际键的大小取决于所使用的索引类型。例如，如果使用B树索引，则键的大小可能限制为一定数量的字节，而对于哈希索引，键的大小可能由所使用的哈希函数确定。</br></br>
ValueType: 索引中每个值的类型。这通常是64位RID（记录标识符），用于唯一标识索引所在表中的记录。
</br></br>
KeyComparator: 用于比较两个KeyType实例大小关系的类。这些类通常会被包含在KeyType实现文件中。
</br></br>



## 叶子节点与非叶子节点的区别
1. page_header：叶子节点多一个next_page_id 字段</br>
2. KV数量限制：叶子最多用n-1，非叶子用n，但跟max_size() 无关</br>
3. 第一个KV：非叶子第一个Key为invalid，而叶子正常存</br>


## 缓存池page与b+树page
缓存池page包含b+树page，b+树page是缓存池page的数据部分，即page->GetData()


## B+ Tree Page 结构图


## B+ Tree Search

### 流程图如下：


1. 首先循环查找key所在的叶子页
2. 从叶子页中查找key，如果未找到则返回false，如果找到则返回true


## B+ Tree Insert