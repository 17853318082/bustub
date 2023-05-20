# Project 1 - Buffer Pool

## 变量介绍：
目录(directories): 这个容器存储指向桶的指针。每个目录给定一个唯一的id，当扩张发生时id可能随之改变。哈希函数返回这个目录的id，这个id被用来指向合适的桶。$$$$目录的数量 = 2^{全局深度} $$$$ <br>
桶(bucket)：它们存储哈希键。目录指向桶。如果局部深度小于全局深度时，一个桶可能包含不止一个指针指向它。(这里是链表结构)<br>
全局深度(global depath): 它跟目录相关联。它们表示哈希函数使用的比特位数目去分类这些键。全局深度=目录id的比特位数。
桶分裂(bucket splitting):当桶的元素超过了特定的大小，那么桶分裂成两个部分。<br>
目录扩充(directory expansion):当桶溢出时，产生目录扩容。当溢出桶的局部深度等于全局深度时，目录扩容被执行。<br>

可扩展hash基本流程：
![可扩展hash执行流程](../../imgs/Basic-Working-of-Extendible-Hashing.png "可扩展hash执行流程")




参考连接1：https://ym9omojhd5.feishu.cn/docx/Fk5MdLNJuopJGaxcDHncC9wZnDg 飞书
参考连接2：https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/ 可扩充哈希

